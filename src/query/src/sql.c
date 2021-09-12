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
#line 23 "sql.y"

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
#line 42 "sql.c"
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
#define YYNSTATE             366
#define YYNRULE              293
#define YYNRULE_WITH_ACTION  293
#define YYNTOKEN             196
#define YY_MAX_SHIFT         365
#define YY_MIN_SHIFTREDUCE   574
#define YY_MAX_SHIFTREDUCE   866
#define YY_ERROR_ACTION      867
#define YY_ACCEPT_ACTION     868
#define YY_NO_ACTION         869
#define YY_MIN_REDUCE        870
#define YY_MAX_REDUCE        1162
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
#define YY_ACTTAB_COUNT (766)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1020,  626,  239,  626,  364,  233, 1026, 1039,  210,  627,
 /*    10 */   662,  627,   38,   58,   59,   38,   62,   63, 1048, 1138,
 /*    20 */   253,   52,   51,  236,   61,  322,   66,   64,   67,   65,
 /*    30 */  1039,  810,  245,  813,   57,   56, 1026,   23,   55,   54,
 /*    40 */    53,   58,   59,  626,   62,   63,  237,  246,  253,   52,
 /*    50 */    51,  627,   61,  322,   66,   64,   67,   65,  868,  365,
 /*    60 */   235, 1022,   57,   56, 1023,  250,   55,   54,   53,  988,
 /*    70 */   976,  977,  978,  979,  980,  981,  982,  983,  984,  985,
 /*    80 */   986,  987,  989,  990,  156,   29, 1045,   81,  575,  576,
 /*    90 */   577,  578,  579,  580,  581,  582,  583,  584,  585,  586,
 /*   100 */   587,  588,  154,  163,  234,  172,   58,   59, 1039,   62,
 /*   110 */    63, 1012,  804,  253,   52,   51,   72,   61,  322,   66,
 /*   120 */    64,   67,   65,  284,  275,  210,  352,   57,   56,  262,
 /*   130 */   163,   55,   54,   53,   58,   60, 1139,   62,   63,   75,
 /*   140 */   177,  253,   52,   51,  626,   61,  322,   66,   64,   67,
 /*   150 */    65,  817,  627,  282,  281,   57,   56,  267,   73,   55,
 /*   160 */    54,   53,   59,  163,   62,   63,  271,  270,  253,   52,
 /*   170 */    51,  320,   61,  322,   66,   64,   67,   65, 1087,   76,
 /*   180 */   294,  247,   57,   56,  207, 1026,   55,   54,   53,   62,
 /*   190 */    63,   38,  249,  253,   52,   51,  320,   61,  322,   66,
 /*   200 */    64,   67,   65,  296,  710,   92,   87,   57,   56,  769,
 /*   210 */   770,   55,   54,   53,   44,  318,  359,  358,  317,  316,
 /*   220 */   315,  357,  314,  313,  312,  356,  311,  355,  354,   24,
 /*   230 */   163,  252,  819,  342,  341,  808,  243,  811, 1086,  814,
 /*   240 */  1023,  252,  819,   45,  208,  808,  213,  811,  254,  814,
 /*   250 */    57,   56,  210,  220,   55,   54,   53,   96,  262,  138,
 /*   260 */   137,  136,  219, 1139,  231,  232,  327,   87,  323,  178,
 /*   270 */    99,    5,   41,  181,  231,  232,  360,  957,  180,  105,
 /*   280 */   110,  101,  109,  363,  362,  147,   66,   64,   67,   65,
 /*   290 */  1009, 1010,   35, 1013,   57,   56,  919,  307,   55,   54,
 /*   300 */    53,  256,  929,  191,   45,   93, 1014,  734,  210,  191,
 /*   310 */   731,   38,  732,   68,  733,  261,  262,  214,   44, 1139,
 /*   320 */   359,  358,  124,   68,  215,  357,  274, 1024,   79,  356,
 /*   330 */  1134,  355,  354, 1133,  352,  227,  122,  116,  127,  258,
 /*   340 */   259,  750,  809,  126,  812,  132,  135,  125,  820,  815,
 /*   350 */   920, 1025,   38,   38,  129,  816,  244,  191,  820,  815,
 /*   360 */  1023,  201,  199,  197,   38,  816,   80,   38,  196,  142,
 /*   370 */   141,  140,  139,  996,   14,  994,  995,  257,   95,  255,
 /*   380 */   997,  330,  329,  786,  998,   38,  999, 1000,   38,  324,
 /*   390 */    38,  263,   84,  260,   85,  337,  336,  331,  332,   38,
 /*   400 */  1011, 1023, 1023,   55,   54,   53,   94, 1132,   98,  333,
 /*   410 */     1,  179,  334, 1023,    3,  192, 1023,  153,  151,  150,
 /*   420 */    82,  747,  276,  735,  736,   34,  754,  766,  776,  278,
 /*   430 */   338,  278,  777,  339, 1023,  340,   39, 1023,  806, 1023,
 /*   440 */   785,  158,   69,  720,  344,  299,   26,    9, 1023,  251,
 /*   450 */   722,  301,  721,  841,  821,  625,   78,   39,  302,   39,
 /*   460 */   229,   16,  818,   15,   69,   97,   69,   25,   25,   25,
 /*   470 */   115,    6,  114, 1158,   18,  807,   17,  739,  737,  740,
 /*   480 */   738,   20,  121,   19,  120,   22,  230,   21,  709,  134,
 /*   490 */   133,  211,  823,  212,  216, 1150,  209, 1097,  217,  218,
 /*   500 */   222,  223,  224,  221,  206, 1096,  241, 1093, 1092,  242,
 /*   510 */   343,  272,  155,   48, 1047, 1058, 1055, 1056, 1060,  157,
 /*   520 */  1040,  279, 1079,  162,  290, 1078,  173, 1021,  174, 1019,
 /*   530 */   175,  176,  934,  283,  238,  152,  167,  165,  304,  305,
 /*   540 */   765, 1037,  164,  306,  309,  310,  285,   46,  204,   42,
 /*   550 */   321,  928,  287,  328, 1157,  112, 1156,   77, 1153,  297,
 /*   560 */   182,  335, 1149,   74,   50,  166,  295,  168,  293,  291,
 /*   570 */   118,  289,  286, 1148, 1145,  183,  954,   43,   40,   47,
 /*   580 */   205,  916,  128,  914,  130,  131,   49,  912,  911,  264,
 /*   590 */   194,  195,  908,  907,  906,  905,  904,  903,  902,  198,
 /*   600 */   200,  899,  897,  895,  893,  202,  890,  203,  308,  886,
 /*   610 */   353,  123,  277,   83,   88,  345,  288, 1080,  346,  347,
 /*   620 */   348,  349,  228,  350,  351,  248,  303,  361,  866,  265,
 /*   630 */   266,  865,  269,  225,  226,  268,  864,  847,  846,  933,
 /*   640 */   932,  106,  107,  273,  278,   10,  298,  742,  280,   86,
 /*   650 */    30,  910,  909,   89,  767,  143,  159,  144,  955,  186,
 /*   660 */   184,  185,  188,  187,  189,  190,  901,    2,  145,  992,
 /*   670 */   900,  892,  171,  169,   33,  170,  956,  146,  891,    4,
 /*   680 */   778,  160,  161,  772,   90,  240,  774, 1002,   91,  292,
 /*   690 */    31,   11,   32,   12,   13,   27,  300,   28,   98,  100,
 /*   700 */   103,   36,  102,  640,   37,  104,  675,  673,  672,  671,
 /*   710 */   669,  668,  667,  664,  630,  319,  108,    7,  824,  822,
 /*   720 */   325,    8,  326,  111,  113,   70,   71,  117,   39,  712,
 /*   730 */   119,  711,  708,  656,  654,  646,  652,  648,  650,  644,
 /*   740 */   642,  678,  677,  676,  674,  670,  666,  665,  193,  628,
 /*   750 */   592,  870,  869,  869,  869,  869,  869,  869,  869,  869,
 /*   760 */   869,  869,  869,  869,  148,  149,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   198,    1,  244,    1,  198,  199,  248,  246,  265,    9,
 /*    10 */     5,    9,  198,   13,   14,  198,   16,   17,  198,  276,
 /*    20 */    20,   21,   22,  262,   24,   25,   26,   27,   28,   29,
 /*    30 */   246,    5,  244,    7,   34,   35,  248,  265,   38,   39,
 /*    40 */    40,   13,   14,    1,   16,   17,  262,  245,   20,   21,
 /*    50 */    22,    9,   24,   25,   26,   27,   28,   29,  196,  197,
 /*    60 */   243,  247,   34,   35,  247,  205,   38,   39,   40,  222,
 /*    70 */   223,  224,  225,  226,  227,  228,  229,  230,  231,  232,
 /*    80 */   233,  234,  235,  236,  198,   83,  266,   87,   46,   47,
 /*    90 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   100 */    58,   59,   60,  198,   62,  252,   13,   14,  246,   16,
 /*   110 */    17,    0,   84,   20,   21,   22,   98,   24,   25,   26,
 /*   120 */    27,   28,   29,  270,  262,  265,   91,   34,   35,  198,
 /*   130 */   198,   38,   39,   40,   13,   14,  276,   16,   17,   98,
 /*   140 */   209,   20,   21,   22,    1,   24,   25,   26,   27,   28,
 /*   150 */    29,  125,    9,  267,  268,   34,   35,  143,  140,   38,
 /*   160 */    39,   40,   14,  198,   16,   17,  152,  153,   20,   21,
 /*   170 */    22,   85,   24,   25,   26,   27,   28,   29,  273,  138,
 /*   180 */   275,  244,   34,   35,  265,  248,   38,   39,   40,   16,
 /*   190 */    17,  198,  205,   20,   21,   22,   85,   24,   25,   26,
 /*   200 */    27,   28,   29,  271,    5,  273,   83,   34,   35,  126,
 /*   210 */   127,   38,   39,   40,   99,  100,  101,  102,  103,  104,
 /*   220 */   105,  106,  107,  108,  109,  110,  111,  112,  113,   45,
 /*   230 */   198,    1,    2,   34,   35,    5,  243,    7,  273,    9,
 /*   240 */   247,    1,    2,  120,  265,    5,   62,    7,  205,    9,
 /*   250 */    34,   35,  265,   69,   38,   39,   40,  206,  198,   75,
 /*   260 */    76,   77,   78,  276,   34,   35,   82,   83,   38,  209,
 /*   270 */   206,   63,   64,   65,   34,   35,  220,  221,   70,   71,
 /*   280 */    72,   73,   74,   66,   67,   68,   26,   27,   28,   29,
 /*   290 */   239,  240,  241,  242,   34,   35,  204,   89,   38,   39,
 /*   300 */    40,   69,  204,  211,  120,  273,  242,    2,  265,  211,
 /*   310 */     5,  198,    7,   83,    9,   69,  198,  265,   99,  276,
 /*   320 */   101,  102,   79,   83,  265,  106,  142,  209,  144,  110,
 /*   330 */   265,  112,  113,  265,   91,  151,   63,   64,   65,   34,
 /*   340 */    35,   38,    5,   70,    7,   72,   73,   74,  118,  119,
 /*   350 */   204,  248,  198,  198,   81,  125,  243,  211,  118,  119,
 /*   360 */   247,   63,   64,   65,  198,  125,  206,  198,   70,   71,
 /*   370 */    72,   73,   74,  222,   83,  224,  225,  145,   87,  147,
 /*   380 */   229,  149,  150,   77,  233,  198,  235,  236,  198,   15,
 /*   390 */   198,  145,   84,  147,   84,  149,  150,  243,  243,  198,
 /*   400 */   240,  247,  247,   38,   39,   40,  249,  265,  117,  243,
 /*   410 */   207,  208,  243,  247,  202,  203,  247,   63,   64,   65,
 /*   420 */   263,   98,   84,  118,  119,   83,  123,   84,   84,  121,
 /*   430 */   243,  121,   84,  243,  247,  243,   98,  247,    1,  247,
 /*   440 */   134,   98,   98,   84,  243,   84,   98,  124,  247,   61,
 /*   450 */    84,   84,   84,   84,   84,   84,   83,   98,  116,   98,
 /*   460 */   265,  146,  125,  148,   98,   98,   98,   98,   98,   98,
 /*   470 */   146,   83,  148,  248,  146,   38,  148,    5,    5,    7,
 /*   480 */     7,  146,  146,  148,  148,  146,  265,  148,  115,   79,
 /*   490 */    80,  265,  118,  265,  265,  248,  265,  238,  265,  265,
 /*   500 */   265,  265,  265,  265,  265,  238,  238,  238,  238,  238,
 /*   510 */   238,  198,  198,  264,  198,  198,  198,  198,  198,  198,
 /*   520 */   246,  246,  274,  198,  198,  274,  250,  246,  198,  198,
 /*   530 */   198,  198,  198,  269,  269,   61,  257,  259,  198,  198,
 /*   540 */   125,  261,  260,  198,  198,  198,  269,  198,  198,  198,
 /*   550 */   198,  198,  269,  198,  198,  198,  198,  137,  198,  132,
 /*   560 */   198,  198,  198,  139,  136,  258,  135,  256,  130,  129,
 /*   570 */   198,  128,  131,  198,  198,  198,  198,  198,  198,  198,
 /*   580 */   198,  198,  198,  198,  198,  198,  141,  198,  198,  198,
 /*   590 */   198,  198,  198,  198,  198,  198,  198,  198,  198,  198,
 /*   600 */   198,  198,  198,  198,  198,  198,  198,  198,   90,  198,
 /*   610 */   114,   97,  200,  200,  200,   96,  200,  200,   52,   93,
 /*   620 */    95,   56,  200,   94,   92,  200,  200,   85,    5,  154,
 /*   630 */     5,    5,    5,  200,  200,  154,    5,  101,  100,  210,
 /*   640 */   210,  206,  206,  143,  121,   83,  116,   84,   98,  122,
 /*   650 */    83,  200,  200,   98,   84,  201,   83,  201,  219,  213,
 /*   660 */   218,  217,  214,  216,  215,  212,  200,  207,  201,  237,
 /*   670 */   200,  200,  253,  255,  251,  254,  221,  201,  200,  202,
 /*   680 */    84,   83,   98,   84,   83,    1,   84,  237,   83,   83,
 /*   690 */    98,  133,   98,  133,   83,   83,  116,   83,  117,   79,
 /*   700 */    71,   88,   87,    5,   88,   87,    9,    5,    5,    5,
 /*   710 */     5,    5,    5,    5,   86,   15,   79,   83,  118,   84,
 /*   720 */    25,   83,   60,  148,  148,   16,   16,  148,   98,    5,
 /*   730 */   148,    5,   84,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */     5,    5,    5,    5,    5,    5,    5,    5,   98,   86,
 /*   750 */    61,    0,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   760 */   277,  277,  277,  277,   21,   21,  277,  277,  277,  277,
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
 /*   950 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   960 */   277,  277,
};
#define YY_SHIFT_COUNT    (365)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (751)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   184,  115,  115,  219,  219,   86,  230,  240,  240,    2,
 /*    10 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*    20 */   143,  143,  143,    0,   42,  240,  305,  305,  305,  123,
 /*    30 */   123,  143,  143,   83,  143,  111,  143,  143,  143,  143,
 /*    40 */   243,   86,   35,   35,    5,  766,  766,  766,  240,  240,
 /*    50 */   240,  240,  240,  240,  240,  240,  240,  240,  240,  240,
 /*    60 */   240,  240,  240,  240,  240,  240,  240,  240,  240,  305,
 /*    70 */   305,  305,  199,  199,  199,  199,  199,  199,  199,  143,
 /*    80 */   143,  143,  303,  143,  143,  143,  123,  123,  143,  143,
 /*    90 */   143,  143,  306,  306,  323,  123,  143,  143,  143,  143,
 /*   100 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   110 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   120 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   130 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   140 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   150 */   143,  143,  143,  143,  143,  474,  474,  474,  415,  415,
 /*   160 */   415,  415,  474,  474,  420,  424,  427,  428,  431,  438,
 /*   170 */   440,  443,  441,  445,  474,  474,  474,  518,  518,  496,
 /*   180 */    86,   86,  474,  474,  514,  519,  566,  526,  525,  565,
 /*   190 */   529,  532,  496,    5,  474,  474,  542,  542,  474,  542,
 /*   200 */   474,  542,  474,  474,  766,  766,   28,   93,   93,  121,
 /*   210 */    93,  148,  173,  208,  260,  260,  260,  260,  260,  273,
 /*   220 */   298,  216,  216,  216,  216,  232,  246,   14,  291,  365,
 /*   230 */   365,   26,  337,  217,  354,  338,  308,  310,  343,  344,
 /*   240 */   348,   18,   41,  359,  361,  366,  367,  368,  342,  369,
 /*   250 */   370,  437,  388,  374,  371,  315,  324,  328,  472,  473,
 /*   260 */   335,  336,  373,  339,  410,  623,  475,  625,  626,  481,
 /*   270 */   627,  631,  536,  538,  500,  523,  530,  562,  527,  563,
 /*   280 */   567,  550,  555,  570,  573,  596,  598,  599,  584,  601,
 /*   290 */   602,  605,  684,  606,  592,  558,  594,  560,  611,  530,
 /*   300 */   612,  580,  614,  581,  620,  613,  615,  629,  698,  616,
 /*   310 */   618,  697,  702,  703,  704,  705,  706,  707,  708,  628,
 /*   320 */   700,  637,  634,  635,  600,  638,  695,  662,  709,  575,
 /*   330 */   576,  630,  630,  630,  630,  710,  579,  582,  630,  630,
 /*   340 */   630,  724,  726,  648,  630,  728,  729,  730,  731,  732,
 /*   350 */   733,  734,  735,  736,  737,  738,  739,  740,  741,  742,
 /*   360 */   650,  663,  743,  744,  689,  751,
};
#define YY_REDUCE_COUNT (205)
#define YY_REDUCE_MIN   (-257)
#define YY_REDUCE_MAX   (478)
static const short yy_reduce_ofst[] = {
 /*     0 */  -138, -153, -153,  151,  151,   51, -140,  -13,   43, -114,
 /*    10 */  -183,  -95,  -68,   -7,  113,  154,  155,  166,  169,  187,
 /*    20 */   190,  192,  201, -180, -194, -257, -242, -212,  -63, -239,
 /*    30 */  -216,  -35,   32, -147, -198,   64,  -69,   60,  118, -186,
 /*    40 */    92,  160,   98,  146,   56,  157,  203,  212, -228,  -81,
 /*    50 */   -21,   52,   59,   65,   68,  142,  195,  221,  226,  228,
 /*    60 */   229,  231,  233,  234,  235,  236,  237,  238,  239,  103,
 /*    70 */   225,  247,  259,  267,  268,  269,  270,  271,  272,  313,
 /*    80 */   314,  316,  249,  317,  318,  319,  274,  275,  320,  321,
 /*    90 */   325,  326,  248,  251,  276,  281,  330,  331,  332,  333,
 /*   100 */   334,  340,  341,  345,  346,  347,  349,  350,  351,  352,
 /*   110 */   353,  355,  356,  357,  358,  360,  362,  363,  364,  372,
 /*   120 */   375,  376,  377,  378,  379,  380,  381,  382,  383,  384,
 /*   130 */   385,  386,  387,  389,  390,  391,  392,  393,  394,  395,
 /*   140 */   396,  397,  398,  399,  400,  401,  402,  403,  404,  405,
 /*   150 */   406,  407,  408,  409,  411,  412,  413,  414,  264,  265,
 /*   160 */   277,  283,  416,  417,  280,  282,  278,  307,  279,  311,
 /*   170 */   418,  421,  419,  423,  422,  425,  426,  429,  430,  432,
 /*   180 */   435,  436,  433,  434,  439,  442,  444,  446,  447,  448,
 /*   190 */   449,  453,  450,  455,  451,  452,  454,  456,  466,  467,
 /*   200 */   470,  476,  471,  478,  460,  477,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   867,  991,  930, 1001,  917,  927, 1141, 1141, 1141,  867,
 /*    10 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*    20 */   867,  867,  867, 1049,  887, 1141,  867,  867,  867,  867,
 /*    30 */   867,  867,  867, 1064,  867,  927,  867,  867,  867,  867,
 /*    40 */   937,  927,  937,  937,  867, 1044,  975,  993,  867,  867,
 /*    50 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*    60 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*    70 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*    80 */   867,  867, 1051, 1057, 1054,  867,  867,  867, 1059,  867,
 /*    90 */   867,  867, 1083, 1083, 1042,  867,  867,  867,  867,  867,
 /*   100 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   110 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   120 */   867,  867,  867,  867,  867,  867,  867,  867,  915,  867,
 /*   130 */   913,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   140 */   867,  867,  867,  867,  867,  867,  867,  898,  867,  867,
 /*   150 */   867,  867,  867,  867,  885,  889,  889,  889,  867,  867,
 /*   160 */   867,  867,  889,  889, 1090, 1094, 1076, 1088, 1084, 1071,
 /*   170 */  1069, 1067, 1075, 1098,  889,  889,  889,  935,  935,  931,
 /*   180 */   927,  927,  889,  889,  953,  951,  949,  941,  947,  943,
 /*   190 */   945,  939,  918,  867,  889,  889,  925,  925,  889,  925,
 /*   200 */   889,  925,  889,  889,  975,  993,  867, 1099, 1089,  867,
 /*   210 */  1140, 1129, 1128,  867, 1136, 1135, 1127, 1126, 1125,  867,
 /*   220 */   867, 1121, 1124, 1123, 1122,  867,  867,  867,  867, 1131,
 /*   230 */  1130,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   240 */   867, 1095, 1091,  867,  867,  867,  867,  867,  867,  867,
 /*   250 */   867,  867, 1101,  867,  867,  867,  867,  867,  867,  867,
 /*   260 */   867,  867, 1003,  867,  867,  867,  867,  867,  867,  867,
 /*   270 */   867,  867,  867,  867,  867, 1041,  867,  867,  867,  867,
 /*   280 */   867, 1053, 1052,  867,  867,  867,  867,  867,  867,  867,
 /*   290 */   867,  867,  867,  867, 1085,  867, 1077,  867,  867, 1015,
 /*   300 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   310 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   320 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   330 */   867, 1159, 1154, 1155, 1152,  867,  867,  867, 1151, 1146,
 /*   340 */  1147,  867,  867,  867, 1144,  867,  867,  867,  867,  867,
 /*   350 */   867,  867,  867,  867,  867,  867,  867,  867,  867,  867,
 /*   360 */   959,  867,  896,  894,  867,  867,
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
    1,  /*      MATCH => ID */
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
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*    IPTOKEN => ID */
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
  /*   22 */ "MATCH",
  /*   23 */ "GLOB",
  /*   24 */ "BETWEEN",
  /*   25 */ "IN",
  /*   26 */ "GT",
  /*   27 */ "GE",
  /*   28 */ "LT",
  /*   29 */ "LE",
  /*   30 */ "BITAND",
  /*   31 */ "BITOR",
  /*   32 */ "LSHIFT",
  /*   33 */ "RSHIFT",
  /*   34 */ "PLUS",
  /*   35 */ "MINUS",
  /*   36 */ "DIVIDE",
  /*   37 */ "TIMES",
  /*   38 */ "STAR",
  /*   39 */ "SLASH",
  /*   40 */ "REM",
  /*   41 */ "CONCAT",
  /*   42 */ "UMINUS",
  /*   43 */ "UPLUS",
  /*   44 */ "BITNOT",
  /*   45 */ "SHOW",
  /*   46 */ "DATABASES",
  /*   47 */ "TOPICS",
  /*   48 */ "FUNCTIONS",
  /*   49 */ "MNODES",
  /*   50 */ "DNODES",
  /*   51 */ "ACCOUNTS",
  /*   52 */ "USERS",
  /*   53 */ "MODULES",
  /*   54 */ "QUERIES",
  /*   55 */ "CONNECTIONS",
  /*   56 */ "STREAMS",
  /*   57 */ "VARIABLES",
  /*   58 */ "SCORES",
  /*   59 */ "GRANTS",
  /*   60 */ "VNODES",
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
  /*  176 */ "KEY",
  /*  177 */ "OF",
  /*  178 */ "RAISE",
  /*  179 */ "REPLACE",
  /*  180 */ "RESTRICT",
  /*  181 */ "ROW",
  /*  182 */ "STATEMENT",
  /*  183 */ "TRIGGER",
  /*  184 */ "VIEW",
  /*  185 */ "IPTOKEN",
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
  /*  198 */ "ids",
  /*  199 */ "dbPrefix",
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
 /*  16 */ "cmd ::= SHOW VNODES ids",
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
 /*  29 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  30 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  32 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  33 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  34 */ "cmd ::= DROP FUNCTION ids",
 /*  35 */ "cmd ::= DROP DNODE ids",
 /*  36 */ "cmd ::= DROP USER ids",
 /*  37 */ "cmd ::= DROP ACCOUNT ids",
 /*  38 */ "cmd ::= USE ids",
 /*  39 */ "cmd ::= DESCRIBE ids cpxName",
 /*  40 */ "cmd ::= DESC ids cpxName",
 /*  41 */ "cmd ::= ALTER USER ids PASS ids",
 /*  42 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids",
 /*  44 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids",
 /*  46 */ "cmd ::= ALTER LOCAL ids ids",
 /*  47 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  48 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  50 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  52 */ "ids ::= ID",
 /*  53 */ "ids ::= STRING",
 /*  54 */ "ifexists ::= IF EXISTS",
 /*  55 */ "ifexists ::=",
 /*  56 */ "ifnotexists ::= IF NOT EXISTS",
 /*  57 */ "ifnotexists ::=",
 /*  58 */ "cmd ::= CREATE DNODE ids",
 /*  59 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  60 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  61 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  62 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  63 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  64 */ "cmd ::= CREATE USER ids PASS ids",
 /*  65 */ "bufsize ::=",
 /*  66 */ "bufsize ::= BUFSIZE INTEGER",
 /*  67 */ "pps ::=",
 /*  68 */ "pps ::= PPS INTEGER",
 /*  69 */ "tseries ::=",
 /*  70 */ "tseries ::= TSERIES INTEGER",
 /*  71 */ "dbs ::=",
 /*  72 */ "dbs ::= DBS INTEGER",
 /*  73 */ "streams ::=",
 /*  74 */ "streams ::= STREAMS INTEGER",
 /*  75 */ "storage ::=",
 /*  76 */ "storage ::= STORAGE INTEGER",
 /*  77 */ "qtime ::=",
 /*  78 */ "qtime ::= QTIME INTEGER",
 /*  79 */ "users ::=",
 /*  80 */ "users ::= USERS INTEGER",
 /*  81 */ "conns ::=",
 /*  82 */ "conns ::= CONNS INTEGER",
 /*  83 */ "state ::=",
 /*  84 */ "state ::= STATE ids",
 /*  85 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  86 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  87 */ "intitemlist ::= intitem",
 /*  88 */ "intitem ::= INTEGER",
 /*  89 */ "keep ::= KEEP intitemlist",
 /*  90 */ "cache ::= CACHE INTEGER",
 /*  91 */ "replica ::= REPLICA INTEGER",
 /*  92 */ "quorum ::= QUORUM INTEGER",
 /*  93 */ "days ::= DAYS INTEGER",
 /*  94 */ "minrows ::= MINROWS INTEGER",
 /*  95 */ "maxrows ::= MAXROWS INTEGER",
 /*  96 */ "blocks ::= BLOCKS INTEGER",
 /*  97 */ "ctime ::= CTIME INTEGER",
 /*  98 */ "wal ::= WAL INTEGER",
 /*  99 */ "fsync ::= FSYNC INTEGER",
 /* 100 */ "comp ::= COMP INTEGER",
 /* 101 */ "prec ::= PRECISION STRING",
 /* 102 */ "update ::= UPDATE INTEGER",
 /* 103 */ "cachelast ::= CACHELAST INTEGER",
 /* 104 */ "partitions ::= PARTITIONS INTEGER",
 /* 105 */ "db_optr ::=",
 /* 106 */ "db_optr ::= db_optr cache",
 /* 107 */ "db_optr ::= db_optr replica",
 /* 108 */ "db_optr ::= db_optr quorum",
 /* 109 */ "db_optr ::= db_optr days",
 /* 110 */ "db_optr ::= db_optr minrows",
 /* 111 */ "db_optr ::= db_optr maxrows",
 /* 112 */ "db_optr ::= db_optr blocks",
 /* 113 */ "db_optr ::= db_optr ctime",
 /* 114 */ "db_optr ::= db_optr wal",
 /* 115 */ "db_optr ::= db_optr fsync",
 /* 116 */ "db_optr ::= db_optr comp",
 /* 117 */ "db_optr ::= db_optr prec",
 /* 118 */ "db_optr ::= db_optr keep",
 /* 119 */ "db_optr ::= db_optr update",
 /* 120 */ "db_optr ::= db_optr cachelast",
 /* 121 */ "topic_optr ::= db_optr",
 /* 122 */ "topic_optr ::= topic_optr partitions",
 /* 123 */ "alter_db_optr ::=",
 /* 124 */ "alter_db_optr ::= alter_db_optr replica",
 /* 125 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 126 */ "alter_db_optr ::= alter_db_optr keep",
 /* 127 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 128 */ "alter_db_optr ::= alter_db_optr comp",
 /* 129 */ "alter_db_optr ::= alter_db_optr update",
 /* 130 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 131 */ "alter_topic_optr ::= alter_db_optr",
 /* 132 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 133 */ "typename ::= ids",
 /* 134 */ "typename ::= ids LP signed RP",
 /* 135 */ "typename ::= ids UNSIGNED",
 /* 136 */ "signed ::= INTEGER",
 /* 137 */ "signed ::= PLUS INTEGER",
 /* 138 */ "signed ::= MINUS INTEGER",
 /* 139 */ "cmd ::= CREATE TABLE create_table_args",
 /* 140 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 141 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 142 */ "cmd ::= CREATE TABLE create_table_list",
 /* 143 */ "create_table_list ::= create_from_stable",
 /* 144 */ "create_table_list ::= create_table_list create_from_stable",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 146 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 147 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 148 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 149 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 150 */ "tagNamelist ::= ids",
 /* 151 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 152 */ "columnlist ::= columnlist COMMA column",
 /* 153 */ "columnlist ::= column",
 /* 154 */ "column ::= ids typename",
 /* 155 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 156 */ "tagitemlist ::= tagitem",
 /* 157 */ "tagitem ::= INTEGER",
 /* 158 */ "tagitem ::= FLOAT",
 /* 159 */ "tagitem ::= STRING",
 /* 160 */ "tagitem ::= BOOL",
 /* 161 */ "tagitem ::= NULL",
 /* 162 */ "tagitem ::= NOW",
 /* 163 */ "tagitem ::= MINUS INTEGER",
 /* 164 */ "tagitem ::= MINUS FLOAT",
 /* 165 */ "tagitem ::= PLUS INTEGER",
 /* 166 */ "tagitem ::= PLUS FLOAT",
 /* 167 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 168 */ "select ::= LP select RP",
 /* 169 */ "union ::= select",
 /* 170 */ "union ::= union UNION ALL select",
 /* 171 */ "cmd ::= union",
 /* 172 */ "select ::= SELECT selcollist",
 /* 173 */ "sclp ::= selcollist COMMA",
 /* 174 */ "sclp ::=",
 /* 175 */ "selcollist ::= sclp distinct expr as",
 /* 176 */ "selcollist ::= sclp STAR",
 /* 177 */ "as ::= AS ids",
 /* 178 */ "as ::= ids",
 /* 179 */ "as ::=",
 /* 180 */ "distinct ::= DISTINCT",
 /* 181 */ "distinct ::=",
 /* 182 */ "from ::= FROM tablelist",
 /* 183 */ "from ::= FROM sub",
 /* 184 */ "sub ::= LP union RP",
 /* 185 */ "sub ::= LP union RP ids",
 /* 186 */ "sub ::= sub COMMA LP union RP ids",
 /* 187 */ "tablelist ::= ids cpxName",
 /* 188 */ "tablelist ::= ids cpxName ids",
 /* 189 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 190 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 191 */ "tmvar ::= VARIABLE",
 /* 192 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 193 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 194 */ "interval_option ::=",
 /* 195 */ "intervalKey ::= INTERVAL",
 /* 196 */ "intervalKey ::= EVERY",
 /* 197 */ "session_option ::=",
 /* 198 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 199 */ "windowstate_option ::=",
 /* 200 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 201 */ "fill_opt ::=",
 /* 202 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 203 */ "fill_opt ::= FILL LP ID RP",
 /* 204 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 205 */ "sliding_opt ::=",
 /* 206 */ "orderby_opt ::=",
 /* 207 */ "orderby_opt ::= ORDER BY sortlist",
 /* 208 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 209 */ "sortlist ::= item sortorder",
 /* 210 */ "item ::= ids cpxName",
 /* 211 */ "sortorder ::= ASC",
 /* 212 */ "sortorder ::= DESC",
 /* 213 */ "sortorder ::=",
 /* 214 */ "groupby_opt ::=",
 /* 215 */ "groupby_opt ::= GROUP BY grouplist",
 /* 216 */ "grouplist ::= grouplist COMMA item",
 /* 217 */ "grouplist ::= item",
 /* 218 */ "having_opt ::=",
 /* 219 */ "having_opt ::= HAVING expr",
 /* 220 */ "limit_opt ::=",
 /* 221 */ "limit_opt ::= LIMIT signed",
 /* 222 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 223 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 224 */ "slimit_opt ::=",
 /* 225 */ "slimit_opt ::= SLIMIT signed",
 /* 226 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 227 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 228 */ "where_opt ::=",
 /* 229 */ "where_opt ::= WHERE expr",
 /* 230 */ "expr ::= LP expr RP",
 /* 231 */ "expr ::= ID",
 /* 232 */ "expr ::= ID DOT ID",
 /* 233 */ "expr ::= ID DOT STAR",
 /* 234 */ "expr ::= INTEGER",
 /* 235 */ "expr ::= MINUS INTEGER",
 /* 236 */ "expr ::= PLUS INTEGER",
 /* 237 */ "expr ::= FLOAT",
 /* 238 */ "expr ::= MINUS FLOAT",
 /* 239 */ "expr ::= PLUS FLOAT",
 /* 240 */ "expr ::= STRING",
 /* 241 */ "expr ::= NOW",
 /* 242 */ "expr ::= VARIABLE",
 /* 243 */ "expr ::= PLUS VARIABLE",
 /* 244 */ "expr ::= MINUS VARIABLE",
 /* 245 */ "expr ::= BOOL",
 /* 246 */ "expr ::= NULL",
 /* 247 */ "expr ::= ID LP exprlist RP",
 /* 248 */ "expr ::= ID LP STAR RP",
 /* 249 */ "expr ::= expr IS NULL",
 /* 250 */ "expr ::= expr IS NOT NULL",
 /* 251 */ "expr ::= expr LT expr",
 /* 252 */ "expr ::= expr GT expr",
 /* 253 */ "expr ::= expr LE expr",
 /* 254 */ "expr ::= expr GE expr",
 /* 255 */ "expr ::= expr NE expr",
 /* 256 */ "expr ::= expr EQ expr",
 /* 257 */ "expr ::= expr BETWEEN expr AND expr",
 /* 258 */ "expr ::= expr AND expr",
 /* 259 */ "expr ::= expr OR expr",
 /* 260 */ "expr ::= expr PLUS expr",
 /* 261 */ "expr ::= expr MINUS expr",
 /* 262 */ "expr ::= expr STAR expr",
 /* 263 */ "expr ::= expr SLASH expr",
 /* 264 */ "expr ::= expr REM expr",
 /* 265 */ "expr ::= expr LIKE expr",
 /* 266 */ "expr ::= expr MATCH expr",
 /* 267 */ "expr ::= expr IN LP exprlist RP",
 /* 268 */ "exprlist ::= exprlist COMMA expritem",
 /* 269 */ "exprlist ::= expritem",
 /* 270 */ "expritem ::= expr",
 /* 271 */ "expritem ::=",
 /* 272 */ "cmd ::= RESET QUERY CACHE",
 /* 273 */ "cmd ::= SYNCDB ids REPLICA",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 290 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 291 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 292 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
#line 761 "sql.y"
tSqlExprListDestroy((yypminor->yy403));
#line 1517 "sql.c"
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
#line 256 "sql.y"
taosArrayDestroy((yypminor->yy403));
#line 1533 "sql.c"
}
      break;
    case 241: /* create_table_list */
{
#line 364 "sql.y"
destroyCreateTableSql((yypminor->yy56));
#line 1540 "sql.c"
}
      break;
    case 246: /* select */
{
#line 484 "sql.y"
destroySqlNode((yypminor->yy224));
#line 1547 "sql.c"
}
      break;
    case 250: /* from */
    case 267: /* tablelist */
    case 268: /* sub */
{
#line 539 "sql.y"
destroyRelationInfo((yypminor->yy114));
#line 1556 "sql.c"
}
      break;
    case 251: /* where_opt */
    case 258: /* having_opt */
    case 265: /* expr */
    case 276: /* expritem */
{
#line 691 "sql.y"
tSqlExprDestroy((yypminor->yy260));
#line 1566 "sql.c"
}
      break;
    case 262: /* union */
{
#line 492 "sql.y"
destroyAllSqlNode((yypminor->yy403));
#line 1573 "sql.c"
}
      break;
    case 272: /* sortitem */
{
#line 624 "sql.y"
tVariantDestroy(&(yypminor->yy488));
#line 1580 "sql.c"
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
   196,  /* (0) program ::= cmd */
   197,  /* (1) cmd ::= SHOW DATABASES */
   197,  /* (2) cmd ::= SHOW TOPICS */
   197,  /* (3) cmd ::= SHOW FUNCTIONS */
   197,  /* (4) cmd ::= SHOW MNODES */
   197,  /* (5) cmd ::= SHOW DNODES */
   197,  /* (6) cmd ::= SHOW ACCOUNTS */
   197,  /* (7) cmd ::= SHOW USERS */
   197,  /* (8) cmd ::= SHOW MODULES */
   197,  /* (9) cmd ::= SHOW QUERIES */
   197,  /* (10) cmd ::= SHOW CONNECTIONS */
   197,  /* (11) cmd ::= SHOW STREAMS */
   197,  /* (12) cmd ::= SHOW VARIABLES */
   197,  /* (13) cmd ::= SHOW SCORES */
   197,  /* (14) cmd ::= SHOW GRANTS */
   197,  /* (15) cmd ::= SHOW VNODES */
   197,  /* (16) cmd ::= SHOW VNODES ids */
   199,  /* (17) dbPrefix ::= */
   199,  /* (18) dbPrefix ::= ids DOT */
   200,  /* (19) cpxName ::= */
   200,  /* (20) cpxName ::= DOT ids */
   197,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   197,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   197,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   197,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   197,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   197,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   197,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   197,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   197,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   197,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   197,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   197,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   197,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   197,  /* (34) cmd ::= DROP FUNCTION ids */
   197,  /* (35) cmd ::= DROP DNODE ids */
   197,  /* (36) cmd ::= DROP USER ids */
   197,  /* (37) cmd ::= DROP ACCOUNT ids */
   197,  /* (38) cmd ::= USE ids */
   197,  /* (39) cmd ::= DESCRIBE ids cpxName */
   197,  /* (40) cmd ::= DESC ids cpxName */
   197,  /* (41) cmd ::= ALTER USER ids PASS ids */
   197,  /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
   197,  /* (43) cmd ::= ALTER DNODE ids ids */
   197,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   197,  /* (45) cmd ::= ALTER LOCAL ids */
   197,  /* (46) cmd ::= ALTER LOCAL ids ids */
   197,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   197,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   197,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   197,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   197,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   198,  /* (52) ids ::= ID */
   198,  /* (53) ids ::= STRING */
   201,  /* (54) ifexists ::= IF EXISTS */
   201,  /* (55) ifexists ::= */
   206,  /* (56) ifnotexists ::= IF NOT EXISTS */
   206,  /* (57) ifnotexists ::= */
   197,  /* (58) cmd ::= CREATE DNODE ids */
   197,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   197,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   197,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   197,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   197,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   197,  /* (64) cmd ::= CREATE USER ids PASS ids */
   210,  /* (65) bufsize ::= */
   210,  /* (66) bufsize ::= BUFSIZE INTEGER */
   211,  /* (67) pps ::= */
   211,  /* (68) pps ::= PPS INTEGER */
   212,  /* (69) tseries ::= */
   212,  /* (70) tseries ::= TSERIES INTEGER */
   213,  /* (71) dbs ::= */
   213,  /* (72) dbs ::= DBS INTEGER */
   214,  /* (73) streams ::= */
   214,  /* (74) streams ::= STREAMS INTEGER */
   215,  /* (75) storage ::= */
   215,  /* (76) storage ::= STORAGE INTEGER */
   216,  /* (77) qtime ::= */
   216,  /* (78) qtime ::= QTIME INTEGER */
   217,  /* (79) users ::= */
   217,  /* (80) users ::= USERS INTEGER */
   218,  /* (81) conns ::= */
   218,  /* (82) conns ::= CONNS INTEGER */
   219,  /* (83) state ::= */
   219,  /* (84) state ::= STATE ids */
   204,  /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   220,  /* (86) intitemlist ::= intitemlist COMMA intitem */
   220,  /* (87) intitemlist ::= intitem */
   221,  /* (88) intitem ::= INTEGER */
   222,  /* (89) keep ::= KEEP intitemlist */
   223,  /* (90) cache ::= CACHE INTEGER */
   224,  /* (91) replica ::= REPLICA INTEGER */
   225,  /* (92) quorum ::= QUORUM INTEGER */
   226,  /* (93) days ::= DAYS INTEGER */
   227,  /* (94) minrows ::= MINROWS INTEGER */
   228,  /* (95) maxrows ::= MAXROWS INTEGER */
   229,  /* (96) blocks ::= BLOCKS INTEGER */
   230,  /* (97) ctime ::= CTIME INTEGER */
   231,  /* (98) wal ::= WAL INTEGER */
   232,  /* (99) fsync ::= FSYNC INTEGER */
   233,  /* (100) comp ::= COMP INTEGER */
   234,  /* (101) prec ::= PRECISION STRING */
   235,  /* (102) update ::= UPDATE INTEGER */
   236,  /* (103) cachelast ::= CACHELAST INTEGER */
   237,  /* (104) partitions ::= PARTITIONS INTEGER */
   207,  /* (105) db_optr ::= */
   207,  /* (106) db_optr ::= db_optr cache */
   207,  /* (107) db_optr ::= db_optr replica */
   207,  /* (108) db_optr ::= db_optr quorum */
   207,  /* (109) db_optr ::= db_optr days */
   207,  /* (110) db_optr ::= db_optr minrows */
   207,  /* (111) db_optr ::= db_optr maxrows */
   207,  /* (112) db_optr ::= db_optr blocks */
   207,  /* (113) db_optr ::= db_optr ctime */
   207,  /* (114) db_optr ::= db_optr wal */
   207,  /* (115) db_optr ::= db_optr fsync */
   207,  /* (116) db_optr ::= db_optr comp */
   207,  /* (117) db_optr ::= db_optr prec */
   207,  /* (118) db_optr ::= db_optr keep */
   207,  /* (119) db_optr ::= db_optr update */
   207,  /* (120) db_optr ::= db_optr cachelast */
   208,  /* (121) topic_optr ::= db_optr */
   208,  /* (122) topic_optr ::= topic_optr partitions */
   202,  /* (123) alter_db_optr ::= */
   202,  /* (124) alter_db_optr ::= alter_db_optr replica */
   202,  /* (125) alter_db_optr ::= alter_db_optr quorum */
   202,  /* (126) alter_db_optr ::= alter_db_optr keep */
   202,  /* (127) alter_db_optr ::= alter_db_optr blocks */
   202,  /* (128) alter_db_optr ::= alter_db_optr comp */
   202,  /* (129) alter_db_optr ::= alter_db_optr update */
   202,  /* (130) alter_db_optr ::= alter_db_optr cachelast */
   203,  /* (131) alter_topic_optr ::= alter_db_optr */
   203,  /* (132) alter_topic_optr ::= alter_topic_optr partitions */
   209,  /* (133) typename ::= ids */
   209,  /* (134) typename ::= ids LP signed RP */
   209,  /* (135) typename ::= ids UNSIGNED */
   238,  /* (136) signed ::= INTEGER */
   238,  /* (137) signed ::= PLUS INTEGER */
   238,  /* (138) signed ::= MINUS INTEGER */
   197,  /* (139) cmd ::= CREATE TABLE create_table_args */
   197,  /* (140) cmd ::= CREATE TABLE create_stable_args */
   197,  /* (141) cmd ::= CREATE STABLE create_stable_args */
   197,  /* (142) cmd ::= CREATE TABLE create_table_list */
   241,  /* (143) create_table_list ::= create_from_stable */
   241,  /* (144) create_table_list ::= create_table_list create_from_stable */
   239,  /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   240,  /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   242,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   242,  /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   245,  /* (149) tagNamelist ::= tagNamelist COMMA ids */
   245,  /* (150) tagNamelist ::= ids */
   239,  /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
   243,  /* (152) columnlist ::= columnlist COMMA column */
   243,  /* (153) columnlist ::= column */
   247,  /* (154) column ::= ids typename */
   244,  /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
   244,  /* (156) tagitemlist ::= tagitem */
   248,  /* (157) tagitem ::= INTEGER */
   248,  /* (158) tagitem ::= FLOAT */
   248,  /* (159) tagitem ::= STRING */
   248,  /* (160) tagitem ::= BOOL */
   248,  /* (161) tagitem ::= NULL */
   248,  /* (162) tagitem ::= NOW */
   248,  /* (163) tagitem ::= MINUS INTEGER */
   248,  /* (164) tagitem ::= MINUS FLOAT */
   248,  /* (165) tagitem ::= PLUS INTEGER */
   248,  /* (166) tagitem ::= PLUS FLOAT */
   246,  /* (167) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   246,  /* (168) select ::= LP select RP */
   262,  /* (169) union ::= select */
   262,  /* (170) union ::= union UNION ALL select */
   197,  /* (171) cmd ::= union */
   246,  /* (172) select ::= SELECT selcollist */
   263,  /* (173) sclp ::= selcollist COMMA */
   263,  /* (174) sclp ::= */
   249,  /* (175) selcollist ::= sclp distinct expr as */
   249,  /* (176) selcollist ::= sclp STAR */
   266,  /* (177) as ::= AS ids */
   266,  /* (178) as ::= ids */
   266,  /* (179) as ::= */
   264,  /* (180) distinct ::= DISTINCT */
   264,  /* (181) distinct ::= */
   250,  /* (182) from ::= FROM tablelist */
   250,  /* (183) from ::= FROM sub */
   268,  /* (184) sub ::= LP union RP */
   268,  /* (185) sub ::= LP union RP ids */
   268,  /* (186) sub ::= sub COMMA LP union RP ids */
   267,  /* (187) tablelist ::= ids cpxName */
   267,  /* (188) tablelist ::= ids cpxName ids */
   267,  /* (189) tablelist ::= tablelist COMMA ids cpxName */
   267,  /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
   269,  /* (191) tmvar ::= VARIABLE */
   252,  /* (192) interval_option ::= intervalKey LP tmvar RP */
   252,  /* (193) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   252,  /* (194) interval_option ::= */
   270,  /* (195) intervalKey ::= INTERVAL */
   270,  /* (196) intervalKey ::= EVERY */
   254,  /* (197) session_option ::= */
   254,  /* (198) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   255,  /* (199) windowstate_option ::= */
   255,  /* (200) windowstate_option ::= STATE_WINDOW LP ids RP */
   256,  /* (201) fill_opt ::= */
   256,  /* (202) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   256,  /* (203) fill_opt ::= FILL LP ID RP */
   253,  /* (204) sliding_opt ::= SLIDING LP tmvar RP */
   253,  /* (205) sliding_opt ::= */
   259,  /* (206) orderby_opt ::= */
   259,  /* (207) orderby_opt ::= ORDER BY sortlist */
   271,  /* (208) sortlist ::= sortlist COMMA item sortorder */
   271,  /* (209) sortlist ::= item sortorder */
   273,  /* (210) item ::= ids cpxName */
   274,  /* (211) sortorder ::= ASC */
   274,  /* (212) sortorder ::= DESC */
   274,  /* (213) sortorder ::= */
   257,  /* (214) groupby_opt ::= */
   257,  /* (215) groupby_opt ::= GROUP BY grouplist */
   275,  /* (216) grouplist ::= grouplist COMMA item */
   275,  /* (217) grouplist ::= item */
   258,  /* (218) having_opt ::= */
   258,  /* (219) having_opt ::= HAVING expr */
   261,  /* (220) limit_opt ::= */
   261,  /* (221) limit_opt ::= LIMIT signed */
   261,  /* (222) limit_opt ::= LIMIT signed OFFSET signed */
   261,  /* (223) limit_opt ::= LIMIT signed COMMA signed */
   260,  /* (224) slimit_opt ::= */
   260,  /* (225) slimit_opt ::= SLIMIT signed */
   260,  /* (226) slimit_opt ::= SLIMIT signed SOFFSET signed */
   260,  /* (227) slimit_opt ::= SLIMIT signed COMMA signed */
   251,  /* (228) where_opt ::= */
   251,  /* (229) where_opt ::= WHERE expr */
   265,  /* (230) expr ::= LP expr RP */
   265,  /* (231) expr ::= ID */
   265,  /* (232) expr ::= ID DOT ID */
   265,  /* (233) expr ::= ID DOT STAR */
   265,  /* (234) expr ::= INTEGER */
   265,  /* (235) expr ::= MINUS INTEGER */
   265,  /* (236) expr ::= PLUS INTEGER */
   265,  /* (237) expr ::= FLOAT */
   265,  /* (238) expr ::= MINUS FLOAT */
   265,  /* (239) expr ::= PLUS FLOAT */
   265,  /* (240) expr ::= STRING */
   265,  /* (241) expr ::= NOW */
   265,  /* (242) expr ::= VARIABLE */
   265,  /* (243) expr ::= PLUS VARIABLE */
   265,  /* (244) expr ::= MINUS VARIABLE */
   265,  /* (245) expr ::= BOOL */
   265,  /* (246) expr ::= NULL */
   265,  /* (247) expr ::= ID LP exprlist RP */
   265,  /* (248) expr ::= ID LP STAR RP */
   265,  /* (249) expr ::= expr IS NULL */
   265,  /* (250) expr ::= expr IS NOT NULL */
   265,  /* (251) expr ::= expr LT expr */
   265,  /* (252) expr ::= expr GT expr */
   265,  /* (253) expr ::= expr LE expr */
   265,  /* (254) expr ::= expr GE expr */
   265,  /* (255) expr ::= expr NE expr */
   265,  /* (256) expr ::= expr EQ expr */
   265,  /* (257) expr ::= expr BETWEEN expr AND expr */
   265,  /* (258) expr ::= expr AND expr */
   265,  /* (259) expr ::= expr OR expr */
   265,  /* (260) expr ::= expr PLUS expr */
   265,  /* (261) expr ::= expr MINUS expr */
   265,  /* (262) expr ::= expr STAR expr */
   265,  /* (263) expr ::= expr SLASH expr */
   265,  /* (264) expr ::= expr REM expr */
   265,  /* (265) expr ::= expr LIKE expr */
   265,  /* (266) expr ::= expr MATCH expr */
   265,  /* (267) expr ::= expr IN LP exprlist RP */
   205,  /* (268) exprlist ::= exprlist COMMA expritem */
   205,  /* (269) exprlist ::= expritem */
   276,  /* (270) expritem ::= expr */
   276,  /* (271) expritem ::= */
   197,  /* (272) cmd ::= RESET QUERY CACHE */
   197,  /* (273) cmd ::= SYNCDB ids REPLICA */
   197,  /* (274) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   197,  /* (275) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   197,  /* (276) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   197,  /* (277) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   197,  /* (278) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   197,  /* (279) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   197,  /* (280) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   197,  /* (281) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   197,  /* (282) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   197,  /* (283) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   197,  /* (284) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   197,  /* (285) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   197,  /* (286) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   197,  /* (287) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   197,  /* (288) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   197,  /* (289) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   197,  /* (290) cmd ::= KILL CONNECTION INTEGER */
   197,  /* (291) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   197,  /* (292) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -3,  /* (16) cmd ::= SHOW VNODES ids */
    0,  /* (17) dbPrefix ::= */
   -2,  /* (18) dbPrefix ::= ids DOT */
    0,  /* (19) cpxName ::= */
   -2,  /* (20) cpxName ::= DOT ids */
   -5,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   -5,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   -4,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (34) cmd ::= DROP FUNCTION ids */
   -3,  /* (35) cmd ::= DROP DNODE ids */
   -3,  /* (36) cmd ::= DROP USER ids */
   -3,  /* (37) cmd ::= DROP ACCOUNT ids */
   -2,  /* (38) cmd ::= USE ids */
   -3,  /* (39) cmd ::= DESCRIBE ids cpxName */
   -3,  /* (40) cmd ::= DESC ids cpxName */
   -5,  /* (41) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (43) cmd ::= ALTER DNODE ids ids */
   -5,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (45) cmd ::= ALTER LOCAL ids */
   -4,  /* (46) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -6,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   -1,  /* (52) ids ::= ID */
   -1,  /* (53) ids ::= STRING */
   -2,  /* (54) ifexists ::= IF EXISTS */
    0,  /* (55) ifexists ::= */
   -3,  /* (56) ifnotexists ::= IF NOT EXISTS */
    0,  /* (57) ifnotexists ::= */
   -3,  /* (58) cmd ::= CREATE DNODE ids */
   -6,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -8,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (64) cmd ::= CREATE USER ids PASS ids */
    0,  /* (65) bufsize ::= */
   -2,  /* (66) bufsize ::= BUFSIZE INTEGER */
    0,  /* (67) pps ::= */
   -2,  /* (68) pps ::= PPS INTEGER */
    0,  /* (69) tseries ::= */
   -2,  /* (70) tseries ::= TSERIES INTEGER */
    0,  /* (71) dbs ::= */
   -2,  /* (72) dbs ::= DBS INTEGER */
    0,  /* (73) streams ::= */
   -2,  /* (74) streams ::= STREAMS INTEGER */
    0,  /* (75) storage ::= */
   -2,  /* (76) storage ::= STORAGE INTEGER */
    0,  /* (77) qtime ::= */
   -2,  /* (78) qtime ::= QTIME INTEGER */
    0,  /* (79) users ::= */
   -2,  /* (80) users ::= USERS INTEGER */
    0,  /* (81) conns ::= */
   -2,  /* (82) conns ::= CONNS INTEGER */
    0,  /* (83) state ::= */
   -2,  /* (84) state ::= STATE ids */
   -9,  /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (86) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (87) intitemlist ::= intitem */
   -1,  /* (88) intitem ::= INTEGER */
   -2,  /* (89) keep ::= KEEP intitemlist */
   -2,  /* (90) cache ::= CACHE INTEGER */
   -2,  /* (91) replica ::= REPLICA INTEGER */
   -2,  /* (92) quorum ::= QUORUM INTEGER */
   -2,  /* (93) days ::= DAYS INTEGER */
   -2,  /* (94) minrows ::= MINROWS INTEGER */
   -2,  /* (95) maxrows ::= MAXROWS INTEGER */
   -2,  /* (96) blocks ::= BLOCKS INTEGER */
   -2,  /* (97) ctime ::= CTIME INTEGER */
   -2,  /* (98) wal ::= WAL INTEGER */
   -2,  /* (99) fsync ::= FSYNC INTEGER */
   -2,  /* (100) comp ::= COMP INTEGER */
   -2,  /* (101) prec ::= PRECISION STRING */
   -2,  /* (102) update ::= UPDATE INTEGER */
   -2,  /* (103) cachelast ::= CACHELAST INTEGER */
   -2,  /* (104) partitions ::= PARTITIONS INTEGER */
    0,  /* (105) db_optr ::= */
   -2,  /* (106) db_optr ::= db_optr cache */
   -2,  /* (107) db_optr ::= db_optr replica */
   -2,  /* (108) db_optr ::= db_optr quorum */
   -2,  /* (109) db_optr ::= db_optr days */
   -2,  /* (110) db_optr ::= db_optr minrows */
   -2,  /* (111) db_optr ::= db_optr maxrows */
   -2,  /* (112) db_optr ::= db_optr blocks */
   -2,  /* (113) db_optr ::= db_optr ctime */
   -2,  /* (114) db_optr ::= db_optr wal */
   -2,  /* (115) db_optr ::= db_optr fsync */
   -2,  /* (116) db_optr ::= db_optr comp */
   -2,  /* (117) db_optr ::= db_optr prec */
   -2,  /* (118) db_optr ::= db_optr keep */
   -2,  /* (119) db_optr ::= db_optr update */
   -2,  /* (120) db_optr ::= db_optr cachelast */
   -1,  /* (121) topic_optr ::= db_optr */
   -2,  /* (122) topic_optr ::= topic_optr partitions */
    0,  /* (123) alter_db_optr ::= */
   -2,  /* (124) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (125) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (126) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (127) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (128) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (129) alter_db_optr ::= alter_db_optr update */
   -2,  /* (130) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (131) alter_topic_optr ::= alter_db_optr */
   -2,  /* (132) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (133) typename ::= ids */
   -4,  /* (134) typename ::= ids LP signed RP */
   -2,  /* (135) typename ::= ids UNSIGNED */
   -1,  /* (136) signed ::= INTEGER */
   -2,  /* (137) signed ::= PLUS INTEGER */
   -2,  /* (138) signed ::= MINUS INTEGER */
   -3,  /* (139) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (140) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (141) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (142) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (143) create_table_list ::= create_from_stable */
   -2,  /* (144) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (149) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (150) tagNamelist ::= ids */
   -5,  /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (152) columnlist ::= columnlist COMMA column */
   -1,  /* (153) columnlist ::= column */
   -2,  /* (154) column ::= ids typename */
   -3,  /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (156) tagitemlist ::= tagitem */
   -1,  /* (157) tagitem ::= INTEGER */
   -1,  /* (158) tagitem ::= FLOAT */
   -1,  /* (159) tagitem ::= STRING */
   -1,  /* (160) tagitem ::= BOOL */
   -1,  /* (161) tagitem ::= NULL */
   -1,  /* (162) tagitem ::= NOW */
   -2,  /* (163) tagitem ::= MINUS INTEGER */
   -2,  /* (164) tagitem ::= MINUS FLOAT */
   -2,  /* (165) tagitem ::= PLUS INTEGER */
   -2,  /* (166) tagitem ::= PLUS FLOAT */
  -14,  /* (167) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (168) select ::= LP select RP */
   -1,  /* (169) union ::= select */
   -4,  /* (170) union ::= union UNION ALL select */
   -1,  /* (171) cmd ::= union */
   -2,  /* (172) select ::= SELECT selcollist */
   -2,  /* (173) sclp ::= selcollist COMMA */
    0,  /* (174) sclp ::= */
   -4,  /* (175) selcollist ::= sclp distinct expr as */
   -2,  /* (176) selcollist ::= sclp STAR */
   -2,  /* (177) as ::= AS ids */
   -1,  /* (178) as ::= ids */
    0,  /* (179) as ::= */
   -1,  /* (180) distinct ::= DISTINCT */
    0,  /* (181) distinct ::= */
   -2,  /* (182) from ::= FROM tablelist */
   -2,  /* (183) from ::= FROM sub */
   -3,  /* (184) sub ::= LP union RP */
   -4,  /* (185) sub ::= LP union RP ids */
   -6,  /* (186) sub ::= sub COMMA LP union RP ids */
   -2,  /* (187) tablelist ::= ids cpxName */
   -3,  /* (188) tablelist ::= ids cpxName ids */
   -4,  /* (189) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (191) tmvar ::= VARIABLE */
   -4,  /* (192) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (193) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (194) interval_option ::= */
   -1,  /* (195) intervalKey ::= INTERVAL */
   -1,  /* (196) intervalKey ::= EVERY */
    0,  /* (197) session_option ::= */
   -7,  /* (198) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (199) windowstate_option ::= */
   -4,  /* (200) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (201) fill_opt ::= */
   -6,  /* (202) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (203) fill_opt ::= FILL LP ID RP */
   -4,  /* (204) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (205) sliding_opt ::= */
    0,  /* (206) orderby_opt ::= */
   -3,  /* (207) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (208) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (209) sortlist ::= item sortorder */
   -2,  /* (210) item ::= ids cpxName */
   -1,  /* (211) sortorder ::= ASC */
   -1,  /* (212) sortorder ::= DESC */
    0,  /* (213) sortorder ::= */
    0,  /* (214) groupby_opt ::= */
   -3,  /* (215) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (216) grouplist ::= grouplist COMMA item */
   -1,  /* (217) grouplist ::= item */
    0,  /* (218) having_opt ::= */
   -2,  /* (219) having_opt ::= HAVING expr */
    0,  /* (220) limit_opt ::= */
   -2,  /* (221) limit_opt ::= LIMIT signed */
   -4,  /* (222) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (223) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (224) slimit_opt ::= */
   -2,  /* (225) slimit_opt ::= SLIMIT signed */
   -4,  /* (226) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (227) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (228) where_opt ::= */
   -2,  /* (229) where_opt ::= WHERE expr */
   -3,  /* (230) expr ::= LP expr RP */
   -1,  /* (231) expr ::= ID */
   -3,  /* (232) expr ::= ID DOT ID */
   -3,  /* (233) expr ::= ID DOT STAR */
   -1,  /* (234) expr ::= INTEGER */
   -2,  /* (235) expr ::= MINUS INTEGER */
   -2,  /* (236) expr ::= PLUS INTEGER */
   -1,  /* (237) expr ::= FLOAT */
   -2,  /* (238) expr ::= MINUS FLOAT */
   -2,  /* (239) expr ::= PLUS FLOAT */
   -1,  /* (240) expr ::= STRING */
   -1,  /* (241) expr ::= NOW */
   -1,  /* (242) expr ::= VARIABLE */
   -2,  /* (243) expr ::= PLUS VARIABLE */
   -2,  /* (244) expr ::= MINUS VARIABLE */
   -1,  /* (245) expr ::= BOOL */
   -1,  /* (246) expr ::= NULL */
   -4,  /* (247) expr ::= ID LP exprlist RP */
   -4,  /* (248) expr ::= ID LP STAR RP */
   -3,  /* (249) expr ::= expr IS NULL */
   -4,  /* (250) expr ::= expr IS NOT NULL */
   -3,  /* (251) expr ::= expr LT expr */
   -3,  /* (252) expr ::= expr GT expr */
   -3,  /* (253) expr ::= expr LE expr */
   -3,  /* (254) expr ::= expr GE expr */
   -3,  /* (255) expr ::= expr NE expr */
   -3,  /* (256) expr ::= expr EQ expr */
   -5,  /* (257) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (258) expr ::= expr AND expr */
   -3,  /* (259) expr ::= expr OR expr */
   -3,  /* (260) expr ::= expr PLUS expr */
   -3,  /* (261) expr ::= expr MINUS expr */
   -3,  /* (262) expr ::= expr STAR expr */
   -3,  /* (263) expr ::= expr SLASH expr */
   -3,  /* (264) expr ::= expr REM expr */
   -3,  /* (265) expr ::= expr LIKE expr */
   -3,  /* (266) expr ::= expr MATCH expr */
   -5,  /* (267) expr ::= expr IN LP exprlist RP */
   -3,  /* (268) exprlist ::= exprlist COMMA expritem */
   -1,  /* (269) exprlist ::= expritem */
   -1,  /* (270) expritem ::= expr */
    0,  /* (271) expritem ::= */
   -3,  /* (272) cmd ::= RESET QUERY CACHE */
   -3,  /* (273) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (274) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (275) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (276) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (277) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (278) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (279) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (280) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (281) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (282) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (283) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (284) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (285) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (286) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (287) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (288) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (289) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (290) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (291) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (292) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 139: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==140);
      case 141: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==141);
#line 63 "sql.y"
{}
#line 2555 "sql.c"
        break;
      case 1: /* cmd ::= SHOW DATABASES */
#line 66 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
#line 2560 "sql.c"
        break;
      case 2: /* cmd ::= SHOW TOPICS */
#line 67 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
#line 2565 "sql.c"
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
#line 68 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
#line 2570 "sql.c"
        break;
      case 4: /* cmd ::= SHOW MNODES */
#line 69 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
#line 2575 "sql.c"
        break;
      case 5: /* cmd ::= SHOW DNODES */
#line 70 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
#line 2580 "sql.c"
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
#line 71 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
#line 2585 "sql.c"
        break;
      case 7: /* cmd ::= SHOW USERS */
#line 72 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
#line 2590 "sql.c"
        break;
      case 8: /* cmd ::= SHOW MODULES */
#line 74 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
#line 2595 "sql.c"
        break;
      case 9: /* cmd ::= SHOW QUERIES */
#line 75 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
#line 2600 "sql.c"
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
#line 76 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
#line 2605 "sql.c"
        break;
      case 11: /* cmd ::= SHOW STREAMS */
#line 77 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
#line 2610 "sql.c"
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
#line 78 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
#line 2615 "sql.c"
        break;
      case 13: /* cmd ::= SHOW SCORES */
#line 79 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
#line 2620 "sql.c"
        break;
      case 14: /* cmd ::= SHOW GRANTS */
#line 80 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
#line 2625 "sql.c"
        break;
      case 15: /* cmd ::= SHOW VNODES */
#line 82 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
#line 2630 "sql.c"
        break;
      case 16: /* cmd ::= SHOW VNODES ids */
#line 83 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
#line 2635 "sql.c"
        break;
      case 17: /* dbPrefix ::= */
#line 87 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
#line 2640 "sql.c"
        break;
      case 18: /* dbPrefix ::= ids DOT */
#line 88 "sql.y"
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
#line 2645 "sql.c"
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
#line 91 "sql.y"
{yymsp[1].minor.yy0.n = 0;  }
#line 2651 "sql.c"
        break;
      case 20: /* cpxName ::= DOT ids */
#line 92 "sql.y"
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
#line 2656 "sql.c"
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
#line 94 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2664 "sql.c"
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
#line 98 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2672 "sql.c"
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
#line 103 "sql.y"
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
#line 2679 "sql.c"
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
#line 107 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
#line 2686 "sql.c"
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
#line 111 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
#line 2693 "sql.c"
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
#line 115 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
#line 2700 "sql.c"
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
#line 119 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
#line 2709 "sql.c"
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
#line 125 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
#line 2718 "sql.c"
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
#line 131 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
#line 2727 "sql.c"
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
#line 138 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
#line 2735 "sql.c"
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
#line 144 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
#line 2743 "sql.c"
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
#line 149 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
#line 2748 "sql.c"
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
#line 150 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
#line 2753 "sql.c"
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
#line 151 "sql.y"
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
#line 2758 "sql.c"
        break;
      case 35: /* cmd ::= DROP DNODE ids */
#line 153 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
#line 2763 "sql.c"
        break;
      case 36: /* cmd ::= DROP USER ids */
#line 154 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
#line 2768 "sql.c"
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
#line 155 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
#line 2773 "sql.c"
        break;
      case 38: /* cmd ::= USE ids */
#line 158 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
#line 2778 "sql.c"
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
#line 161 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2787 "sql.c"
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
#line 170 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
#line 2792 "sql.c"
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
#line 171 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
#line 2797 "sql.c"
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
#line 172 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2802 "sql.c"
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
#line 173 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
#line 2807 "sql.c"
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
#line 174 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
#line 2812 "sql.c"
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
#line 175 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2817 "sql.c"
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 48: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==48);
#line 176 "sql.y"
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy246, &t);}
#line 2823 "sql.c"
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
#line 179 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy377);}
#line 2828 "sql.c"
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
#line 180 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy377);}
#line 2833 "sql.c"
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
#line 184 "sql.y"
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy403);}
#line 2838 "sql.c"
        break;
      case 52: /* ids ::= ID */
      case 53: /* ids ::= STRING */ yytestcase(yyruleno==53);
#line 190 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
#line 2844 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 54: /* ifexists ::= IF EXISTS */
#line 194 "sql.y"
{ yymsp[-1].minor.yy0.n = 1;}
#line 2850 "sql.c"
        break;
      case 55: /* ifexists ::= */
      case 57: /* ifnotexists ::= */ yytestcase(yyruleno==57);
      case 181: /* distinct ::= */ yytestcase(yyruleno==181);
#line 195 "sql.y"
{ yymsp[1].minor.yy0.n = 0;}
#line 2857 "sql.c"
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
#line 198 "sql.y"
{ yymsp[-2].minor.yy0.n = 1;}
#line 2862 "sql.c"
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
#line 203 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
#line 2867 "sql.c"
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
#line 205 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy377);}
#line 2872 "sql.c"
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
#line 206 "sql.y"
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy246, &yymsp[-2].minor.yy0);}
#line 2878 "sql.c"
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 208 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy363, &yymsp[0].minor.yy0, 1);}
#line 2883 "sql.c"
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 209 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy363, &yymsp[0].minor.yy0, 2);}
#line 2888 "sql.c"
        break;
      case 64: /* cmd ::= CREATE USER ids PASS ids */
#line 210 "sql.y"
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 2893 "sql.c"
        break;
      case 65: /* bufsize ::= */
      case 67: /* pps ::= */ yytestcase(yyruleno==67);
      case 69: /* tseries ::= */ yytestcase(yyruleno==69);
      case 71: /* dbs ::= */ yytestcase(yyruleno==71);
      case 73: /* streams ::= */ yytestcase(yyruleno==73);
      case 75: /* storage ::= */ yytestcase(yyruleno==75);
      case 77: /* qtime ::= */ yytestcase(yyruleno==77);
      case 79: /* users ::= */ yytestcase(yyruleno==79);
      case 81: /* conns ::= */ yytestcase(yyruleno==81);
      case 83: /* state ::= */ yytestcase(yyruleno==83);
#line 212 "sql.y"
{ yymsp[1].minor.yy0.n = 0;   }
#line 2907 "sql.c"
        break;
      case 66: /* bufsize ::= BUFSIZE INTEGER */
      case 68: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==70);
      case 72: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==76);
      case 78: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==78);
      case 80: /* users ::= USERS INTEGER */ yytestcase(yyruleno==80);
      case 82: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==82);
      case 84: /* state ::= STATE ids */ yytestcase(yyruleno==84);
#line 213 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
#line 2921 "sql.c"
        break;
      case 85: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
#line 243 "sql.y"
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
#line 2936 "sql.c"
  yymsp[-8].minor.yy377 = yylhsminor.yy377;
        break;
      case 86: /* intitemlist ::= intitemlist COMMA intitem */
      case 155: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==155);
#line 259 "sql.y"
{ yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);    }
#line 2943 "sql.c"
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
#line 260 "sql.y"
{ yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1); }
#line 2950 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
#line 262 "sql.y"
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
#line 2960 "sql.c"
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 89: /* keep ::= KEEP intitemlist */
#line 266 "sql.y"
{ yymsp[-1].minor.yy403 = yymsp[0].minor.yy403; }
#line 2966 "sql.c"
        break;
      case 90: /* cache ::= CACHE INTEGER */
      case 91: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==91);
      case 92: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==92);
      case 93: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==96);
      case 97: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==97);
      case 98: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==98);
      case 99: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==99);
      case 100: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==100);
      case 101: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==101);
      case 102: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==102);
      case 103: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==103);
      case 104: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==104);
#line 268 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
#line 2985 "sql.c"
        break;
      case 105: /* db_optr ::= */
#line 285 "sql.y"
{setDefaultCreateDbOption(&yymsp[1].minor.yy246); yymsp[1].minor.yy246.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 2990 "sql.c"
        break;
      case 106: /* db_optr ::= db_optr cache */
#line 287 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2995 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 107: /* db_optr ::= db_optr replica */
      case 124: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==124);
#line 288 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3002 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 108: /* db_optr ::= db_optr quorum */
      case 125: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==125);
#line 289 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3009 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 109: /* db_optr ::= db_optr days */
#line 290 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3015 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 110: /* db_optr ::= db_optr minrows */
#line 291 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3021 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 111: /* db_optr ::= db_optr maxrows */
#line 292 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3027 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 112: /* db_optr ::= db_optr blocks */
      case 127: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==127);
#line 293 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3034 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 113: /* db_optr ::= db_optr ctime */
#line 294 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3040 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 114: /* db_optr ::= db_optr wal */
#line 295 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3046 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 115: /* db_optr ::= db_optr fsync */
#line 296 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3052 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 116: /* db_optr ::= db_optr comp */
      case 128: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==128);
#line 297 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3059 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 117: /* db_optr ::= db_optr prec */
#line 298 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.precision = yymsp[0].minor.yy0; }
#line 3065 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 118: /* db_optr ::= db_optr keep */
      case 126: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==126);
#line 299 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.keep = yymsp[0].minor.yy403; }
#line 3072 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 119: /* db_optr ::= db_optr update */
      case 129: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==129);
#line 300 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3079 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 120: /* db_optr ::= db_optr cachelast */
      case 130: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==130);
#line 301 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3086 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 121: /* topic_optr ::= db_optr */
      case 131: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==131);
#line 305 "sql.y"
{ yylhsminor.yy246 = yymsp[0].minor.yy246; yylhsminor.yy246.dbType = TSDB_DB_TYPE_TOPIC; }
#line 3093 "sql.c"
  yymsp[0].minor.yy246 = yylhsminor.yy246;
        break;
      case 122: /* topic_optr ::= topic_optr partitions */
      case 132: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==132);
#line 306 "sql.y"
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3100 "sql.c"
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 123: /* alter_db_optr ::= */
#line 309 "sql.y"
{ setDefaultCreateDbOption(&yymsp[1].minor.yy246); yymsp[1].minor.yy246.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 3106 "sql.c"
        break;
      case 133: /* typename ::= ids */
#line 329 "sql.y"
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy363, &yymsp[0].minor.yy0);
}
#line 3114 "sql.c"
  yymsp[0].minor.yy363 = yylhsminor.yy363;
        break;
      case 134: /* typename ::= ids LP signed RP */
#line 335 "sql.y"
{
  if (yymsp[-1].minor.yy387 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy387;  // negative value of name length
    tSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  }
}
#line 3128 "sql.c"
  yymsp[-3].minor.yy363 = yylhsminor.yy363;
        break;
      case 135: /* typename ::= ids UNSIGNED */
#line 346 "sql.y"
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy363, &yymsp[-1].minor.yy0);
}
#line 3138 "sql.c"
  yymsp[-1].minor.yy363 = yylhsminor.yy363;
        break;
      case 136: /* signed ::= INTEGER */
#line 353 "sql.y"
{ yylhsminor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3144 "sql.c"
  yymsp[0].minor.yy387 = yylhsminor.yy387;
        break;
      case 137: /* signed ::= PLUS INTEGER */
#line 354 "sql.y"
{ yymsp[-1].minor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3150 "sql.c"
        break;
      case 138: /* signed ::= MINUS INTEGER */
#line 355 "sql.y"
{ yymsp[-1].minor.yy387 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
#line 3155 "sql.c"
        break;
      case 142: /* cmd ::= CREATE TABLE create_table_list */
#line 361 "sql.y"
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy56;}
#line 3160 "sql.c"
        break;
      case 143: /* create_table_list ::= create_from_stable */
#line 365 "sql.y"
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy84);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy56 = pCreateTable;
}
#line 3172 "sql.c"
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 144: /* create_table_list ::= create_table_list create_from_stable */
#line 374 "sql.y"
{
  taosArrayPush(yymsp[-1].minor.yy56->childTableInfo, &yymsp[0].minor.yy84);
  yylhsminor.yy56 = yymsp[-1].minor.yy56;
}
#line 3181 "sql.c"
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
#line 380 "sql.y"
{
  yylhsminor.yy56 = tSetCreateTableInfo(yymsp[-1].minor.yy403, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
#line 3193 "sql.c"
  yymsp[-5].minor.yy56 = yylhsminor.yy56;
        break;
      case 146: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
#line 390 "sql.y"
{
  yylhsminor.yy56 = tSetCreateTableInfo(yymsp[-5].minor.yy403, yymsp[-1].minor.yy403, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3205 "sql.c"
  yymsp[-9].minor.yy56 = yylhsminor.yy56;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
#line 401 "sql.y"
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy84 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy403, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3215 "sql.c"
  yymsp[-9].minor.yy84 = yylhsminor.yy84;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
#line 407 "sql.y"
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy84 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy403, yymsp[-1].minor.yy403, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
#line 3225 "sql.c"
  yymsp[-12].minor.yy84 = yylhsminor.yy84;
        break;
      case 149: /* tagNamelist ::= tagNamelist COMMA ids */
#line 415 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy403, &yymsp[0].minor.yy0); yylhsminor.yy403 = yymsp[-2].minor.yy403;  }
#line 3231 "sql.c"
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 150: /* tagNamelist ::= ids */
#line 416 "sql.y"
{yylhsminor.yy403 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy403, &yymsp[0].minor.yy0);}
#line 3237 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 151: /* create_table_args ::= ifnotexists ids cpxName AS select */
#line 420 "sql.y"
{
  yylhsminor.yy56 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy224, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
#line 3249 "sql.c"
  yymsp[-4].minor.yy56 = yylhsminor.yy56;
        break;
      case 152: /* columnlist ::= columnlist COMMA column */
#line 431 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy403, &yymsp[0].minor.yy363); yylhsminor.yy403 = yymsp[-2].minor.yy403;  }
#line 3255 "sql.c"
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 153: /* columnlist ::= column */
#line 432 "sql.y"
{yylhsminor.yy403 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy403, &yymsp[0].minor.yy363);}
#line 3261 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 154: /* column ::= ids typename */
#line 436 "sql.y"
{
  tSetColumnInfo(&yylhsminor.yy363, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy363);
}
#line 3269 "sql.c"
  yymsp[-1].minor.yy363 = yylhsminor.yy363;
        break;
      case 161: /* tagitem ::= NULL */
#line 451 "sql.y"
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
#line 3275 "sql.c"
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 162: /* tagitem ::= NOW */
#line 452 "sql.y"
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0);}
#line 3281 "sql.c"
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 163: /* tagitem ::= MINUS INTEGER */
      case 164: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==166);
#line 454 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
#line 3295 "sql.c"
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
#line 485 "sql.y"
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy403, yymsp[-11].minor.yy114, yymsp[-10].minor.yy260, yymsp[-4].minor.yy403, yymsp[-2].minor.yy403, &yymsp[-9].minor.yy222, &yymsp[-7].minor.yy365, &yymsp[-6].minor.yy544, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy403, &yymsp[0].minor.yy404, &yymsp[-1].minor.yy404, yymsp[-3].minor.yy260);
}
#line 3303 "sql.c"
  yymsp[-13].minor.yy224 = yylhsminor.yy224;
        break;
      case 168: /* select ::= LP select RP */
#line 489 "sql.y"
{yymsp[-2].minor.yy224 = yymsp[-1].minor.yy224;}
#line 3309 "sql.c"
        break;
      case 169: /* union ::= select */
#line 493 "sql.y"
{ yylhsminor.yy403 = setSubclause(NULL, yymsp[0].minor.yy224); }
#line 3314 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 170: /* union ::= union UNION ALL select */
#line 494 "sql.y"
{ yylhsminor.yy403 = appendSelectClause(yymsp[-3].minor.yy403, yymsp[0].minor.yy224); }
#line 3320 "sql.c"
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 171: /* cmd ::= union */
#line 496 "sql.y"
{ setSqlInfo(pInfo, yymsp[0].minor.yy403, NULL, TSDB_SQL_SELECT); }
#line 3326 "sql.c"
        break;
      case 172: /* select ::= SELECT selcollist */
#line 503 "sql.y"
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy403, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
#line 3333 "sql.c"
  yymsp[-1].minor.yy224 = yylhsminor.yy224;
        break;
      case 173: /* sclp ::= selcollist COMMA */
#line 515 "sql.y"
{yylhsminor.yy403 = yymsp[-1].minor.yy403;}
#line 3339 "sql.c"
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 174: /* sclp ::= */
      case 206: /* orderby_opt ::= */ yytestcase(yyruleno==206);
#line 516 "sql.y"
{yymsp[1].minor.yy403 = 0;}
#line 3346 "sql.c"
        break;
      case 175: /* selcollist ::= sclp distinct expr as */
#line 517 "sql.y"
{
   yylhsminor.yy403 = tSqlExprListAppend(yymsp[-3].minor.yy403, yymsp[-1].minor.yy260,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
#line 3353 "sql.c"
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 176: /* selcollist ::= sclp STAR */
#line 521 "sql.y"
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy403 = tSqlExprListAppend(yymsp[-1].minor.yy403, pNode, 0, 0);
}
#line 3362 "sql.c"
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 177: /* as ::= AS ids */
#line 529 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
#line 3368 "sql.c"
        break;
      case 178: /* as ::= ids */
#line 530 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
#line 3373 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* as ::= */
#line 531 "sql.y"
{ yymsp[1].minor.yy0.n = 0;  }
#line 3379 "sql.c"
        break;
      case 180: /* distinct ::= DISTINCT */
#line 534 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
#line 3384 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* from ::= FROM tablelist */
      case 183: /* from ::= FROM sub */ yytestcase(yyruleno==183);
#line 540 "sql.y"
{yymsp[-1].minor.yy114 = yymsp[0].minor.yy114;}
#line 3391 "sql.c"
        break;
      case 184: /* sub ::= LP union RP */
#line 545 "sql.y"
{yymsp[-2].minor.yy114 = addSubqueryElem(NULL, yymsp[-1].minor.yy403, NULL);}
#line 3396 "sql.c"
        break;
      case 185: /* sub ::= LP union RP ids */
#line 546 "sql.y"
{yymsp[-3].minor.yy114 = addSubqueryElem(NULL, yymsp[-2].minor.yy403, &yymsp[0].minor.yy0);}
#line 3401 "sql.c"
        break;
      case 186: /* sub ::= sub COMMA LP union RP ids */
#line 547 "sql.y"
{yylhsminor.yy114 = addSubqueryElem(yymsp[-5].minor.yy114, yymsp[-2].minor.yy403, &yymsp[0].minor.yy0);}
#line 3406 "sql.c"
  yymsp[-5].minor.yy114 = yylhsminor.yy114;
        break;
      case 187: /* tablelist ::= ids cpxName */
#line 551 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
#line 3415 "sql.c"
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 188: /* tablelist ::= ids cpxName ids */
#line 556 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3424 "sql.c"
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName */
#line 561 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(yymsp[-3].minor.yy114, &yymsp[-1].minor.yy0, NULL);
}
#line 3433 "sql.c"
  yymsp[-3].minor.yy114 = yylhsminor.yy114;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName ids */
#line 566 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(yymsp[-4].minor.yy114, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3442 "sql.c"
  yymsp[-4].minor.yy114 = yylhsminor.yy114;
        break;
      case 191: /* tmvar ::= VARIABLE */
#line 573 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
#line 3448 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* interval_option ::= intervalKey LP tmvar RP */
#line 576 "sql.y"
{yylhsminor.yy222.interval = yymsp[-1].minor.yy0; yylhsminor.yy222.offset.n = 0; yylhsminor.yy222.token = yymsp[-3].minor.yy202;}
#line 3454 "sql.c"
  yymsp[-3].minor.yy222 = yylhsminor.yy222;
        break;
      case 193: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
#line 577 "sql.y"
{yylhsminor.yy222.interval = yymsp[-3].minor.yy0; yylhsminor.yy222.offset = yymsp[-1].minor.yy0;   yylhsminor.yy222.token = yymsp[-5].minor.yy202;}
#line 3460 "sql.c"
  yymsp[-5].minor.yy222 = yylhsminor.yy222;
        break;
      case 194: /* interval_option ::= */
#line 578 "sql.y"
{memset(&yymsp[1].minor.yy222, 0, sizeof(yymsp[1].minor.yy222));}
#line 3466 "sql.c"
        break;
      case 195: /* intervalKey ::= INTERVAL */
#line 581 "sql.y"
{yymsp[0].minor.yy202 = TK_INTERVAL;}
#line 3471 "sql.c"
        break;
      case 196: /* intervalKey ::= EVERY */
#line 582 "sql.y"
{yymsp[0].minor.yy202 = TK_EVERY;   }
#line 3476 "sql.c"
        break;
      case 197: /* session_option ::= */
#line 585 "sql.y"
{yymsp[1].minor.yy365.col.n = 0; yymsp[1].minor.yy365.gap.n = 0;}
#line 3481 "sql.c"
        break;
      case 198: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
#line 586 "sql.y"
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy365.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy365.gap = yymsp[-1].minor.yy0;
}
#line 3490 "sql.c"
        break;
      case 199: /* windowstate_option ::= */
#line 593 "sql.y"
{ yymsp[1].minor.yy544.col.n = 0; yymsp[1].minor.yy544.col.z = NULL;}
#line 3495 "sql.c"
        break;
      case 200: /* windowstate_option ::= STATE_WINDOW LP ids RP */
#line 594 "sql.y"
{ yymsp[-3].minor.yy544.col = yymsp[-1].minor.yy0; }
#line 3500 "sql.c"
        break;
      case 201: /* fill_opt ::= */
#line 598 "sql.y"
{ yymsp[1].minor.yy403 = 0;     }
#line 3505 "sql.c"
        break;
      case 202: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
#line 599 "sql.y"
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy403, &A, -1, 0);
    yymsp[-5].minor.yy403 = yymsp[-1].minor.yy403;
}
#line 3517 "sql.c"
        break;
      case 203: /* fill_opt ::= FILL LP ID RP */
#line 608 "sql.y"
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy403 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
#line 3525 "sql.c"
        break;
      case 204: /* sliding_opt ::= SLIDING LP tmvar RP */
#line 614 "sql.y"
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
#line 3530 "sql.c"
        break;
      case 205: /* sliding_opt ::= */
#line 615 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
#line 3535 "sql.c"
        break;
      case 207: /* orderby_opt ::= ORDER BY sortlist */
#line 627 "sql.y"
{yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
#line 3540 "sql.c"
        break;
      case 208: /* sortlist ::= sortlist COMMA item sortorder */
#line 629 "sql.y"
{
    yylhsminor.yy403 = tVariantListAppend(yymsp[-3].minor.yy403, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
#line 3547 "sql.c"
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 209: /* sortlist ::= item sortorder */
#line 633 "sql.y"
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
#line 3555 "sql.c"
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 210: /* item ::= ids cpxName */
#line 638 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
#line 3566 "sql.c"
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 211: /* sortorder ::= ASC */
#line 646 "sql.y"
{ yymsp[0].minor.yy70 = TSDB_ORDER_ASC; }
#line 3572 "sql.c"
        break;
      case 212: /* sortorder ::= DESC */
#line 647 "sql.y"
{ yymsp[0].minor.yy70 = TSDB_ORDER_DESC;}
#line 3577 "sql.c"
        break;
      case 213: /* sortorder ::= */
#line 648 "sql.y"
{ yymsp[1].minor.yy70 = TSDB_ORDER_ASC; }
#line 3582 "sql.c"
        break;
      case 214: /* groupby_opt ::= */
#line 656 "sql.y"
{ yymsp[1].minor.yy403 = 0;}
#line 3587 "sql.c"
        break;
      case 215: /* groupby_opt ::= GROUP BY grouplist */
#line 657 "sql.y"
{ yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
#line 3592 "sql.c"
        break;
      case 216: /* grouplist ::= grouplist COMMA item */
#line 659 "sql.y"
{
  yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);
}
#line 3599 "sql.c"
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 217: /* grouplist ::= item */
#line 663 "sql.y"
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1);
}
#line 3607 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 218: /* having_opt ::= */
      case 228: /* where_opt ::= */ yytestcase(yyruleno==228);
      case 271: /* expritem ::= */ yytestcase(yyruleno==271);
#line 670 "sql.y"
{yymsp[1].minor.yy260 = 0;}
#line 3615 "sql.c"
        break;
      case 219: /* having_opt ::= HAVING expr */
      case 229: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==229);
#line 671 "sql.y"
{yymsp[-1].minor.yy260 = yymsp[0].minor.yy260;}
#line 3621 "sql.c"
        break;
      case 220: /* limit_opt ::= */
      case 224: /* slimit_opt ::= */ yytestcase(yyruleno==224);
#line 675 "sql.y"
{yymsp[1].minor.yy404.limit = -1; yymsp[1].minor.yy404.offset = 0;}
#line 3627 "sql.c"
        break;
      case 221: /* limit_opt ::= LIMIT signed */
      case 225: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==225);
#line 676 "sql.y"
{yymsp[-1].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-1].minor.yy404.offset = 0;}
#line 3633 "sql.c"
        break;
      case 222: /* limit_opt ::= LIMIT signed OFFSET signed */
#line 678 "sql.y"
{ yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
#line 3638 "sql.c"
        break;
      case 223: /* limit_opt ::= LIMIT signed COMMA signed */
#line 680 "sql.y"
{ yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
#line 3643 "sql.c"
        break;
      case 226: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
#line 686 "sql.y"
{yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
#line 3648 "sql.c"
        break;
      case 227: /* slimit_opt ::= SLIMIT signed COMMA signed */
#line 688 "sql.y"
{yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
#line 3653 "sql.c"
        break;
      case 230: /* expr ::= LP expr RP */
#line 701 "sql.y"
{yylhsminor.yy260 = yymsp[-1].minor.yy260; yylhsminor.yy260->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy260->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
#line 3658 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 231: /* expr ::= ID */
#line 703 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
#line 3664 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 232: /* expr ::= ID DOT ID */
#line 704 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
#line 3670 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 233: /* expr ::= ID DOT STAR */
#line 705 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
#line 3676 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 234: /* expr ::= INTEGER */
#line 707 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
#line 3682 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 235: /* expr ::= MINUS INTEGER */
      case 236: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==236);
#line 708 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
#line 3689 "sql.c"
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 237: /* expr ::= FLOAT */
#line 710 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
#line 3695 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 238: /* expr ::= MINUS FLOAT */
      case 239: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==239);
#line 711 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
#line 3702 "sql.c"
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 240: /* expr ::= STRING */
#line 713 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
#line 3708 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 241: /* expr ::= NOW */
#line 714 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
#line 3714 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 242: /* expr ::= VARIABLE */
#line 715 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
#line 3720 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 243: /* expr ::= PLUS VARIABLE */
      case 244: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==244);
#line 716 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
#line 3727 "sql.c"
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 245: /* expr ::= BOOL */
#line 718 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
#line 3733 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 246: /* expr ::= NULL */
#line 719 "sql.y"
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
#line 3739 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 247: /* expr ::= ID LP exprlist RP */
#line 722 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy260 = tSqlExprCreateFunction(yymsp[-1].minor.yy403, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3745 "sql.c"
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 248: /* expr ::= ID LP STAR RP */
#line 725 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy260 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3751 "sql.c"
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 249: /* expr ::= expr IS NULL */
#line 728 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, NULL, TK_ISNULL);}
#line 3757 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 250: /* expr ::= expr IS NOT NULL */
#line 729 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-3].minor.yy260, NULL, TK_NOTNULL);}
#line 3763 "sql.c"
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 251: /* expr ::= expr LT expr */
#line 732 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LT);}
#line 3769 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 252: /* expr ::= expr GT expr */
#line 733 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_GT);}
#line 3775 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 253: /* expr ::= expr LE expr */
#line 734 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LE);}
#line 3781 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 254: /* expr ::= expr GE expr */
#line 735 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_GE);}
#line 3787 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 255: /* expr ::= expr NE expr */
#line 736 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_NE);}
#line 3793 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 256: /* expr ::= expr EQ expr */
#line 737 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_EQ);}
#line 3799 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 257: /* expr ::= expr BETWEEN expr AND expr */
#line 739 "sql.y"
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy260); yylhsminor.yy260 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy260, yymsp[-2].minor.yy260, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy260, TK_LE), TK_AND);}
#line 3805 "sql.c"
  yymsp[-4].minor.yy260 = yylhsminor.yy260;
        break;
      case 258: /* expr ::= expr AND expr */
#line 741 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_AND);}
#line 3811 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 259: /* expr ::= expr OR expr */
#line 742 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_OR); }
#line 3817 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 260: /* expr ::= expr PLUS expr */
#line 745 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_PLUS);  }
#line 3823 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 261: /* expr ::= expr MINUS expr */
#line 746 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_MINUS); }
#line 3829 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 262: /* expr ::= expr STAR expr */
#line 747 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_STAR);  }
#line 3835 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 263: /* expr ::= expr SLASH expr */
#line 748 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_DIVIDE);}
#line 3841 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 264: /* expr ::= expr REM expr */
#line 749 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_REM);   }
#line 3847 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 265: /* expr ::= expr LIKE expr */
#line 752 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LIKE);  }
#line 3853 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 266: /* expr ::= expr MATCH expr */
#line 755 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_MATCH);  }
#line 3859 "sql.c"
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 267: /* expr ::= expr IN LP exprlist RP */
#line 758 "sql.y"
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-4].minor.yy260, (tSqlExpr*)yymsp[-1].minor.yy403, TK_IN); }
#line 3865 "sql.c"
  yymsp[-4].minor.yy260 = yylhsminor.yy260;
        break;
      case 268: /* exprlist ::= exprlist COMMA expritem */
#line 766 "sql.y"
{yylhsminor.yy403 = tSqlExprListAppend(yymsp[-2].minor.yy403,yymsp[0].minor.yy260,0, 0);}
#line 3871 "sql.c"
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 269: /* exprlist ::= expritem */
#line 767 "sql.y"
{yylhsminor.yy403 = tSqlExprListAppend(0,yymsp[0].minor.yy260,0, 0);}
#line 3877 "sql.c"
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 270: /* expritem ::= expr */
#line 768 "sql.y"
{yylhsminor.yy260 = yymsp[0].minor.yy260;}
#line 3883 "sql.c"
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 272: /* cmd ::= RESET QUERY CACHE */
#line 772 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
#line 3889 "sql.c"
        break;
      case 273: /* cmd ::= SYNCDB ids REPLICA */
#line 775 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
#line 3894 "sql.c"
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
#line 778 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3903 "sql.c"
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
#line 784 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3916 "sql.c"
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
#line 794 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3925 "sql.c"
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
#line 801 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3934 "sql.c"
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
#line 806 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3947 "sql.c"
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
#line 816 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3963 "sql.c"
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
#line 829 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3977 "sql.c"
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
#line 840 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3986 "sql.c"
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
#line 847 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3995 "sql.c"
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
#line 853 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4008 "sql.c"
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
#line 863 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4017 "sql.c"
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
#line 870 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4026 "sql.c"
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
#line 875 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4039 "sql.c"
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
#line 885 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4055 "sql.c"
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
#line 898 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4069 "sql.c"
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
#line 909 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4078 "sql.c"
        break;
      case 290: /* cmd ::= KILL CONNECTION INTEGER */
#line 916 "sql.y"
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
#line 4083 "sql.c"
        break;
      case 291: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
#line 917 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
#line 4088 "sql.c"
        break;
      case 292: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
#line 918 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
#line 4093 "sql.c"
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
#line 37 "sql.y"

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
#line 4178 "sql.c"
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
#line 61 "sql.y"
#line 4205 "sql.c"
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
