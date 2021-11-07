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
#define YYNOCODE 280
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy78;
  SCreateTableSql* yy110;
  SLimitVal yy126;
  int yy130;
  SArray* yy135;
  SIntervalVal yy160;
  TAOS_FIELD yy181;
  SCreateDbInfo yy256;
  SWindowStateVal yy258;
  int32_t yy262;
  SCreateAcctInfo yy277;
  tVariant yy308;
  SRelationInfo* yy460;
  SSqlNode* yy488;
  SSessionWindowVal yy511;
  tSqlExpr* yy526;
  int64_t yy531;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             364
#define YYNRULE              292
#define YYNTOKEN             197
#define YY_MAX_SHIFT         363
#define YY_MIN_SHIFTREDUCE   572
#define YY_MAX_SHIFTREDUCE   863
#define YY_ERROR_ACTION      864
#define YY_ACCEPT_ACTION     865
#define YY_NO_ACTION         866
#define YY_MIN_REDUCE        867
#define YY_MAX_REDUCE        1158
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
#define YY_ACTTAB_COUNT (758)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   170,  624,  236,  624,  230,  361, 1023, 1045,  242,  625,
 /*    10 */   247,  625, 1023,   57,   58,  154,   61,   62,  281,   38,
 /*    20 */   250,   51,  624,   60,  319,   65,   63,   66,   64,  993,
 /*    30 */   625,  991,  992,   56,   55,  161,  994,   54,   53,   52,
 /*    40 */   995,  161,  996,  997,  865,  363, 1036,  573,  574,  575,
 /*    50 */   576,  577,  578,  579,  580,  581,  582,  583,  584,  585,
 /*    60 */   586,  362,  233,  232,  231,   57,   58, 1020,   61,   62,
 /*    70 */   208,  660,  250,   51, 1042,   60,  319,   65,   63,   66,
 /*    80 */    64, 1135, 1009,  279,  278,   56,   55,   80,   98,   54,
 /*    90 */    53,   52,   57,   58, 1036,   61,   62,  208,   86,  250,
 /*   100 */    51, 1017,   60,  319,   65,   63,   66,   64, 1134, 1084,
 /*   110 */   272,  291,   56,   55,  317, 1083,   54,   53,   52,   57,
 /*   120 */    59,  244,   61,   62, 1011, 1023,  250,   51,   95,   60,
 /*   130 */   319,   65,   63,   66,   64,   45,  802,   23, 1036,   56,
 /*   140 */    55,  161,  349,   54,   53,   52,   58,  243,   61,   62,
 /*   150 */   767,  768,  250,   51,  234,   60,  319,   65,   63,   66,
 /*   160 */    64, 1006, 1007,   35, 1010,   56,   55,  317,  246,   54,
 /*   170 */    53,   52,   44,  315,  356,  355,  314,  313,  312,  354,
 /*   180 */   311,  310,  309,  353,  308,  352,  351,  985,  973,  974,
 /*   190 */   975,  976,  977,  978,  979,  980,  981,  982,  983,  984,
 /*   200 */   986,  987,   61,   62,   24,  205,  250,   51,  264,   60,
 /*   210 */   319,   65,   63,   66,   64,   92,  206,  268,  267,   56,
 /*   220 */    55,  259,  211,   54,   53,   52,  249,  817,  208,  217,
 /*   230 */   806,  175,  809,  123,  812,  137,  136,  135,  216, 1135,
 /*   240 */   249,  817,  324,   86,  806,  349,  809,  259,  812,  808,
 /*   250 */   251,  811,   65,   63,   66,   64,  161,  176,  228,  229,
 /*   260 */    56,   55,  320,  916,   54,   53,   52,    5,   41,  179,
 /*   270 */   189,  624,  228,  229,  178,  104,  109,  100,  108,  625,
 /*   280 */    45,  732,  357,  954,  729,   38,  730,   38,  731,  121,
 /*   290 */   115,  126,  253,  304,   38,   16,  125,   15,  131,  134,
 /*   300 */   124,  737,  271,  738,   78,  258,   79,  128,   67,  259,
 /*   310 */   208,  224,  255,  256,  212,   38,  199,  197,  195, 1021,
 /*   320 */  1131, 1135,   67,  194,  141,  140,  139,  138,  293,  240,
 /*   330 */    91,  241,   38, 1020,   44, 1020,  356,  355,  328, 1130,
 /*   340 */  1008,  354, 1020,  818,  813,  353,   38,  352,  351,  708,
 /*   350 */   814,   38,   38,   29,   54,   53,   52,  818,  813,  329,
 /*   360 */    38,   56,   55, 1020,  814,   54,   53,   52,  254,  815,
 /*   370 */   252,   38,  327,  326,   38,   14,  330,  339,  338,   94,
 /*   380 */  1020,  260,  807,  257,  810,  334,  333,  152,  150,  149,
 /*   390 */   331,  360,  359,  146, 1020,  335,  926,  733,  734, 1020,
 /*   400 */  1019,    1,  177,  189,  336,  917,   93,  748, 1020,   97,
 /*   410 */   784,  745,  189,    3,  190,  337,  273,   71,  341, 1020,
 /*   420 */    81,   83, 1020,   84,   74,  764,  774,  775,  248,  718,
 /*   430 */    39,  296,  720,  298,  719,   34,  804,    9,  838,  156,
 /*   440 */    68,   26,  819,   39,  321,   39,   68,   96,   68,  623,
 /*   450 */     6,  114,   25,  113,   77,   18,   25,   17,  275,   72,
 /*   460 */   275,  133,  132,   25,   75, 1129,  269,  783,  299,  735,
 /*   470 */   226,  736,  805,   20,  227,   19,  120,   22,  119,   21,
 /*   480 */   209,  210,  213,  207,  214,  215,  707,  219,  220,  221,
 /*   490 */  1022,  218, 1154,  752, 1094,  204, 1146, 1093,  238, 1090,
 /*   500 */  1089,  239,  816,  340,  153, 1044, 1076,   48, 1055, 1052,
 /*   510 */  1037, 1053,  276, 1057, 1075,  155,  160,  287,  171, 1018,
 /*   520 */   151,  172, 1016,  173,  174,  343,  280,  235,  305,  162,
 /*   530 */   931, 1034,  167,  165,  163,  301,  302,  303,  763,  306,
 /*   540 */   282,  307,  164,  284,  166,  286,  290,  821,  274,   76,
 /*   550 */   294,   50,   46,   73,  202,  292,   42,  318,  925,  288,
 /*   560 */   325, 1153,  111, 1152, 1149,  180,  169,  332,  283, 1145,
 /*   570 */   117,   33,   49, 1144, 1141,  181,  951,   43,  122,   40,
 /*   580 */    47,  203,  913,  350,  127,  911,  129,  130,  909,  908,
 /*   590 */   261,  192,  193,  905,  904,  903,  902,  901,  900,  899,
 /*   600 */   196,  198,  896,  894,  892,  890,  200,  887,  201,  342,
 /*   610 */    82,   87,  285, 1077,  344,  345,  346,  347,  348,  358,
 /*   620 */   863,  263,  862,  225,  245,  300,  262,  265,  266,  861,
 /*   630 */   222,  844,  843,  270,   10,  105,  930,  929,  223,  275,
 /*   640 */   106,  295,  740,   30,   85,  277,   88,  765,  907,  906,
 /*   650 */   157,  158,  142,  183,  952,  184,  143,  186,  898,  182,
 /*   660 */   185,  187,  188,  144,  897,  989,  145,  889,  953,  888,
 /*   670 */     2,  776,    4,  770,  159,  168,   89,  237,  772,   90,
 /*   680 */   289,   31,  999,   11,   32,   12,   13,   27,  297,   28,
 /*   690 */    97,   99,  102,   36,  101,  638,   37,  103,  673,  671,
 /*   700 */   670,  669,  667,  666,  665,  662,  628,  316,  107,    7,
 /*   710 */   822,  820,  322,    8,  323,  710,   39,   69,  110,  112,
 /*   720 */    70,  709,  116,  118,  706,  654,  652,  644,  650,  646,
 /*   730 */   648,  642,  640,  676,  675,  674,  672,  668,  664,  663,
 /*   740 */   191,  590,  626,  588,  867,  866,  866,  866,  866,  866,
 /*   750 */   866,  866,  866,  866,  866,  866,  147,  148,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   254,    1,  246,    1,  200,  201,  250,  201,  246,    9,
 /*    10 */   207,    9,  250,   13,   14,  201,   16,   17,  272,  201,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  224,
 /*    30 */     9,  226,  227,   33,   34,  201,  231,   37,   38,   39,
 /*    40 */   235,  201,  237,  238,  198,  199,  248,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  264,  245,   62,   13,   14,  249,   16,   17,
 /*    70 */   267,    5,   20,   21,  268,   23,   24,   25,   26,   27,
 /*    80 */    28,  278,    0,  269,  270,   33,   34,   87,  208,   37,
 /*    90 */    38,   39,   13,   14,  248,   16,   17,  267,   83,   20,
 /*   100 */    21,  201,   23,   24,   25,   26,   27,   28,  278,  275,
 /*   110 */   264,  277,   33,   34,   85,  275,   37,   38,   39,   13,
 /*   120 */    14,  246,   16,   17,  244,  250,   20,   21,  208,   23,
 /*   130 */    24,   25,   26,   27,   28,  120,   84,  267,  248,   33,
 /*   140 */    34,  201,   91,   37,   38,   39,   14,  247,   16,   17,
 /*   150 */   126,  127,   20,   21,  264,   23,   24,   25,   26,   27,
 /*   160 */    28,  241,  242,  243,  244,   33,   34,   85,  207,   37,
 /*   170 */    38,   39,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   180 */   107,  108,  109,  110,  111,  112,  113,  224,  225,  226,
 /*   190 */   227,  228,  229,  230,  231,  232,  233,  234,  235,  236,
 /*   200 */   237,  238,   16,   17,   44,  267,   20,   21,  143,   23,
 /*   210 */    24,   25,   26,   27,   28,  275,  267,  152,  153,   33,
 /*   220 */    34,  201,   62,   37,   38,   39,    1,    2,  267,   69,
 /*   230 */     5,  211,    7,   79,    9,   75,   76,   77,   78,  278,
 /*   240 */     1,    2,   82,   83,    5,   91,    7,  201,    9,    5,
 /*   250 */   207,    7,   25,   26,   27,   28,  201,  211,   33,   34,
 /*   260 */    33,   34,   37,  206,   37,   38,   39,   63,   64,   65,
 /*   270 */   213,    1,   33,   34,   70,   71,   72,   73,   74,    9,
 /*   280 */   120,    2,  222,  223,    5,  201,    7,  201,    9,   63,
 /*   290 */    64,   65,   69,   89,  201,  146,   70,  148,   72,   73,
 /*   300 */    74,    5,  142,    7,  144,   69,  208,   81,   83,  201,
 /*   310 */   267,  151,   33,   34,  267,  201,   63,   64,   65,  211,
 /*   320 */   267,  278,   83,   70,   71,   72,   73,   74,  273,  245,
 /*   330 */   275,  245,  201,  249,   99,  249,  101,  102,  245,  267,
 /*   340 */   242,  106,  249,  118,  119,  110,  201,  112,  113,    5,
 /*   350 */   125,  201,  201,   83,   37,   38,   39,  118,  119,  245,
 /*   360 */   201,   33,   34,  249,  125,   37,   38,   39,  145,  125,
 /*   370 */   147,  201,  149,  150,  201,   83,  245,   33,   34,   87,
 /*   380 */   249,  145,    5,  147,    7,  149,  150,   63,   64,   65,
 /*   390 */   245,   66,   67,   68,  249,  245,  206,  118,  119,  249,
 /*   400 */   249,  209,  210,  213,  245,  206,  251,   37,  249,  117,
 /*   410 */    77,   98,  213,  204,  205,  245,   84,   98,  245,  249,
 /*   420 */   265,   84,  249,   84,   98,   84,   84,   84,   61,   84,
 /*   430 */    98,   84,   84,   84,   84,   83,    1,  124,   84,   98,
 /*   440 */    98,   98,   84,   98,   15,   98,   98,   98,   98,   84,
 /*   450 */    83,  146,   98,  148,   83,  146,   98,  148,  121,  140,
 /*   460 */   121,   79,   80,   98,  138,  267,  201,  134,  116,    5,
 /*   470 */   267,    7,   37,  146,  267,  148,  146,  146,  148,  148,
 /*   480 */   267,  267,  267,  267,  267,  267,  115,  267,  267,  267,
 /*   490 */   250,  267,  250,  123,  240,  267,  250,  240,  240,  240,
 /*   500 */   240,  240,  125,  240,  201,  201,  276,  266,  201,  201,
 /*   510 */   248,  201,  248,  201,  276,  201,  201,  201,  252,  248,
 /*   520 */    61,  201,  201,  201,  201,   51,  271,  271,   90,  262,
 /*   530 */   201,  263,  257,  259,  261,  201,  201,  201,  125,  201,
 /*   540 */   271,  201,  260,  271,  258,  128,  130,  118,  202,  137,
 /*   550 */   132,  136,  201,  139,  201,  135,  201,  201,  201,  129,
 /*   560 */   201,  201,  201,  201,  201,  201,  255,  201,  131,  201,
 /*   570 */   201,  253,  141,  201,  201,  201,  201,  201,   97,  201,
 /*   580 */   201,  201,  201,  114,  201,  201,  201,  201,  201,  201,
 /*   590 */   201,  201,  201,  201,  201,  201,  201,  201,  201,  201,
 /*   600 */   201,  201,  201,  201,  201,  201,  201,  201,  201,   96,
 /*   610 */   202,  202,  202,  202,   93,   95,   55,   94,   92,   85,
 /*   620 */     5,    5,    5,  202,  202,  202,  154,  154,    5,    5,
 /*   630 */   202,  101,  100,  143,   83,  208,  212,  212,  202,  121,
 /*   640 */   208,  116,   84,   83,  122,   98,   98,   84,  202,  202,
 /*   650 */    83,   83,  203,  219,  221,  215,  203,  216,  202,  220,
 /*   660 */   218,  217,  214,  203,  202,  239,  203,  202,  223,  202,
 /*   670 */   209,   84,  204,   84,   98,  256,   83,    1,   84,   83,
 /*   680 */    83,   98,  239,  133,   98,  133,   83,   83,  116,   83,
 /*   690 */   117,   79,   71,   88,   87,    5,   88,   87,    9,    5,
 /*   700 */     5,    5,    5,    5,    5,    5,   86,   15,   79,   83,
 /*   710 */   118,   84,   24,   83,   59,    5,   98,   16,  148,  148,
 /*   720 */    16,    5,  148,  148,   84,    5,    5,    5,    5,    5,
 /*   730 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */    98,   61,   86,   60,    0,  279,  279,  279,  279,  279,
 /*   750 */   279,  279,  279,  279,  279,  279,   21,   21,  279,  279,
 /*   760 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   770 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   780 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   790 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   800 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   810 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   820 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   830 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   840 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   850 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   860 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   870 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   880 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   890 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   900 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   910 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   920 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   930 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   940 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   950 */   279,  279,  279,  279,  279,
};
#define YY_SHIFT_COUNT    (363)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (744)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   160,   73,   73,  235,  235,   29,  225,  239,  239,  270,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,   21,    0,    2,  239,  279,  279,  279,   15,
 /*    30 */    15,   21,   21,   24,   21,   82,   21,   21,   21,   21,
 /*    40 */   154,   29,   51,   51,   66,  758,  758,  758,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  239,  239,  239,  239,  239,  239,  279,  279,
 /*    70 */   279,  344,  344,  344,  344,  344,  344,  344,   21,   21,
 /*    80 */    21,  370,   21,   21,   21,   15,   15,   21,   21,   21,
 /*    90 */    21,  333,  333,  313,   15,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   150 */    21,   21,   21,  459,  459,  459,  413,  413,  413,  413,
 /*   160 */   459,  459,  412,  414,  418,  415,  420,  416,  430,  417,
 /*   170 */   437,  431,  459,  459,  459,  438,  438,  469,   29,   29,
 /*   180 */   459,  459,  481,  513,  474,  521,  520,  561,  523,  526,
 /*   190 */   469,   66,  459,  459,  534,  534,  459,  534,  459,  534,
 /*   200 */   459,  459,  758,  758,   52,   79,   79,  106,   79,  132,
 /*   210 */   186,  204,  227,  227,  227,  227,  226,  253,  328,  328,
 /*   220 */   328,  328,  223,  236,   65,  292,  317,  317,  244,  377,
 /*   230 */   325,  324,  332,  337,  339,  341,  342,  343,  319,  326,
 /*   240 */   345,  347,  348,  349,  350,  352,  354,  358,  435,  367,
 /*   250 */   429,  365,  149,  305,  309,  296,  464,  327,  330,  371,
 /*   260 */   331,  382,  615,  472,  616,  617,  473,  623,  624,  530,
 /*   270 */   532,  490,  518,  525,  551,  522,  558,  560,  547,  548,
 /*   280 */   563,  567,  587,  568,  589,  576,  593,  594,  596,  676,
 /*   290 */   597,  583,  550,  586,  552,  603,  525,  604,  572,  606,
 /*   300 */   573,  612,  605,  607,  621,  690,  608,  610,  689,  694,
 /*   310 */   695,  696,  697,  698,  699,  700,  620,  692,  629,  626,
 /*   320 */   627,  592,  630,  688,  655,  701,  570,  571,  618,  618,
 /*   330 */   618,  618,  704,  574,  575,  618,  618,  618,  710,  716,
 /*   340 */   640,  618,  720,  721,  722,  723,  724,  725,  726,  727,
 /*   350 */   728,  729,  730,  731,  732,  733,  734,  642,  656,  735,
 /*   360 */   736,  680,  683,  744,
};
#define YY_REDUCE_COUNT (203)
#define YY_REDUCE_MIN   (-254)
#define YY_REDUCE_MAX   (468)
static const short yy_reduce_ofst[] = {
 /*     0 */  -154,  -37,  -37, -195, -195,  -80, -197,  -39,   43, -186,
 /*    10 */  -182, -166,   55,   84,   86,   93,  114,  131,  145,  150,
 /*    20 */   159,  170,  173, -194, -196, -170, -244, -238, -125, -202,
 /*    30 */  -110, -160,  -60, -254, -100, -120,   20,   46,  108,  151,
 /*    40 */    57,   98,  190,  199,   60,  155,  192,  209, -130,  -62,
 /*    50 */   -51,   47,   53,   72,  198,  203,  207,  213,  214,  215,
 /*    60 */   216,  217,  218,  220,  221,  222,  224,  228,  240,  242,
 /*    70 */   246,  254,  257,  258,  259,  260,  261,  263,  265,  303,
 /*    80 */   304,  241,  307,  308,  310,  262,  264,  312,  314,  315,
 /*    90 */   316,  230,  238,  266,  271,  320,  321,  322,  323,  329,
 /*   100 */   334,  335,  336,  338,  340,  351,  353,  355,  356,  357,
 /*   110 */   359,  360,  361,  362,  363,  364,  366,  368,  369,  372,
 /*   120 */   373,  374,  375,  376,  378,  379,  380,  381,  383,  384,
 /*   130 */   385,  386,  387,  388,  389,  390,  391,  392,  393,  394,
 /*   140 */   395,  396,  397,  398,  399,  400,  401,  402,  403,  404,
 /*   150 */   405,  406,  407,  346,  408,  409,  255,  256,  269,  272,
 /*   160 */   410,  411,  268,  267,  273,  282,  274,  286,  275,  419,
 /*   170 */   311,  318,  421,  422,  423,  424,  425,  426,  427,  432,
 /*   180 */   428,  436,  433,  439,  434,  440,  442,  441,  444,  448,
 /*   190 */   443,  445,  446,  447,  449,  453,  456,  460,  462,  463,
 /*   200 */   465,  467,  461,  468,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   864,  988,  927,  998,  914,  924, 1137, 1137, 1137,  864,
 /*    10 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*    20 */   864,  864,  864, 1046,  884, 1137,  864,  864,  864,  864,
 /*    30 */   864,  864,  864, 1061,  864,  924,  864,  864,  864,  864,
 /*    40 */   934,  924,  934,  934,  864, 1041,  972,  990,  864,  864,
 /*    50 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*    60 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*    70 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*    80 */   864, 1048, 1054, 1051,  864,  864,  864, 1056,  864,  864,
 /*    90 */   864, 1080, 1080, 1039,  864,  864,  864,  864,  864,  864,
 /*   100 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   110 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   120 */   864,  864,  864,  864,  864,  864,  864,  912,  864,  910,
 /*   130 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   140 */   864,  864,  864,  864,  864,  864,  895,  864,  864,  864,
 /*   150 */   864,  864,  864,  886,  886,  886,  864,  864,  864,  864,
 /*   160 */   886,  886, 1087, 1091, 1073, 1085, 1081, 1068, 1066, 1064,
 /*   170 */  1072, 1095,  886,  886,  886,  932,  932,  928,  924,  924,
 /*   180 */   886,  886,  950,  948,  946,  938,  944,  940,  942,  936,
 /*   190 */   915,  864,  886,  886,  922,  922,  886,  922,  886,  922,
 /*   200 */   886,  886,  972,  990,  864, 1096, 1086,  864, 1136, 1126,
 /*   210 */  1125,  864, 1132, 1124, 1123, 1122,  864,  864, 1118, 1121,
 /*   220 */  1120, 1119,  864,  864,  864,  864, 1128, 1127,  864,  864,
 /*   230 */   864,  864,  864,  864,  864,  864,  864,  864, 1092, 1088,
 /*   240 */   864,  864,  864,  864,  864,  864,  864,  864,  864, 1098,
 /*   250 */   864,  864,  864,  864,  864,  864,  864,  864,  864, 1000,
 /*   260 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   270 */   864,  864, 1038,  864,  864,  864,  864,  864, 1050, 1049,
 /*   280 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   290 */   864, 1082,  864, 1074,  864,  864, 1012,  864,  864,  864,
 /*   300 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   310 */   864,  864,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   320 */   864,  864,  864,  864,  864,  864,  864,  864, 1155, 1150,
 /*   330 */  1151, 1148,  864,  864,  864, 1147, 1142, 1143,  864,  864,
 /*   340 */   864, 1140,  864,  864,  864,  864,  864,  864,  864,  864,
 /*   350 */   864,  864,  864,  864,  864,  864,  864,  956,  864,  893,
 /*   360 */   891,  864,  882,  864,
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
    1,  /*       FILE => ID */
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
  /*  196 */ "FILE",
  /*  197 */ "error",
  /*  198 */ "program",
  /*  199 */ "cmd",
  /*  200 */ "dbPrefix",
  /*  201 */ "ids",
  /*  202 */ "cpxName",
  /*  203 */ "ifexists",
  /*  204 */ "alter_db_optr",
  /*  205 */ "alter_topic_optr",
  /*  206 */ "acct_optr",
  /*  207 */ "exprlist",
  /*  208 */ "ifnotexists",
  /*  209 */ "db_optr",
  /*  210 */ "topic_optr",
  /*  211 */ "typename",
  /*  212 */ "bufsize",
  /*  213 */ "pps",
  /*  214 */ "tseries",
  /*  215 */ "dbs",
  /*  216 */ "streams",
  /*  217 */ "storage",
  /*  218 */ "qtime",
  /*  219 */ "users",
  /*  220 */ "conns",
  /*  221 */ "state",
  /*  222 */ "intitemlist",
  /*  223 */ "intitem",
  /*  224 */ "keep",
  /*  225 */ "cache",
  /*  226 */ "replica",
  /*  227 */ "quorum",
  /*  228 */ "days",
  /*  229 */ "minrows",
  /*  230 */ "maxrows",
  /*  231 */ "blocks",
  /*  232 */ "ctime",
  /*  233 */ "wal",
  /*  234 */ "fsync",
  /*  235 */ "comp",
  /*  236 */ "prec",
  /*  237 */ "update",
  /*  238 */ "cachelast",
  /*  239 */ "partitions",
  /*  240 */ "signed",
  /*  241 */ "create_table_args",
  /*  242 */ "create_stable_args",
  /*  243 */ "create_table_list",
  /*  244 */ "create_from_stable",
  /*  245 */ "columnlist",
  /*  246 */ "tagitemlist",
  /*  247 */ "tagNamelist",
  /*  248 */ "select",
  /*  249 */ "column",
  /*  250 */ "tagitem",
  /*  251 */ "selcollist",
  /*  252 */ "from",
  /*  253 */ "where_opt",
  /*  254 */ "interval_option",
  /*  255 */ "sliding_opt",
  /*  256 */ "session_option",
  /*  257 */ "windowstate_option",
  /*  258 */ "fill_opt",
  /*  259 */ "groupby_opt",
  /*  260 */ "having_opt",
  /*  261 */ "orderby_opt",
  /*  262 */ "slimit_opt",
  /*  263 */ "limit_opt",
  /*  264 */ "union",
  /*  265 */ "sclp",
  /*  266 */ "distinct",
  /*  267 */ "expr",
  /*  268 */ "as",
  /*  269 */ "tablelist",
  /*  270 */ "sub",
  /*  271 */ "tmvar",
  /*  272 */ "intervalKey",
  /*  273 */ "sortlist",
  /*  274 */ "sortitem",
  /*  275 */ "item",
  /*  276 */ "sortorder",
  /*  277 */ "grouplist",
  /*  278 */ "expritem",
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
 /* 266 */ "expr ::= expr IN LP exprlist RP",
 /* 267 */ "exprlist ::= exprlist COMMA expritem",
 /* 268 */ "exprlist ::= expritem",
 /* 269 */ "expritem ::= expr",
 /* 270 */ "expritem ::=",
 /* 271 */ "cmd ::= RESET QUERY CACHE",
 /* 272 */ "cmd ::= SYNCDB ids REPLICA",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 289 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 290 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 291 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 207: /* exprlist */
    case 251: /* selcollist */
    case 265: /* sclp */
{
tSqlExprListDestroy((yypminor->yy135));
}
      break;
    case 222: /* intitemlist */
    case 224: /* keep */
    case 245: /* columnlist */
    case 246: /* tagitemlist */
    case 247: /* tagNamelist */
    case 258: /* fill_opt */
    case 259: /* groupby_opt */
    case 261: /* orderby_opt */
    case 273: /* sortlist */
    case 277: /* grouplist */
{
taosArrayDestroy((yypminor->yy135));
}
      break;
    case 243: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy110));
}
      break;
    case 248: /* select */
{
destroySqlNode((yypminor->yy488));
}
      break;
    case 252: /* from */
    case 269: /* tablelist */
    case 270: /* sub */
{
destroyRelationInfo((yypminor->yy460));
}
      break;
    case 253: /* where_opt */
    case 260: /* having_opt */
    case 267: /* expr */
    case 278: /* expritem */
{
tSqlExprDestroy((yypminor->yy526));
}
      break;
    case 264: /* union */
{
destroyAllSqlNode((yypminor->yy135));
}
      break;
    case 274: /* sortitem */
{
tVariantDestroy(&(yypminor->yy308));
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
  {  198,   -1 }, /* (0) program ::= cmd */
  {  199,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  199,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  199,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  199,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  199,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  199,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  199,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  199,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  199,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  199,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  199,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  199,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  199,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  199,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  199,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  199,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  200,    0 }, /* (17) dbPrefix ::= */
  {  200,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  202,    0 }, /* (19) cpxName ::= */
  {  202,   -2 }, /* (20) cpxName ::= DOT ids */
  {  199,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  199,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  199,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  199,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  199,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  199,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  199,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  199,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  199,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  199,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  199,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  199,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  199,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  199,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  199,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  199,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  199,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  199,   -2 }, /* (38) cmd ::= USE ids */
  {  199,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  199,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  199,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  199,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  199,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  199,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  199,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  199,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  199,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  199,   -4 }, /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  199,   -4 }, /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  199,   -6 }, /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  199,   -6 }, /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  201,   -1 }, /* (52) ids ::= ID */
  {  201,   -1 }, /* (53) ids ::= STRING */
  {  203,   -2 }, /* (54) ifexists ::= IF EXISTS */
  {  203,    0 }, /* (55) ifexists ::= */
  {  208,   -3 }, /* (56) ifnotexists ::= IF NOT EXISTS */
  {  208,    0 }, /* (57) ifnotexists ::= */
  {  199,   -3 }, /* (58) cmd ::= CREATE DNODE ids */
  {  199,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  199,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  199,   -5 }, /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  199,   -8 }, /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -9 }, /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -5 }, /* (64) cmd ::= CREATE USER ids PASS ids */
  {  212,    0 }, /* (65) bufsize ::= */
  {  212,   -2 }, /* (66) bufsize ::= BUFSIZE INTEGER */
  {  213,    0 }, /* (67) pps ::= */
  {  213,   -2 }, /* (68) pps ::= PPS INTEGER */
  {  214,    0 }, /* (69) tseries ::= */
  {  214,   -2 }, /* (70) tseries ::= TSERIES INTEGER */
  {  215,    0 }, /* (71) dbs ::= */
  {  215,   -2 }, /* (72) dbs ::= DBS INTEGER */
  {  216,    0 }, /* (73) streams ::= */
  {  216,   -2 }, /* (74) streams ::= STREAMS INTEGER */
  {  217,    0 }, /* (75) storage ::= */
  {  217,   -2 }, /* (76) storage ::= STORAGE INTEGER */
  {  218,    0 }, /* (77) qtime ::= */
  {  218,   -2 }, /* (78) qtime ::= QTIME INTEGER */
  {  219,    0 }, /* (79) users ::= */
  {  219,   -2 }, /* (80) users ::= USERS INTEGER */
  {  220,    0 }, /* (81) conns ::= */
  {  220,   -2 }, /* (82) conns ::= CONNS INTEGER */
  {  221,    0 }, /* (83) state ::= */
  {  221,   -2 }, /* (84) state ::= STATE ids */
  {  206,   -9 }, /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  222,   -3 }, /* (86) intitemlist ::= intitemlist COMMA intitem */
  {  222,   -1 }, /* (87) intitemlist ::= intitem */
  {  223,   -1 }, /* (88) intitem ::= INTEGER */
  {  224,   -2 }, /* (89) keep ::= KEEP intitemlist */
  {  225,   -2 }, /* (90) cache ::= CACHE INTEGER */
  {  226,   -2 }, /* (91) replica ::= REPLICA INTEGER */
  {  227,   -2 }, /* (92) quorum ::= QUORUM INTEGER */
  {  228,   -2 }, /* (93) days ::= DAYS INTEGER */
  {  229,   -2 }, /* (94) minrows ::= MINROWS INTEGER */
  {  230,   -2 }, /* (95) maxrows ::= MAXROWS INTEGER */
  {  231,   -2 }, /* (96) blocks ::= BLOCKS INTEGER */
  {  232,   -2 }, /* (97) ctime ::= CTIME INTEGER */
  {  233,   -2 }, /* (98) wal ::= WAL INTEGER */
  {  234,   -2 }, /* (99) fsync ::= FSYNC INTEGER */
  {  235,   -2 }, /* (100) comp ::= COMP INTEGER */
  {  236,   -2 }, /* (101) prec ::= PRECISION STRING */
  {  237,   -2 }, /* (102) update ::= UPDATE INTEGER */
  {  238,   -2 }, /* (103) cachelast ::= CACHELAST INTEGER */
  {  239,   -2 }, /* (104) partitions ::= PARTITIONS INTEGER */
  {  209,    0 }, /* (105) db_optr ::= */
  {  209,   -2 }, /* (106) db_optr ::= db_optr cache */
  {  209,   -2 }, /* (107) db_optr ::= db_optr replica */
  {  209,   -2 }, /* (108) db_optr ::= db_optr quorum */
  {  209,   -2 }, /* (109) db_optr ::= db_optr days */
  {  209,   -2 }, /* (110) db_optr ::= db_optr minrows */
  {  209,   -2 }, /* (111) db_optr ::= db_optr maxrows */
  {  209,   -2 }, /* (112) db_optr ::= db_optr blocks */
  {  209,   -2 }, /* (113) db_optr ::= db_optr ctime */
  {  209,   -2 }, /* (114) db_optr ::= db_optr wal */
  {  209,   -2 }, /* (115) db_optr ::= db_optr fsync */
  {  209,   -2 }, /* (116) db_optr ::= db_optr comp */
  {  209,   -2 }, /* (117) db_optr ::= db_optr prec */
  {  209,   -2 }, /* (118) db_optr ::= db_optr keep */
  {  209,   -2 }, /* (119) db_optr ::= db_optr update */
  {  209,   -2 }, /* (120) db_optr ::= db_optr cachelast */
  {  210,   -1 }, /* (121) topic_optr ::= db_optr */
  {  210,   -2 }, /* (122) topic_optr ::= topic_optr partitions */
  {  204,    0 }, /* (123) alter_db_optr ::= */
  {  204,   -2 }, /* (124) alter_db_optr ::= alter_db_optr replica */
  {  204,   -2 }, /* (125) alter_db_optr ::= alter_db_optr quorum */
  {  204,   -2 }, /* (126) alter_db_optr ::= alter_db_optr keep */
  {  204,   -2 }, /* (127) alter_db_optr ::= alter_db_optr blocks */
  {  204,   -2 }, /* (128) alter_db_optr ::= alter_db_optr comp */
  {  204,   -2 }, /* (129) alter_db_optr ::= alter_db_optr update */
  {  204,   -2 }, /* (130) alter_db_optr ::= alter_db_optr cachelast */
  {  205,   -1 }, /* (131) alter_topic_optr ::= alter_db_optr */
  {  205,   -2 }, /* (132) alter_topic_optr ::= alter_topic_optr partitions */
  {  211,   -1 }, /* (133) typename ::= ids */
  {  211,   -4 }, /* (134) typename ::= ids LP signed RP */
  {  211,   -2 }, /* (135) typename ::= ids UNSIGNED */
  {  240,   -1 }, /* (136) signed ::= INTEGER */
  {  240,   -2 }, /* (137) signed ::= PLUS INTEGER */
  {  240,   -2 }, /* (138) signed ::= MINUS INTEGER */
  {  199,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_args */
  {  199,   -3 }, /* (140) cmd ::= CREATE TABLE create_stable_args */
  {  199,   -3 }, /* (141) cmd ::= CREATE STABLE create_stable_args */
  {  199,   -3 }, /* (142) cmd ::= CREATE TABLE create_table_list */
  {  243,   -1 }, /* (143) create_table_list ::= create_from_stable */
  {  243,   -2 }, /* (144) create_table_list ::= create_table_list create_from_stable */
  {  241,   -6 }, /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  242,  -10 }, /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  244,  -10 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  244,  -13 }, /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  247,   -3 }, /* (149) tagNamelist ::= tagNamelist COMMA ids */
  {  247,   -1 }, /* (150) tagNamelist ::= ids */
  {  241,   -5 }, /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
  {  245,   -3 }, /* (152) columnlist ::= columnlist COMMA column */
  {  245,   -1 }, /* (153) columnlist ::= column */
  {  249,   -2 }, /* (154) column ::= ids typename */
  {  246,   -3 }, /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
  {  246,   -1 }, /* (156) tagitemlist ::= tagitem */
  {  250,   -1 }, /* (157) tagitem ::= INTEGER */
  {  250,   -1 }, /* (158) tagitem ::= FLOAT */
  {  250,   -1 }, /* (159) tagitem ::= STRING */
  {  250,   -1 }, /* (160) tagitem ::= BOOL */
  {  250,   -1 }, /* (161) tagitem ::= NULL */
  {  250,   -1 }, /* (162) tagitem ::= NOW */
  {  250,   -2 }, /* (163) tagitem ::= MINUS INTEGER */
  {  250,   -2 }, /* (164) tagitem ::= MINUS FLOAT */
  {  250,   -2 }, /* (165) tagitem ::= PLUS INTEGER */
  {  250,   -2 }, /* (166) tagitem ::= PLUS FLOAT */
  {  248,  -14 }, /* (167) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  248,   -3 }, /* (168) select ::= LP select RP */
  {  264,   -1 }, /* (169) union ::= select */
  {  264,   -4 }, /* (170) union ::= union UNION ALL select */
  {  199,   -1 }, /* (171) cmd ::= union */
  {  248,   -2 }, /* (172) select ::= SELECT selcollist */
  {  265,   -2 }, /* (173) sclp ::= selcollist COMMA */
  {  265,    0 }, /* (174) sclp ::= */
  {  251,   -4 }, /* (175) selcollist ::= sclp distinct expr as */
  {  251,   -2 }, /* (176) selcollist ::= sclp STAR */
  {  268,   -2 }, /* (177) as ::= AS ids */
  {  268,   -1 }, /* (178) as ::= ids */
  {  268,    0 }, /* (179) as ::= */
  {  266,   -1 }, /* (180) distinct ::= DISTINCT */
  {  266,    0 }, /* (181) distinct ::= */
  {  252,   -2 }, /* (182) from ::= FROM tablelist */
  {  252,   -2 }, /* (183) from ::= FROM sub */
  {  270,   -3 }, /* (184) sub ::= LP union RP */
  {  270,   -4 }, /* (185) sub ::= LP union RP ids */
  {  270,   -6 }, /* (186) sub ::= sub COMMA LP union RP ids */
  {  269,   -2 }, /* (187) tablelist ::= ids cpxName */
  {  269,   -3 }, /* (188) tablelist ::= ids cpxName ids */
  {  269,   -4 }, /* (189) tablelist ::= tablelist COMMA ids cpxName */
  {  269,   -5 }, /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
  {  271,   -1 }, /* (191) tmvar ::= VARIABLE */
  {  254,   -4 }, /* (192) interval_option ::= intervalKey LP tmvar RP */
  {  254,   -6 }, /* (193) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  254,    0 }, /* (194) interval_option ::= */
  {  272,   -1 }, /* (195) intervalKey ::= INTERVAL */
  {  272,   -1 }, /* (196) intervalKey ::= EVERY */
  {  256,    0 }, /* (197) session_option ::= */
  {  256,   -7 }, /* (198) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  257,    0 }, /* (199) windowstate_option ::= */
  {  257,   -4 }, /* (200) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  258,    0 }, /* (201) fill_opt ::= */
  {  258,   -6 }, /* (202) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  258,   -4 }, /* (203) fill_opt ::= FILL LP ID RP */
  {  255,   -4 }, /* (204) sliding_opt ::= SLIDING LP tmvar RP */
  {  255,    0 }, /* (205) sliding_opt ::= */
  {  261,    0 }, /* (206) orderby_opt ::= */
  {  261,   -3 }, /* (207) orderby_opt ::= ORDER BY sortlist */
  {  273,   -4 }, /* (208) sortlist ::= sortlist COMMA item sortorder */
  {  273,   -2 }, /* (209) sortlist ::= item sortorder */
  {  275,   -2 }, /* (210) item ::= ids cpxName */
  {  276,   -1 }, /* (211) sortorder ::= ASC */
  {  276,   -1 }, /* (212) sortorder ::= DESC */
  {  276,    0 }, /* (213) sortorder ::= */
  {  259,    0 }, /* (214) groupby_opt ::= */
  {  259,   -3 }, /* (215) groupby_opt ::= GROUP BY grouplist */
  {  277,   -3 }, /* (216) grouplist ::= grouplist COMMA item */
  {  277,   -1 }, /* (217) grouplist ::= item */
  {  260,    0 }, /* (218) having_opt ::= */
  {  260,   -2 }, /* (219) having_opt ::= HAVING expr */
  {  263,    0 }, /* (220) limit_opt ::= */
  {  263,   -2 }, /* (221) limit_opt ::= LIMIT signed */
  {  263,   -4 }, /* (222) limit_opt ::= LIMIT signed OFFSET signed */
  {  263,   -4 }, /* (223) limit_opt ::= LIMIT signed COMMA signed */
  {  262,    0 }, /* (224) slimit_opt ::= */
  {  262,   -2 }, /* (225) slimit_opt ::= SLIMIT signed */
  {  262,   -4 }, /* (226) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  262,   -4 }, /* (227) slimit_opt ::= SLIMIT signed COMMA signed */
  {  253,    0 }, /* (228) where_opt ::= */
  {  253,   -2 }, /* (229) where_opt ::= WHERE expr */
  {  267,   -3 }, /* (230) expr ::= LP expr RP */
  {  267,   -1 }, /* (231) expr ::= ID */
  {  267,   -3 }, /* (232) expr ::= ID DOT ID */
  {  267,   -3 }, /* (233) expr ::= ID DOT STAR */
  {  267,   -1 }, /* (234) expr ::= INTEGER */
  {  267,   -2 }, /* (235) expr ::= MINUS INTEGER */
  {  267,   -2 }, /* (236) expr ::= PLUS INTEGER */
  {  267,   -1 }, /* (237) expr ::= FLOAT */
  {  267,   -2 }, /* (238) expr ::= MINUS FLOAT */
  {  267,   -2 }, /* (239) expr ::= PLUS FLOAT */
  {  267,   -1 }, /* (240) expr ::= STRING */
  {  267,   -1 }, /* (241) expr ::= NOW */
  {  267,   -1 }, /* (242) expr ::= VARIABLE */
  {  267,   -2 }, /* (243) expr ::= PLUS VARIABLE */
  {  267,   -2 }, /* (244) expr ::= MINUS VARIABLE */
  {  267,   -1 }, /* (245) expr ::= BOOL */
  {  267,   -1 }, /* (246) expr ::= NULL */
  {  267,   -4 }, /* (247) expr ::= ID LP exprlist RP */
  {  267,   -4 }, /* (248) expr ::= ID LP STAR RP */
  {  267,   -3 }, /* (249) expr ::= expr IS NULL */
  {  267,   -4 }, /* (250) expr ::= expr IS NOT NULL */
  {  267,   -3 }, /* (251) expr ::= expr LT expr */
  {  267,   -3 }, /* (252) expr ::= expr GT expr */
  {  267,   -3 }, /* (253) expr ::= expr LE expr */
  {  267,   -3 }, /* (254) expr ::= expr GE expr */
  {  267,   -3 }, /* (255) expr ::= expr NE expr */
  {  267,   -3 }, /* (256) expr ::= expr EQ expr */
  {  267,   -5 }, /* (257) expr ::= expr BETWEEN expr AND expr */
  {  267,   -3 }, /* (258) expr ::= expr AND expr */
  {  267,   -3 }, /* (259) expr ::= expr OR expr */
  {  267,   -3 }, /* (260) expr ::= expr PLUS expr */
  {  267,   -3 }, /* (261) expr ::= expr MINUS expr */
  {  267,   -3 }, /* (262) expr ::= expr STAR expr */
  {  267,   -3 }, /* (263) expr ::= expr SLASH expr */
  {  267,   -3 }, /* (264) expr ::= expr REM expr */
  {  267,   -3 }, /* (265) expr ::= expr LIKE expr */
  {  267,   -5 }, /* (266) expr ::= expr IN LP exprlist RP */
  {  207,   -3 }, /* (267) exprlist ::= exprlist COMMA expritem */
  {  207,   -1 }, /* (268) exprlist ::= expritem */
  {  278,   -1 }, /* (269) expritem ::= expr */
  {  278,    0 }, /* (270) expritem ::= */
  {  199,   -3 }, /* (271) cmd ::= RESET QUERY CACHE */
  {  199,   -3 }, /* (272) cmd ::= SYNCDB ids REPLICA */
  {  199,   -7 }, /* (273) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (274) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (275) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (278) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (279) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -7 }, /* (281) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (282) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (283) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (286) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (287) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -3 }, /* (289) cmd ::= KILL CONNECTION INTEGER */
  {  199,   -5 }, /* (290) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  199,   -5 }, /* (291) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 139: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==140);
      case 141: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==141);
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
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 35: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 36: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 38: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 48: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==48);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy256, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy277);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy277);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy135);}
        break;
      case 52: /* ids ::= ID */
      case 53: /* ids ::= STRING */ yytestcase(yyruleno==53);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 54: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 55: /* ifexists ::= */
      case 57: /* ifnotexists ::= */ yytestcase(yyruleno==57);
      case 181: /* distinct ::= */ yytestcase(yyruleno==181);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy277);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy256, &yymsp[-2].minor.yy0);}
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy181, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy181, &yymsp[0].minor.yy0, 2);}
        break;
      case 64: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
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
{ yymsp[1].minor.yy0.n = 0;   }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 85: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy277.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy277.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy277.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy277.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy277.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy277.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy277.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy277.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy277.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy277 = yylhsminor.yy277;
        break;
      case 86: /* intitemlist ::= intitemlist COMMA intitem */
      case 155: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy135 = tVariantListAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy308, -1);    }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
{ yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[0].minor.yy308, -1); }
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy308, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy308 = yylhsminor.yy308;
        break;
      case 89: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy135 = yymsp[0].minor.yy135; }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 105: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy256); yymsp[1].minor.yy256.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 106: /* db_optr ::= db_optr cache */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 107: /* db_optr ::= db_optr replica */
      case 124: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==124);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 108: /* db_optr ::= db_optr quorum */
      case 125: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==125);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 109: /* db_optr ::= db_optr days */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 110: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 111: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 112: /* db_optr ::= db_optr blocks */
      case 127: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==127);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 113: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 114: /* db_optr ::= db_optr wal */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 115: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 116: /* db_optr ::= db_optr comp */
      case 128: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==128);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 117: /* db_optr ::= db_optr prec */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 118: /* db_optr ::= db_optr keep */
      case 126: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==126);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.keep = yymsp[0].minor.yy135; }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 119: /* db_optr ::= db_optr update */
      case 129: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==129);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 120: /* db_optr ::= db_optr cachelast */
      case 130: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==130);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 121: /* topic_optr ::= db_optr */
      case 131: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==131);
{ yylhsminor.yy256 = yymsp[0].minor.yy256; yylhsminor.yy256.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 122: /* topic_optr ::= topic_optr partitions */
      case 132: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==132);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 123: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy256); yymsp[1].minor.yy256.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 133: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy181, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy181 = yylhsminor.yy181;
        break;
      case 134: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy531 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy531;  // negative value of name length
    tSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy181 = yylhsminor.yy181;
        break;
      case 135: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy181, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 136: /* signed ::= INTEGER */
{ yylhsminor.yy531 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy531 = yylhsminor.yy531;
        break;
      case 137: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy531 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 138: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy531 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 142: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy110;}
        break;
      case 143: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy78);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy110 = pCreateTable;
}
  yymsp[0].minor.yy110 = yylhsminor.yy110;
        break;
      case 144: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy110->childTableInfo, &yymsp[0].minor.yy78);
  yylhsminor.yy110 = yymsp[-1].minor.yy110;
}
  yymsp[-1].minor.yy110 = yylhsminor.yy110;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy110 = tSetCreateTableInfo(yymsp[-1].minor.yy135, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy110, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy110 = yylhsminor.yy110;
        break;
      case 146: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy110 = tSetCreateTableInfo(yymsp[-5].minor.yy135, yymsp[-1].minor.yy135, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy110, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy110 = yylhsminor.yy110;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy78 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy135, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy78 = yylhsminor.yy78;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy78 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy135, yymsp[-1].minor.yy135, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy78 = yylhsminor.yy78;
        break;
      case 149: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy135, &yymsp[0].minor.yy0); yylhsminor.yy135 = yymsp[-2].minor.yy135;  }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 150: /* tagNamelist ::= ids */
{yylhsminor.yy135 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy135, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 151: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy110 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy488, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy110, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy110 = yylhsminor.yy110;
        break;
      case 152: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy135, &yymsp[0].minor.yy181); yylhsminor.yy135 = yymsp[-2].minor.yy135;  }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 153: /* columnlist ::= column */
{yylhsminor.yy135 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy135, &yymsp[0].minor.yy181);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 154: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy181, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy181);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 161: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy308, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy308 = yylhsminor.yy308;
        break;
      case 162: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy308, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy308 = yylhsminor.yy308;
        break;
      case 163: /* tagitem ::= MINUS INTEGER */
      case 164: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==166);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy308, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy308 = yylhsminor.yy308;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy488 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy135, yymsp[-11].minor.yy460, yymsp[-10].minor.yy526, yymsp[-4].minor.yy135, yymsp[-2].minor.yy135, &yymsp[-9].minor.yy160, &yymsp[-7].minor.yy511, &yymsp[-6].minor.yy258, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy135, &yymsp[0].minor.yy126, &yymsp[-1].minor.yy126, yymsp[-3].minor.yy526);
}
  yymsp[-13].minor.yy488 = yylhsminor.yy488;
        break;
      case 168: /* select ::= LP select RP */
{yymsp[-2].minor.yy488 = yymsp[-1].minor.yy488;}
        break;
      case 169: /* union ::= select */
{ yylhsminor.yy135 = setSubclause(NULL, yymsp[0].minor.yy488); }
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 170: /* union ::= union UNION ALL select */
{ yylhsminor.yy135 = appendSelectClause(yymsp[-3].minor.yy135, yymsp[0].minor.yy488); }
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 171: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy135, NULL, TSDB_SQL_SELECT); }
        break;
      case 172: /* select ::= SELECT selcollist */
{
  yylhsminor.yy488 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy135, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 173: /* sclp ::= selcollist COMMA */
{yylhsminor.yy135 = yymsp[-1].minor.yy135;}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 174: /* sclp ::= */
      case 206: /* orderby_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy135 = 0;}
        break;
      case 175: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy135 = tSqlExprListAppend(yymsp[-3].minor.yy135, yymsp[-1].minor.yy526,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 176: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy135 = tSqlExprListAppend(yymsp[-1].minor.yy135, pNode, 0, 0);
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 177: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 178: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 180: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* from ::= FROM tablelist */
      case 183: /* from ::= FROM sub */ yytestcase(yyruleno==183);
{yymsp[-1].minor.yy460 = yymsp[0].minor.yy460;}
        break;
      case 184: /* sub ::= LP union RP */
{yymsp[-2].minor.yy460 = addSubqueryElem(NULL, yymsp[-1].minor.yy135, NULL);}
        break;
      case 185: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy460 = addSubqueryElem(NULL, yymsp[-2].minor.yy135, &yymsp[0].minor.yy0);}
        break;
      case 186: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy460 = addSubqueryElem(yymsp[-5].minor.yy460, yymsp[-2].minor.yy135, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy460 = yylhsminor.yy460;
        break;
      case 187: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy460 = yylhsminor.yy460;
        break;
      case 188: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy460 = yylhsminor.yy460;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(yymsp[-3].minor.yy460, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy460 = yylhsminor.yy460;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(yymsp[-4].minor.yy460, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy460 = yylhsminor.yy460;
        break;
      case 191: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy160.interval = yymsp[-1].minor.yy0; yylhsminor.yy160.offset.n = 0; yylhsminor.yy160.token = yymsp[-3].minor.yy262;}
  yymsp[-3].minor.yy160 = yylhsminor.yy160;
        break;
      case 193: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy160.interval = yymsp[-3].minor.yy0; yylhsminor.yy160.offset = yymsp[-1].minor.yy0;   yylhsminor.yy160.token = yymsp[-5].minor.yy262;}
  yymsp[-5].minor.yy160 = yylhsminor.yy160;
        break;
      case 194: /* interval_option ::= */
{memset(&yymsp[1].minor.yy160, 0, sizeof(yymsp[1].minor.yy160));}
        break;
      case 195: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy262 = TK_INTERVAL;}
        break;
      case 196: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy262 = TK_EVERY;   }
        break;
      case 197: /* session_option ::= */
{yymsp[1].minor.yy511.col.n = 0; yymsp[1].minor.yy511.gap.n = 0;}
        break;
      case 198: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy511.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy511.gap = yymsp[-1].minor.yy0;
}
        break;
      case 199: /* windowstate_option ::= */
{ yymsp[1].minor.yy258.col.n = 0; yymsp[1].minor.yy258.col.z = NULL;}
        break;
      case 200: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy258.col = yymsp[-1].minor.yy0; }
        break;
      case 201: /* fill_opt ::= */
{ yymsp[1].minor.yy135 = 0;     }
        break;
      case 202: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy135, &A, -1, 0);
    yymsp[-5].minor.yy135 = yymsp[-1].minor.yy135;
}
        break;
      case 203: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy135 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 204: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 205: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 207: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 208: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy135 = tVariantListAppend(yymsp[-3].minor.yy135, &yymsp[-1].minor.yy308, yymsp[0].minor.yy130);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 209: /* sortlist ::= item sortorder */
{
  yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[-1].minor.yy308, yymsp[0].minor.yy130);
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 210: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy308, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy308 = yylhsminor.yy308;
        break;
      case 211: /* sortorder ::= ASC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 212: /* sortorder ::= DESC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_DESC;}
        break;
      case 213: /* sortorder ::= */
{ yymsp[1].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 214: /* groupby_opt ::= */
{ yymsp[1].minor.yy135 = 0;}
        break;
      case 215: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 216: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy135 = tVariantListAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy308, -1);
}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 217: /* grouplist ::= item */
{
  yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[0].minor.yy308, -1);
}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 218: /* having_opt ::= */
      case 228: /* where_opt ::= */ yytestcase(yyruleno==228);
      case 270: /* expritem ::= */ yytestcase(yyruleno==270);
{yymsp[1].minor.yy526 = 0;}
        break;
      case 219: /* having_opt ::= HAVING expr */
      case 229: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==229);
{yymsp[-1].minor.yy526 = yymsp[0].minor.yy526;}
        break;
      case 220: /* limit_opt ::= */
      case 224: /* slimit_opt ::= */ yytestcase(yyruleno==224);
{yymsp[1].minor.yy126.limit = -1; yymsp[1].minor.yy126.offset = 0;}
        break;
      case 221: /* limit_opt ::= LIMIT signed */
      case 225: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==225);
{yymsp[-1].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-1].minor.yy126.offset = 0;}
        break;
      case 222: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy531;}
        break;
      case 223: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy531;}
        break;
      case 226: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy531;}
        break;
      case 227: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy531;}
        break;
      case 230: /* expr ::= LP expr RP */
{yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy526->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 231: /* expr ::= ID */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 232: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 233: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 234: /* expr ::= INTEGER */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 235: /* expr ::= MINUS INTEGER */
      case 236: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 237: /* expr ::= FLOAT */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 238: /* expr ::= MINUS FLOAT */
      case 239: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==239);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 240: /* expr ::= STRING */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 241: /* expr ::= NOW */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 242: /* expr ::= VARIABLE */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 243: /* expr ::= PLUS VARIABLE */
      case 244: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==244);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 245: /* expr ::= BOOL */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 246: /* expr ::= NULL */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 247: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy526 = tSqlExprCreateFunction(yymsp[-1].minor.yy135, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 248: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy526 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 249: /* expr ::= expr IS NULL */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 250: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-3].minor.yy526, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 251: /* expr ::= expr LT expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LT);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 252: /* expr ::= expr GT expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_GT);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 253: /* expr ::= expr LE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 254: /* expr ::= expr GE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_GE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 255: /* expr ::= expr NE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_NE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 256: /* expr ::= expr EQ expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_EQ);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 257: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy526); yylhsminor.yy526 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy526, yymsp[-2].minor.yy526, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy526, TK_LE), TK_AND);}
  yymsp[-4].minor.yy526 = yylhsminor.yy526;
        break;
      case 258: /* expr ::= expr AND expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_AND);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 259: /* expr ::= expr OR expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_OR); }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 260: /* expr ::= expr PLUS expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_PLUS);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 261: /* expr ::= expr MINUS expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_MINUS); }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 262: /* expr ::= expr STAR expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_STAR);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 263: /* expr ::= expr SLASH expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_DIVIDE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 264: /* expr ::= expr REM expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_REM);   }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 265: /* expr ::= expr LIKE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LIKE);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 266: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-4].minor.yy526, (tSqlExpr*)yymsp[-1].minor.yy135, TK_IN); }
  yymsp[-4].minor.yy526 = yylhsminor.yy526;
        break;
      case 267: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy135 = tSqlExprListAppend(yymsp[-2].minor.yy135,yymsp[0].minor.yy526,0, 0);}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 268: /* exprlist ::= expritem */
{yylhsminor.yy135 = tSqlExprListAppend(0,yymsp[0].minor.yy526,0, 0);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 269: /* expritem ::= expr */
{yylhsminor.yy526 = yymsp[0].minor.yy526;}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 271: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 272: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 279: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy308, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 287: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy308, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 290: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 291: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
