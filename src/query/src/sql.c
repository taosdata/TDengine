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
#define YYNOCODE 279
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SRelationInfo* yy8;
  SWindowStateVal yy40;
  SSqlNode* yy56;
  SCreateDbInfo yy90;
  int yy96;
  int32_t yy104;
  SSessionWindowVal yy147;
  SCreatedTableInfo yy152;
  SLimitVal yy166;
  SCreateAcctInfo yy171;
  TAOS_FIELD yy183;
  int64_t yy325;
  SIntervalVal yy400;
  SArray* yy421;
  tVariant yy430;
  SCreateTableSql* yy438;
  tSqlExpr* yy439;
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
#define YYNTOKEN             196
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
 /*   130 */   319,   65,   63,   66,   64,   45,  802,  349, 1036,   56,
 /*   140 */    55,  161, 1022,   54,   53,   52,   58,  243,   61,   62,
 /*   150 */   767,  768,  250,   51,  234,   60,  319,   65,   63,   66,
 /*   160 */    64, 1006, 1007,   35, 1010,   56,   55,  317,  246,   54,
 /*   170 */    53,   52,   44,  315,  356,  355,  314,  313,  312,  354,
 /*   180 */   311,  310,  309,  353,  308,  352,  351,  985,  973,  974,
 /*   190 */   975,  976,  977,  978,  979,  980,  981,  982,  983,  984,
 /*   200 */   986,  987,   61,   62,   24,   23,  250,   51,  264,   60,
 /*   210 */   319,   65,   63,   66,   64,   92,   79,  268,  267,   56,
 /*   220 */    55,  123,  211,   54,   53,   52,  249,  817,  208,  217,
 /*   230 */   806,   93,  809,  349,  812,  137,  136,  135,  216, 1135,
 /*   240 */   249,  817,  324,   86,  806,   81,  809,  259,  812,  251,
 /*   250 */  1008,  205,   65,   63,   66,   64,  161,  175,  228,  229,
 /*   260 */    56,   55,  320,  916,   54,   53,   52,    5,   41,  179,
 /*   270 */   189,  624,  228,  229,  178,  104,  109,  100,  108,  625,
 /*   280 */    45,  732,  357,  954,  729,   38,  730,   38,  731,  121,
 /*   290 */   115,  126,  253,  304,   38,   16,  125,   15,  131,  134,
 /*   300 */   124,  206,  271,   38,   78,  258,  784,  128,   67,  208,
 /*   310 */   748,  224,  255,  256,    3,  190,  199,  197,  195,  808,
 /*   320 */  1135,  811,   67,  194,  141,  140,  139,  138,  293,  240,
 /*   330 */    91,  241,  926, 1020,   44, 1020,  356,  355,  328,  189,
 /*   340 */    38,  354, 1020,  818,  813,  353,   38,  352,  351,  708,
 /*   350 */   814, 1019,  807,   29,  810,  212,   38,  818,  813,   38,
 /*   360 */  1094,   56,   55,  783,  814,   54,   53,   52,  254,   38,
 /*   370 */   252,   38,  327,  326,   38,   14, 1131,  339,  338,   94,
 /*   380 */   917,  260,  745,  257,  329,  334,  333,  189, 1020,  259,
 /*   390 */   330,   54,   53,   52, 1020,  259,  752,  733,  734,  176,
 /*   400 */   331,    1,  177,  335, 1020, 1021,  273, 1020,    9,   97,
 /*   410 */    83,   84,   34,  336,   71,  337,  764, 1020,  341, 1020,
 /*   420 */    39,  774, 1020,  360,  359,  146,  152,  150,  149,  775,
 /*   430 */   156,   74,  718,  804,  321,   68,  296,  720,   77,  815,
 /*   440 */   248,  133,  132,   26,  298,  299,   39,  275,  275,  719,
 /*   450 */    39,   68,  838,  819,  623,  114,   72,  113,   96,   18,
 /*   460 */  1130,   17,    6,   68, 1129,  226,   25,   25,   25,  805,
 /*   470 */   707,   75,  816,  737,  735,  738,  736,   20,  227,   19,
 /*   480 */   120,   22,  119,   21,  209,  210,  213,  207,  214, 1154,
 /*   490 */   215, 1146, 1037, 1093,  219,  238,  220,  221,  218,  204,
 /*   500 */  1090, 1089,  239,  340,  269,  153, 1044, 1055,   48, 1076,
 /*   510 */  1052, 1053,  276, 1075, 1018, 1057,  151,  155,  160,  287,
 /*   520 */   171,  172,  280, 1016,  173,  174,  163,  235,  282,  284,
 /*   530 */   931,  164,  165, 1034,  166,  763,  301,  821,  302,  303,
 /*   540 */   306,  307,   46,  202,  162,  294,   42,  290,  168,  318,
 /*   550 */   925,  325,   76,   73,   50,  292, 1153,  288,  111, 1152,
 /*   560 */   283, 1149,  286,   49,  180,  305,  332, 1145,  167,  122,
 /*   570 */   350,  117, 1144, 1141,  181,  951,   43,   40,   47,  342,
 /*   580 */   203,  913,  127,  911,  129,  130,  909,  908,  261,  192,
 /*   590 */   193,  905,  904,  903,  902,  901,  900,  899,  196,  198,
 /*   600 */   896,  894,  892,  890,  200,  887,  201,  343,  274,   82,
 /*   610 */    87,  344,  285, 1077,  345,  346,  347,  348,  358,  863,
 /*   620 */   263,  262,  862,  266,  225,  245,  300,  265,  861,  844,
 /*   630 */   843,  270,  222,  275,   10,   85,  295,  930,  929,  223,
 /*   640 */   105,  740,  106,  277,   30,   88,  765,  907,  906,  157,
 /*   650 */   158,  142,  143,  898,  184,  183,  952,  187,  182,  185,
 /*   660 */   186,  188,  144,  897,  776,  145,  953,  889,  989,    2,
 /*   670 */   169,  888,  770,   33,    4,  159,   89,  237,  772,   90,
 /*   680 */   289,  999,   31,   11,   32,   12,   13,   27,  297,   28,
 /*   690 */    97,   99,  102,   36,  101,  638,   37,  103,  673,  671,
 /*   700 */   670,  669,  667,  666,  665,  662,  628,  316,  107,    7,
 /*   710 */   822,  820,  322,    8,  323,  710,  110,   69,  112,   70,
 /*   720 */    39,  709,  116,  118,  706,  654,  652,  644,  650,  646,
 /*   730 */   648,  642,  640,  676,  675,  674,  672,  668,  664,  663,
 /*   740 */   191,  590,  626,  588,  867,  866,  866,  866,  866,  866,
 /*   750 */   866,  866,  866,  866,  866,  866,  147,  148,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   253,    1,  245,    1,  199,  200,  249,  200,  245,    9,
 /*    10 */   206,    9,  249,   13,   14,  200,   16,   17,  271,  200,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  223,
 /*    30 */     9,  225,  226,   33,   34,  200,  230,   37,   38,   39,
 /*    40 */   234,  200,  236,  237,  197,  198,  247,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  263,  244,   62,   13,   14,  248,   16,   17,
 /*    70 */   266,    5,   20,   21,  267,   23,   24,   25,   26,   27,
 /*    80 */    28,  277,    0,  268,  269,   33,   34,   87,  207,   37,
 /*    90 */    38,   39,   13,   14,  247,   16,   17,  266,   83,   20,
 /*   100 */    21,  200,   23,   24,   25,   26,   27,   28,  277,  274,
 /*   110 */   263,  276,   33,   34,   85,  274,   37,   38,   39,   13,
 /*   120 */    14,  245,   16,   17,  243,  249,   20,   21,  207,   23,
 /*   130 */    24,   25,   26,   27,   28,  120,   84,   91,  247,   33,
 /*   140 */    34,  200,  249,   37,   38,   39,   14,  246,   16,   17,
 /*   150 */   126,  127,   20,   21,  263,   23,   24,   25,   26,   27,
 /*   160 */    28,  240,  241,  242,  243,   33,   34,   85,  206,   37,
 /*   170 */    38,   39,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   180 */   107,  108,  109,  110,  111,  112,  113,  223,  224,  225,
 /*   190 */   226,  227,  228,  229,  230,  231,  232,  233,  234,  235,
 /*   200 */   236,  237,   16,   17,   44,  266,   20,   21,  143,   23,
 /*   210 */    24,   25,   26,   27,   28,  274,  207,  152,  153,   33,
 /*   220 */    34,   79,   62,   37,   38,   39,    1,    2,  266,   69,
 /*   230 */     5,  250,    7,   91,    9,   75,   76,   77,   78,  277,
 /*   240 */     1,    2,   82,   83,    5,  264,    7,  200,    9,  206,
 /*   250 */   241,  266,   25,   26,   27,   28,  200,  210,   33,   34,
 /*   260 */    33,   34,   37,  205,   37,   38,   39,   63,   64,   65,
 /*   270 */   212,    1,   33,   34,   70,   71,   72,   73,   74,    9,
 /*   280 */   120,    2,  221,  222,    5,  200,    7,  200,    9,   63,
 /*   290 */    64,   65,   69,   89,  200,  146,   70,  148,   72,   73,
 /*   300 */    74,  266,  142,  200,  144,   69,   77,   81,   83,  266,
 /*   310 */    37,  151,   33,   34,  203,  204,   63,   64,   65,    5,
 /*   320 */   277,    7,   83,   70,   71,   72,   73,   74,  272,  244,
 /*   330 */   274,  244,  205,  248,   99,  248,  101,  102,  244,  212,
 /*   340 */   200,  106,  248,  118,  119,  110,  200,  112,  113,    5,
 /*   350 */   125,  248,    5,   83,    7,  266,  200,  118,  119,  200,
 /*   360 */   239,   33,   34,  134,  125,   37,   38,   39,  145,  200,
 /*   370 */   147,  200,  149,  150,  200,   83,  266,   33,   34,   87,
 /*   380 */   205,  145,   98,  147,  244,  149,  150,  212,  248,  200,
 /*   390 */   244,   37,   38,   39,  248,  200,  123,  118,  119,  210,
 /*   400 */   244,  208,  209,  244,  248,  210,   84,  248,  124,  117,
 /*   410 */    84,   84,   83,  244,   98,  244,   84,  248,  244,  248,
 /*   420 */    98,   84,  248,   66,   67,   68,   63,   64,   65,   84,
 /*   430 */    98,   98,   84,    1,   15,   98,   84,   84,   83,  125,
 /*   440 */    61,   79,   80,   98,   84,  116,   98,  121,  121,   84,
 /*   450 */    98,   98,   84,   84,   84,  146,  140,  148,   98,  146,
 /*   460 */   266,  148,   83,   98,  266,  266,   98,   98,   98,   37,
 /*   470 */   115,  138,  125,    5,    5,    7,    7,  146,  266,  148,
 /*   480 */   146,  146,  148,  148,  266,  266,  266,  266,  266,  249,
 /*   490 */   266,  249,  247,  239,  266,  239,  266,  266,  266,  266,
 /*   500 */   239,  239,  239,  239,  200,  200,  200,  200,  265,  275,
 /*   510 */   200,  200,  247,  275,  247,  200,   61,  200,  200,  200,
 /*   520 */   251,  200,  270,  200,  200,  200,  260,  270,  270,  270,
 /*   530 */   200,  259,  258,  262,  257,  125,  200,  118,  200,  200,
 /*   540 */   200,  200,  200,  200,  261,  132,  200,  130,  255,  200,
 /*   550 */   200,  200,  137,  139,  136,  135,  200,  129,  200,  200,
 /*   560 */   131,  200,  128,  141,  200,   90,  200,  200,  256,   97,
 /*   570 */   114,  200,  200,  200,  200,  200,  200,  200,  200,   96,
 /*   580 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   590 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   600 */   200,  200,  200,  200,  200,  200,  200,   51,  201,  201,
 /*   610 */   201,   93,  201,  201,   95,   55,   94,   92,   85,    5,
 /*   620 */     5,  154,    5,    5,  201,  201,  201,  154,    5,  101,
 /*   630 */   100,  143,  201,  121,   83,  122,  116,  211,  211,  201,
 /*   640 */   207,   84,  207,   98,   83,   98,   84,  201,  201,   83,
 /*   650 */    83,  202,  202,  201,  214,  218,  220,  216,  219,  217,
 /*   660 */   215,  213,  202,  201,   84,  202,  222,  201,  238,  208,
 /*   670 */   254,  201,   84,  252,  203,   98,   83,    1,   84,   83,
 /*   680 */    83,  238,   98,  133,   98,  133,   83,   83,  116,   83,
 /*   690 */   117,   79,   71,   88,   87,    5,   88,   87,    9,    5,
 /*   700 */     5,    5,    5,    5,    5,    5,   86,   15,   79,   83,
 /*   710 */   118,   84,   24,   83,   59,    5,  148,   16,  148,   16,
 /*   720 */    98,    5,  148,  148,   84,    5,    5,    5,    5,    5,
 /*   730 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */    98,   61,   86,   60,    0,  278,  278,  278,  278,  278,
 /*   750 */   278,  278,  278,  278,  278,  278,   21,   21,  278,  278,
 /*   760 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   770 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   780 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   790 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   800 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   810 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   820 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   830 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   840 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   850 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   860 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   870 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   880 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   890 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   900 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   910 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   920 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   930 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   940 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   950 */   278,  278,  278,  278,
};
#define YY_SHIFT_COUNT    (363)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (744)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   160,   73,   73,  235,  235,   29,  225,  239,  239,  270,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,   21,    0,    2,  239,  279,  279,  279,   15,
 /*    30 */    15,   21,   21,   24,   21,   82,   21,   21,   21,   21,
 /*    40 */   142,   29,   46,   46,   66,  758,  758,  758,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  239,  239,  239,  239,  239,  239,  279,  279,
 /*    70 */   279,  344,  344,  344,  344,  344,  344,  344,   21,   21,
 /*    80 */    21,  273,   21,   21,   21,   15,   15,   21,   21,   21,
 /*    90 */    21,  229,  229,  284,   15,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   150 */    21,   21,   21,  455,  455,  455,  410,  410,  410,  410,
 /*   160 */   455,  455,  415,  414,  413,  418,  420,  417,  428,  434,
 /*   170 */   429,  422,  455,  455,  455,  475,  475,  456,   29,   29,
 /*   180 */   455,  455,  472,  483,  556,  518,  519,  560,  522,  525,
 /*   190 */   456,   66,  455,  455,  533,  533,  455,  533,  455,  533,
 /*   200 */   455,  455,  758,  758,   52,   79,   79,  106,   79,  132,
 /*   210 */   186,  204,  227,  227,  227,  227,  226,  253,  328,  328,
 /*   220 */   328,  328,  223,  236,   65,  292,  354,  354,  314,  347,
 /*   230 */   357,  363,  322,  326,  327,  332,  337,  345,  316,  333,
 /*   240 */   348,  352,  353,  360,  365,  329,  368,  369,  432,  379,
 /*   250 */   419,  370,  149,  309,  313,  468,  469,  331,  334,  355,
 /*   260 */   335,  362,  614,  467,  615,  617,  473,  618,  623,  528,
 /*   270 */   530,  488,  512,  520,  551,  513,  557,  561,  545,  547,
 /*   280 */   562,  566,  580,  567,  588,  577,  593,  594,  596,  676,
 /*   290 */   597,  584,  550,  586,  552,  603,  520,  604,  572,  606,
 /*   300 */   573,  612,  605,  607,  621,  690,  608,  610,  689,  694,
 /*   310 */   695,  696,  697,  698,  699,  700,  620,  692,  629,  626,
 /*   320 */   627,  592,  630,  688,  655,  701,  568,  570,  622,  622,
 /*   330 */   622,  622,  703,  574,  575,  622,  622,  622,  710,  716,
 /*   340 */   640,  622,  720,  721,  722,  723,  724,  725,  726,  727,
 /*   350 */   728,  729,  730,  731,  732,  733,  734,  642,  656,  735,
 /*   360 */   736,  680,  683,  744,
};
#define YY_REDUCE_COUNT (203)
#define YY_REDUCE_MIN   (-253)
#define YY_REDUCE_MAX   (471)
static const short yy_reduce_ofst[] = {
 /*     0 */  -153,  -36,  -36, -194, -194,  -79, -196,  -38,   43, -185,
 /*    10 */  -181, -165,   56,   85,   87,   94,  140,  146,  156,  159,
 /*    20 */   169,  171,  174, -193, -195, -169, -243, -237, -124, -201,
 /*    30 */  -109, -159,  -59, -253,  -99, -119,   47,  189,  195,  103,
 /*    40 */    58,    9,  127,  175,   61,  -19,  193,  111,  -61,  -15,
 /*    50 */    35,   89,  110,  194,  198,  199,  212,  218,  219,  220,
 /*    60 */   221,  222,  224,  228,  230,  231,  232,  233, -107,  240,
 /*    70 */   242,  121,  254,  256,  261,  262,  263,  264,  304,  305,
 /*    80 */   306,  243,  307,  310,  311,  245,  265,  315,  317,  318,
 /*    90 */   319,  234,  238,  269,  267,  321,  323,  324,  325,  330,
 /*   100 */   336,  338,  339,  340,  341,  342,  343,  346,  349,  350,
 /*   110 */   351,  356,  358,  359,  361,  364,  366,  367,  371,  372,
 /*   120 */   373,  374,  375,  376,  377,  378,  380,  381,  382,  383,
 /*   130 */   384,  385,  386,  387,  388,  389,  390,  391,  392,  393,
 /*   140 */   394,  395,  396,  397,  398,  399,  400,  401,  402,  403,
 /*   150 */   404,  405,  406,  407,  408,  409,  252,  257,  258,  259,
 /*   160 */   411,  412,  271,  283,  266,  272,  274,  277,  312,  293,
 /*   170 */   416,  421,  423,  424,  425,  426,  427,  430,  433,  435,
 /*   180 */   431,  438,  436,  439,  437,  440,  442,  445,  441,  448,
 /*   190 */   443,  444,  446,  447,  449,  450,  452,  460,  462,  463,
 /*   200 */   466,  470,  461,  471,
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
  /*  196 */ "error",
  /*  197 */ "program",
  /*  198 */ "cmd",
  /*  199 */ "dbPrefix",
  /*  200 */ "ids",
  /*  201 */ "cpxName",
  /*  202 */ "ifexists",
  /*  203 */ "alter_db_optr",
  /*  204 */ "alter_topic_optr",
  /*  205 */ "acct_optr",
  /*  206 */ "exprlist",
  /*  207 */ "ifnotexists",
  /*  208 */ "db_optr",
  /*  209 */ "topic_optr",
  /*  210 */ "typename",
  /*  211 */ "bufsize",
  /*  212 */ "pps",
  /*  213 */ "tseries",
  /*  214 */ "dbs",
  /*  215 */ "streams",
  /*  216 */ "storage",
  /*  217 */ "qtime",
  /*  218 */ "users",
  /*  219 */ "conns",
  /*  220 */ "state",
  /*  221 */ "intitemlist",
  /*  222 */ "intitem",
  /*  223 */ "keep",
  /*  224 */ "cache",
  /*  225 */ "replica",
  /*  226 */ "quorum",
  /*  227 */ "days",
  /*  228 */ "minrows",
  /*  229 */ "maxrows",
  /*  230 */ "blocks",
  /*  231 */ "ctime",
  /*  232 */ "wal",
  /*  233 */ "fsync",
  /*  234 */ "comp",
  /*  235 */ "prec",
  /*  236 */ "update",
  /*  237 */ "cachelast",
  /*  238 */ "partitions",
  /*  239 */ "signed",
  /*  240 */ "create_table_args",
  /*  241 */ "create_stable_args",
  /*  242 */ "create_table_list",
  /*  243 */ "create_from_stable",
  /*  244 */ "columnlist",
  /*  245 */ "tagitemlist",
  /*  246 */ "tagNamelist",
  /*  247 */ "select",
  /*  248 */ "column",
  /*  249 */ "tagitem",
  /*  250 */ "selcollist",
  /*  251 */ "from",
  /*  252 */ "where_opt",
  /*  253 */ "interval_option",
  /*  254 */ "sliding_opt",
  /*  255 */ "session_option",
  /*  256 */ "windowstate_option",
  /*  257 */ "fill_opt",
  /*  258 */ "groupby_opt",
  /*  259 */ "having_opt",
  /*  260 */ "orderby_opt",
  /*  261 */ "slimit_opt",
  /*  262 */ "limit_opt",
  /*  263 */ "union",
  /*  264 */ "sclp",
  /*  265 */ "distinct",
  /*  266 */ "expr",
  /*  267 */ "as",
  /*  268 */ "tablelist",
  /*  269 */ "sub",
  /*  270 */ "tmvar",
  /*  271 */ "intervalKey",
  /*  272 */ "sortlist",
  /*  273 */ "sortitem",
  /*  274 */ "item",
  /*  275 */ "sortorder",
  /*  276 */ "grouplist",
  /*  277 */ "expritem",
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
    case 206: /* exprlist */
    case 250: /* selcollist */
    case 264: /* sclp */
{
tSqlExprListDestroy((yypminor->yy421));
}
      break;
    case 221: /* intitemlist */
    case 223: /* keep */
    case 244: /* columnlist */
    case 245: /* tagitemlist */
    case 246: /* tagNamelist */
    case 257: /* fill_opt */
    case 258: /* groupby_opt */
    case 260: /* orderby_opt */
    case 272: /* sortlist */
    case 276: /* grouplist */
{
taosArrayDestroy((yypminor->yy421));
}
      break;
    case 242: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy438));
}
      break;
    case 247: /* select */
{
destroySqlNode((yypminor->yy56));
}
      break;
    case 251: /* from */
    case 268: /* tablelist */
    case 269: /* sub */
{
destroyRelationInfo((yypminor->yy8));
}
      break;
    case 252: /* where_opt */
    case 259: /* having_opt */
    case 266: /* expr */
    case 277: /* expritem */
{
tSqlExprDestroy((yypminor->yy439));
}
      break;
    case 263: /* union */
{
destroyAllSqlNode((yypminor->yy421));
}
      break;
    case 273: /* sortitem */
{
tVariantDestroy(&(yypminor->yy430));
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
  {  197,   -1 }, /* (0) program ::= cmd */
  {  198,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  198,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  198,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  198,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  198,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  198,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  198,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  198,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  198,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  198,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  198,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  198,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  198,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  198,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  198,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  198,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  199,    0 }, /* (17) dbPrefix ::= */
  {  199,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  201,    0 }, /* (19) cpxName ::= */
  {  201,   -2 }, /* (20) cpxName ::= DOT ids */
  {  198,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  198,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  198,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  198,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  198,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  198,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  198,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  198,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  198,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  198,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  198,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  198,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  198,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  198,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  198,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  198,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  198,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  198,   -2 }, /* (38) cmd ::= USE ids */
  {  198,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  198,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  198,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  198,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  198,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  198,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  198,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  198,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  198,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  198,   -4 }, /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  198,   -4 }, /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  198,   -6 }, /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  198,   -6 }, /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  200,   -1 }, /* (52) ids ::= ID */
  {  200,   -1 }, /* (53) ids ::= STRING */
  {  202,   -2 }, /* (54) ifexists ::= IF EXISTS */
  {  202,    0 }, /* (55) ifexists ::= */
  {  207,   -3 }, /* (56) ifnotexists ::= IF NOT EXISTS */
  {  207,    0 }, /* (57) ifnotexists ::= */
  {  198,   -3 }, /* (58) cmd ::= CREATE DNODE ids */
  {  198,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  198,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  198,   -5 }, /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  198,   -8 }, /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  198,   -9 }, /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  198,   -5 }, /* (64) cmd ::= CREATE USER ids PASS ids */
  {  211,    0 }, /* (65) bufsize ::= */
  {  211,   -2 }, /* (66) bufsize ::= BUFSIZE INTEGER */
  {  212,    0 }, /* (67) pps ::= */
  {  212,   -2 }, /* (68) pps ::= PPS INTEGER */
  {  213,    0 }, /* (69) tseries ::= */
  {  213,   -2 }, /* (70) tseries ::= TSERIES INTEGER */
  {  214,    0 }, /* (71) dbs ::= */
  {  214,   -2 }, /* (72) dbs ::= DBS INTEGER */
  {  215,    0 }, /* (73) streams ::= */
  {  215,   -2 }, /* (74) streams ::= STREAMS INTEGER */
  {  216,    0 }, /* (75) storage ::= */
  {  216,   -2 }, /* (76) storage ::= STORAGE INTEGER */
  {  217,    0 }, /* (77) qtime ::= */
  {  217,   -2 }, /* (78) qtime ::= QTIME INTEGER */
  {  218,    0 }, /* (79) users ::= */
  {  218,   -2 }, /* (80) users ::= USERS INTEGER */
  {  219,    0 }, /* (81) conns ::= */
  {  219,   -2 }, /* (82) conns ::= CONNS INTEGER */
  {  220,    0 }, /* (83) state ::= */
  {  220,   -2 }, /* (84) state ::= STATE ids */
  {  205,   -9 }, /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  221,   -3 }, /* (86) intitemlist ::= intitemlist COMMA intitem */
  {  221,   -1 }, /* (87) intitemlist ::= intitem */
  {  222,   -1 }, /* (88) intitem ::= INTEGER */
  {  223,   -2 }, /* (89) keep ::= KEEP intitemlist */
  {  224,   -2 }, /* (90) cache ::= CACHE INTEGER */
  {  225,   -2 }, /* (91) replica ::= REPLICA INTEGER */
  {  226,   -2 }, /* (92) quorum ::= QUORUM INTEGER */
  {  227,   -2 }, /* (93) days ::= DAYS INTEGER */
  {  228,   -2 }, /* (94) minrows ::= MINROWS INTEGER */
  {  229,   -2 }, /* (95) maxrows ::= MAXROWS INTEGER */
  {  230,   -2 }, /* (96) blocks ::= BLOCKS INTEGER */
  {  231,   -2 }, /* (97) ctime ::= CTIME INTEGER */
  {  232,   -2 }, /* (98) wal ::= WAL INTEGER */
  {  233,   -2 }, /* (99) fsync ::= FSYNC INTEGER */
  {  234,   -2 }, /* (100) comp ::= COMP INTEGER */
  {  235,   -2 }, /* (101) prec ::= PRECISION STRING */
  {  236,   -2 }, /* (102) update ::= UPDATE INTEGER */
  {  237,   -2 }, /* (103) cachelast ::= CACHELAST INTEGER */
  {  238,   -2 }, /* (104) partitions ::= PARTITIONS INTEGER */
  {  208,    0 }, /* (105) db_optr ::= */
  {  208,   -2 }, /* (106) db_optr ::= db_optr cache */
  {  208,   -2 }, /* (107) db_optr ::= db_optr replica */
  {  208,   -2 }, /* (108) db_optr ::= db_optr quorum */
  {  208,   -2 }, /* (109) db_optr ::= db_optr days */
  {  208,   -2 }, /* (110) db_optr ::= db_optr minrows */
  {  208,   -2 }, /* (111) db_optr ::= db_optr maxrows */
  {  208,   -2 }, /* (112) db_optr ::= db_optr blocks */
  {  208,   -2 }, /* (113) db_optr ::= db_optr ctime */
  {  208,   -2 }, /* (114) db_optr ::= db_optr wal */
  {  208,   -2 }, /* (115) db_optr ::= db_optr fsync */
  {  208,   -2 }, /* (116) db_optr ::= db_optr comp */
  {  208,   -2 }, /* (117) db_optr ::= db_optr prec */
  {  208,   -2 }, /* (118) db_optr ::= db_optr keep */
  {  208,   -2 }, /* (119) db_optr ::= db_optr update */
  {  208,   -2 }, /* (120) db_optr ::= db_optr cachelast */
  {  209,   -1 }, /* (121) topic_optr ::= db_optr */
  {  209,   -2 }, /* (122) topic_optr ::= topic_optr partitions */
  {  203,    0 }, /* (123) alter_db_optr ::= */
  {  203,   -2 }, /* (124) alter_db_optr ::= alter_db_optr replica */
  {  203,   -2 }, /* (125) alter_db_optr ::= alter_db_optr quorum */
  {  203,   -2 }, /* (126) alter_db_optr ::= alter_db_optr keep */
  {  203,   -2 }, /* (127) alter_db_optr ::= alter_db_optr blocks */
  {  203,   -2 }, /* (128) alter_db_optr ::= alter_db_optr comp */
  {  203,   -2 }, /* (129) alter_db_optr ::= alter_db_optr update */
  {  203,   -2 }, /* (130) alter_db_optr ::= alter_db_optr cachelast */
  {  204,   -1 }, /* (131) alter_topic_optr ::= alter_db_optr */
  {  204,   -2 }, /* (132) alter_topic_optr ::= alter_topic_optr partitions */
  {  210,   -1 }, /* (133) typename ::= ids */
  {  210,   -4 }, /* (134) typename ::= ids LP signed RP */
  {  210,   -2 }, /* (135) typename ::= ids UNSIGNED */
  {  239,   -1 }, /* (136) signed ::= INTEGER */
  {  239,   -2 }, /* (137) signed ::= PLUS INTEGER */
  {  239,   -2 }, /* (138) signed ::= MINUS INTEGER */
  {  198,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_args */
  {  198,   -3 }, /* (140) cmd ::= CREATE TABLE create_stable_args */
  {  198,   -3 }, /* (141) cmd ::= CREATE STABLE create_stable_args */
  {  198,   -3 }, /* (142) cmd ::= CREATE TABLE create_table_list */
  {  242,   -1 }, /* (143) create_table_list ::= create_from_stable */
  {  242,   -2 }, /* (144) create_table_list ::= create_table_list create_from_stable */
  {  240,   -6 }, /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  241,  -10 }, /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  243,  -10 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  243,  -13 }, /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  246,   -3 }, /* (149) tagNamelist ::= tagNamelist COMMA ids */
  {  246,   -1 }, /* (150) tagNamelist ::= ids */
  {  240,   -5 }, /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
  {  244,   -3 }, /* (152) columnlist ::= columnlist COMMA column */
  {  244,   -1 }, /* (153) columnlist ::= column */
  {  248,   -2 }, /* (154) column ::= ids typename */
  {  245,   -3 }, /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
  {  245,   -1 }, /* (156) tagitemlist ::= tagitem */
  {  249,   -1 }, /* (157) tagitem ::= INTEGER */
  {  249,   -1 }, /* (158) tagitem ::= FLOAT */
  {  249,   -1 }, /* (159) tagitem ::= STRING */
  {  249,   -1 }, /* (160) tagitem ::= BOOL */
  {  249,   -1 }, /* (161) tagitem ::= NULL */
  {  249,   -1 }, /* (162) tagitem ::= NOW */
  {  249,   -2 }, /* (163) tagitem ::= MINUS INTEGER */
  {  249,   -2 }, /* (164) tagitem ::= MINUS FLOAT */
  {  249,   -2 }, /* (165) tagitem ::= PLUS INTEGER */
  {  249,   -2 }, /* (166) tagitem ::= PLUS FLOAT */
  {  247,  -14 }, /* (167) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  247,   -3 }, /* (168) select ::= LP select RP */
  {  263,   -1 }, /* (169) union ::= select */
  {  263,   -4 }, /* (170) union ::= union UNION ALL select */
  {  198,   -1 }, /* (171) cmd ::= union */
  {  247,   -2 }, /* (172) select ::= SELECT selcollist */
  {  264,   -2 }, /* (173) sclp ::= selcollist COMMA */
  {  264,    0 }, /* (174) sclp ::= */
  {  250,   -4 }, /* (175) selcollist ::= sclp distinct expr as */
  {  250,   -2 }, /* (176) selcollist ::= sclp STAR */
  {  267,   -2 }, /* (177) as ::= AS ids */
  {  267,   -1 }, /* (178) as ::= ids */
  {  267,    0 }, /* (179) as ::= */
  {  265,   -1 }, /* (180) distinct ::= DISTINCT */
  {  265,    0 }, /* (181) distinct ::= */
  {  251,   -2 }, /* (182) from ::= FROM tablelist */
  {  251,   -2 }, /* (183) from ::= FROM sub */
  {  269,   -3 }, /* (184) sub ::= LP union RP */
  {  269,   -4 }, /* (185) sub ::= LP union RP ids */
  {  269,   -6 }, /* (186) sub ::= sub COMMA LP union RP ids */
  {  268,   -2 }, /* (187) tablelist ::= ids cpxName */
  {  268,   -3 }, /* (188) tablelist ::= ids cpxName ids */
  {  268,   -4 }, /* (189) tablelist ::= tablelist COMMA ids cpxName */
  {  268,   -5 }, /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
  {  270,   -1 }, /* (191) tmvar ::= VARIABLE */
  {  253,   -4 }, /* (192) interval_option ::= intervalKey LP tmvar RP */
  {  253,   -6 }, /* (193) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  253,    0 }, /* (194) interval_option ::= */
  {  271,   -1 }, /* (195) intervalKey ::= INTERVAL */
  {  271,   -1 }, /* (196) intervalKey ::= EVERY */
  {  255,    0 }, /* (197) session_option ::= */
  {  255,   -7 }, /* (198) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  256,    0 }, /* (199) windowstate_option ::= */
  {  256,   -4 }, /* (200) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  257,    0 }, /* (201) fill_opt ::= */
  {  257,   -6 }, /* (202) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  257,   -4 }, /* (203) fill_opt ::= FILL LP ID RP */
  {  254,   -4 }, /* (204) sliding_opt ::= SLIDING LP tmvar RP */
  {  254,    0 }, /* (205) sliding_opt ::= */
  {  260,    0 }, /* (206) orderby_opt ::= */
  {  260,   -3 }, /* (207) orderby_opt ::= ORDER BY sortlist */
  {  272,   -4 }, /* (208) sortlist ::= sortlist COMMA item sortorder */
  {  272,   -2 }, /* (209) sortlist ::= item sortorder */
  {  274,   -2 }, /* (210) item ::= ids cpxName */
  {  275,   -1 }, /* (211) sortorder ::= ASC */
  {  275,   -1 }, /* (212) sortorder ::= DESC */
  {  275,    0 }, /* (213) sortorder ::= */
  {  258,    0 }, /* (214) groupby_opt ::= */
  {  258,   -3 }, /* (215) groupby_opt ::= GROUP BY grouplist */
  {  276,   -3 }, /* (216) grouplist ::= grouplist COMMA item */
  {  276,   -1 }, /* (217) grouplist ::= item */
  {  259,    0 }, /* (218) having_opt ::= */
  {  259,   -2 }, /* (219) having_opt ::= HAVING expr */
  {  262,    0 }, /* (220) limit_opt ::= */
  {  262,   -2 }, /* (221) limit_opt ::= LIMIT signed */
  {  262,   -4 }, /* (222) limit_opt ::= LIMIT signed OFFSET signed */
  {  262,   -4 }, /* (223) limit_opt ::= LIMIT signed COMMA signed */
  {  261,    0 }, /* (224) slimit_opt ::= */
  {  261,   -2 }, /* (225) slimit_opt ::= SLIMIT signed */
  {  261,   -4 }, /* (226) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  261,   -4 }, /* (227) slimit_opt ::= SLIMIT signed COMMA signed */
  {  252,    0 }, /* (228) where_opt ::= */
  {  252,   -2 }, /* (229) where_opt ::= WHERE expr */
  {  266,   -3 }, /* (230) expr ::= LP expr RP */
  {  266,   -1 }, /* (231) expr ::= ID */
  {  266,   -3 }, /* (232) expr ::= ID DOT ID */
  {  266,   -3 }, /* (233) expr ::= ID DOT STAR */
  {  266,   -1 }, /* (234) expr ::= INTEGER */
  {  266,   -2 }, /* (235) expr ::= MINUS INTEGER */
  {  266,   -2 }, /* (236) expr ::= PLUS INTEGER */
  {  266,   -1 }, /* (237) expr ::= FLOAT */
  {  266,   -2 }, /* (238) expr ::= MINUS FLOAT */
  {  266,   -2 }, /* (239) expr ::= PLUS FLOAT */
  {  266,   -1 }, /* (240) expr ::= STRING */
  {  266,   -1 }, /* (241) expr ::= NOW */
  {  266,   -1 }, /* (242) expr ::= VARIABLE */
  {  266,   -2 }, /* (243) expr ::= PLUS VARIABLE */
  {  266,   -2 }, /* (244) expr ::= MINUS VARIABLE */
  {  266,   -1 }, /* (245) expr ::= BOOL */
  {  266,   -1 }, /* (246) expr ::= NULL */
  {  266,   -4 }, /* (247) expr ::= ID LP exprlist RP */
  {  266,   -4 }, /* (248) expr ::= ID LP STAR RP */
  {  266,   -3 }, /* (249) expr ::= expr IS NULL */
  {  266,   -4 }, /* (250) expr ::= expr IS NOT NULL */
  {  266,   -3 }, /* (251) expr ::= expr LT expr */
  {  266,   -3 }, /* (252) expr ::= expr GT expr */
  {  266,   -3 }, /* (253) expr ::= expr LE expr */
  {  266,   -3 }, /* (254) expr ::= expr GE expr */
  {  266,   -3 }, /* (255) expr ::= expr NE expr */
  {  266,   -3 }, /* (256) expr ::= expr EQ expr */
  {  266,   -5 }, /* (257) expr ::= expr BETWEEN expr AND expr */
  {  266,   -3 }, /* (258) expr ::= expr AND expr */
  {  266,   -3 }, /* (259) expr ::= expr OR expr */
  {  266,   -3 }, /* (260) expr ::= expr PLUS expr */
  {  266,   -3 }, /* (261) expr ::= expr MINUS expr */
  {  266,   -3 }, /* (262) expr ::= expr STAR expr */
  {  266,   -3 }, /* (263) expr ::= expr SLASH expr */
  {  266,   -3 }, /* (264) expr ::= expr REM expr */
  {  266,   -3 }, /* (265) expr ::= expr LIKE expr */
  {  266,   -5 }, /* (266) expr ::= expr IN LP exprlist RP */
  {  206,   -3 }, /* (267) exprlist ::= exprlist COMMA expritem */
  {  206,   -1 }, /* (268) exprlist ::= expritem */
  {  277,   -1 }, /* (269) expritem ::= expr */
  {  277,    0 }, /* (270) expritem ::= */
  {  198,   -3 }, /* (271) cmd ::= RESET QUERY CACHE */
  {  198,   -3 }, /* (272) cmd ::= SYNCDB ids REPLICA */
  {  198,   -7 }, /* (273) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  198,   -7 }, /* (274) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  198,   -7 }, /* (275) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  198,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  198,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  198,   -8 }, /* (278) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  198,   -9 }, /* (279) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  198,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  198,   -7 }, /* (281) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  198,   -7 }, /* (282) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  198,   -7 }, /* (283) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  198,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  198,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  198,   -8 }, /* (286) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  198,   -9 }, /* (287) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  198,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  198,   -3 }, /* (289) cmd ::= KILL CONNECTION INTEGER */
  {  198,   -5 }, /* (290) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  198,   -5 }, /* (291) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy171);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy421);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &yymsp[-2].minor.yy0);}
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy183, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy183, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy171.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy171.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy171.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy171.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy171.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy171.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy171.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy171 = yylhsminor.yy171;
        break;
      case 86: /* intitemlist ::= intitemlist COMMA intitem */
      case 155: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);    }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
{ yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 89: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy90); yymsp[1].minor.yy90.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 106: /* db_optr ::= db_optr cache */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 107: /* db_optr ::= db_optr replica */
      case 124: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==124);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 108: /* db_optr ::= db_optr quorum */
      case 125: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==125);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 109: /* db_optr ::= db_optr days */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 110: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 111: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 112: /* db_optr ::= db_optr blocks */
      case 127: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==127);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 113: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 114: /* db_optr ::= db_optr wal */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 115: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 116: /* db_optr ::= db_optr comp */
      case 128: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==128);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 117: /* db_optr ::= db_optr prec */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 118: /* db_optr ::= db_optr keep */
      case 126: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==126);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.keep = yymsp[0].minor.yy421; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 119: /* db_optr ::= db_optr update */
      case 129: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==129);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 120: /* db_optr ::= db_optr cachelast */
      case 130: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==130);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 121: /* topic_optr ::= db_optr */
      case 131: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==131);
{ yylhsminor.yy90 = yymsp[0].minor.yy90; yylhsminor.yy90.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 122: /* topic_optr ::= topic_optr partitions */
      case 132: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==132);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 123: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy90); yymsp[1].minor.yy90.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 133: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy183, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy183 = yylhsminor.yy183;
        break;
      case 134: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy325 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy325;  // negative value of name length
    tSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy183 = yylhsminor.yy183;
        break;
      case 135: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy183, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 136: /* signed ::= INTEGER */
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 137: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 138: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 142: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
        break;
      case 143: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy438 = pCreateTable;
}
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 144: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 146: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy152 = yylhsminor.yy152;
        break;
      case 149: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 150: /* tagNamelist ::= ids */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 151: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy56, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 152: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy183); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 153: /* columnlist ::= column */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy183);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 154: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy183, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 161: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 162: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 163: /* tagitem ::= MINUS INTEGER */
      case 164: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==166);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy421, yymsp[-11].minor.yy8, yymsp[-10].minor.yy439, yymsp[-4].minor.yy421, yymsp[-2].minor.yy421, &yymsp[-9].minor.yy400, &yymsp[-7].minor.yy147, &yymsp[-6].minor.yy40, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, &yymsp[0].minor.yy166, &yymsp[-1].minor.yy166, yymsp[-3].minor.yy439);
}
  yymsp[-13].minor.yy56 = yylhsminor.yy56;
        break;
      case 168: /* select ::= LP select RP */
{yymsp[-2].minor.yy56 = yymsp[-1].minor.yy56;}
        break;
      case 169: /* union ::= select */
{ yylhsminor.yy421 = setSubclause(NULL, yymsp[0].minor.yy56); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 170: /* union ::= union UNION ALL select */
{ yylhsminor.yy421 = appendSelectClause(yymsp[-3].minor.yy421, yymsp[0].minor.yy56); }
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 171: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy421, NULL, TSDB_SQL_SELECT); }
        break;
      case 172: /* select ::= SELECT selcollist */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy421, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 173: /* sclp ::= selcollist COMMA */
{yylhsminor.yy421 = yymsp[-1].minor.yy421;}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 174: /* sclp ::= */
      case 206: /* orderby_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy421 = 0;}
        break;
      case 175: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-3].minor.yy421, yymsp[-1].minor.yy439,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 176: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-1].minor.yy421, pNode, 0, 0);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
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
{yymsp[-1].minor.yy8 = yymsp[0].minor.yy8;}
        break;
      case 184: /* sub ::= LP union RP */
{yymsp[-2].minor.yy8 = addSubqueryElem(NULL, yymsp[-1].minor.yy421, NULL);}
        break;
      case 185: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy8 = addSubqueryElem(NULL, yymsp[-2].minor.yy421, &yymsp[0].minor.yy0);}
        break;
      case 186: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy8 = addSubqueryElem(yymsp[-5].minor.yy8, yymsp[-2].minor.yy421, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy8 = yylhsminor.yy8;
        break;
      case 187: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy8 = yylhsminor.yy8;
        break;
      case 188: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-3].minor.yy8, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy8 = yylhsminor.yy8;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-4].minor.yy8, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy8 = yylhsminor.yy8;
        break;
      case 191: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy400.interval = yymsp[-1].minor.yy0; yylhsminor.yy400.offset.n = 0; yylhsminor.yy400.token = yymsp[-3].minor.yy104;}
  yymsp[-3].minor.yy400 = yylhsminor.yy400;
        break;
      case 193: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy400.interval = yymsp[-3].minor.yy0; yylhsminor.yy400.offset = yymsp[-1].minor.yy0;   yylhsminor.yy400.token = yymsp[-5].minor.yy104;}
  yymsp[-5].minor.yy400 = yylhsminor.yy400;
        break;
      case 194: /* interval_option ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 195: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy104 = TK_INTERVAL;}
        break;
      case 196: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy104 = TK_EVERY;   }
        break;
      case 197: /* session_option ::= */
{yymsp[1].minor.yy147.col.n = 0; yymsp[1].minor.yy147.gap.n = 0;}
        break;
      case 198: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy147.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy147.gap = yymsp[-1].minor.yy0;
}
        break;
      case 199: /* windowstate_option ::= */
{ yymsp[1].minor.yy40.col.n = 0; yymsp[1].minor.yy40.col.z = NULL;}
        break;
      case 200: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy40.col = yymsp[-1].minor.yy0; }
        break;
      case 201: /* fill_opt ::= */
{ yymsp[1].minor.yy421 = 0;     }
        break;
      case 202: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
        break;
      case 203: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 204: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 205: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 207: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 208: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy421 = tVariantListAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 209: /* sortlist ::= item sortorder */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 210: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 211: /* sortorder ::= ASC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 212: /* sortorder ::= DESC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
        break;
      case 213: /* sortorder ::= */
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 214: /* groupby_opt ::= */
{ yymsp[1].minor.yy421 = 0;}
        break;
      case 215: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 216: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 217: /* grouplist ::= item */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1);
}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 218: /* having_opt ::= */
      case 228: /* where_opt ::= */ yytestcase(yyruleno==228);
      case 270: /* expritem ::= */ yytestcase(yyruleno==270);
{yymsp[1].minor.yy439 = 0;}
        break;
      case 219: /* having_opt ::= HAVING expr */
      case 229: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==229);
{yymsp[-1].minor.yy439 = yymsp[0].minor.yy439;}
        break;
      case 220: /* limit_opt ::= */
      case 224: /* slimit_opt ::= */ yytestcase(yyruleno==224);
{yymsp[1].minor.yy166.limit = -1; yymsp[1].minor.yy166.offset = 0;}
        break;
      case 221: /* limit_opt ::= LIMIT signed */
      case 225: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==225);
{yymsp[-1].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy166.offset = 0;}
        break;
      case 222: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy166.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[0].minor.yy325;}
        break;
      case 223: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[-2].minor.yy325;}
        break;
      case 226: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy166.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[0].minor.yy325;}
        break;
      case 227: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[-2].minor.yy325;}
        break;
      case 230: /* expr ::= LP expr RP */
{yylhsminor.yy439 = yymsp[-1].minor.yy439; yylhsminor.yy439->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy439->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 231: /* expr ::= ID */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 232: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 233: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 234: /* expr ::= INTEGER */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 235: /* expr ::= MINUS INTEGER */
      case 236: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 237: /* expr ::= FLOAT */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 238: /* expr ::= MINUS FLOAT */
      case 239: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==239);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 240: /* expr ::= STRING */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 241: /* expr ::= NOW */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 242: /* expr ::= VARIABLE */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 243: /* expr ::= PLUS VARIABLE */
      case 244: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==244);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 245: /* expr ::= BOOL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 246: /* expr ::= NULL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 247: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(yymsp[-1].minor.yy421, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 248: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 249: /* expr ::= expr IS NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 250: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-3].minor.yy439, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 251: /* expr ::= expr LT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 252: /* expr ::= expr GT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 253: /* expr ::= expr LE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 254: /* expr ::= expr GE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 255: /* expr ::= expr NE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 256: /* expr ::= expr EQ expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_EQ);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 257: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy439); yylhsminor.yy439 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy439, yymsp[-2].minor.yy439, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy439, TK_LE), TK_AND);}
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 258: /* expr ::= expr AND expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_AND);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 259: /* expr ::= expr OR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_OR); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 260: /* expr ::= expr PLUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_PLUS);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 261: /* expr ::= expr MINUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MINUS); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 262: /* expr ::= expr STAR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_STAR);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 263: /* expr ::= expr SLASH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_DIVIDE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 264: /* expr ::= expr REM expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_REM);   }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 265: /* expr ::= expr LIKE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LIKE);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 266: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-4].minor.yy439, (tSqlExpr*)yymsp[-1].minor.yy421, TK_IN); }
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 267: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy421 = tSqlExprListAppend(yymsp[-2].minor.yy421,yymsp[0].minor.yy439,0, 0);}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 268: /* exprlist ::= expritem */
{yylhsminor.yy421 = tSqlExprListAppend(0,yymsp[0].minor.yy439,0, 0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 269: /* expritem ::= expr */
{yylhsminor.yy439 = yymsp[0].minor.yy439;}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
