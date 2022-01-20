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

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "qSqlparser.h"
#include "tmsgtype.h"
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
#define YYNOCODE 278
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
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             368
#define YYNRULE              294
#define YYNRULE_WITH_ACTION  294
#define YYNTOKEN             197
#define YY_MAX_SHIFT         367
#define YY_MIN_SHIFTREDUCE   576
#define YY_MAX_SHIFTREDUCE   869
#define YY_ERROR_ACTION      870
#define YY_ACCEPT_ACTION     871
#define YY_NO_ACTION         872
#define YY_MIN_REDUCE        873
#define YY_MAX_REDUCE        1166
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
#define YY_ACTTAB_COUNT (773)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    23,  628,  366,  235, 1051,  208,  241,  712,  211,  629,
 /*    10 */  1029,  871,  367,   59,   60,  173,   63,   64, 1042, 1142,
 /*    20 */   255,   53,   52,   51,  628,   62,  324,   67,   65,   68,
 /*    30 */    66,  157,  629,  286,  238,   58,   57,  344,  343,   56,
 /*    40 */    55,   54,   59,   60,  247,   63,   64,  252, 1029,  255,
 /*    50 */    53,   52,   51,  209,   62,  324,   67,   65,   68,   66,
 /*    60 */   999, 1042,  997,  998,   58,   57,  664, 1000,   56,   55,
 /*    70 */    54, 1001, 1048, 1002, 1003,   58,   57,  277, 1015,   56,
 /*    80 */    55,   54,   59,   60,  164,   63,   64,   38,   82,  255,
 /*    90 */    53,   52,   51,   88,   62,  324,   67,   65,   68,   66,
 /*   100 */   284,  283,  249,  752,   58,   57, 1029,  211,   56,   55,
 /*   110 */    54,   38,   59,   61,  806,   63,   64, 1042, 1143,  255,
 /*   120 */    53,   52,   51,  628,   62,  324,   67,   65,   68,   66,
 /*   130 */    45,  629,  237,  239,   58,   57, 1026,  164,   56,   55,
 /*   140 */    54,   60, 1023,   63,   64,  771,  772,  255,   53,   52,
 /*   150 */    51,   95,   62,  324,   67,   65,   68,   66,   38, 1090,
 /*   160 */  1025,  296,   58,   57,  322,   83,   56,   55,   54,  577,
 /*   170 */   578,  579,  580,  581,  582,  583,  584,  585,  586,  587,
 /*   180 */   588,  589,  590,  155,  322,  236,   63,   64,  756,  248,
 /*   190 */   255,   53,   52,   51,  628,   62,  324,   67,   65,   68,
 /*   200 */    66,  251,  629,  245,  354,   58,   57, 1026,  215,   56,
 /*   210 */    55,   54, 1089,   44,  320,  361,  360,  319,  318,  317,
 /*   220 */   359,  316,  315,  314,  358,  313,  357,  356,  808,   38,
 /*   230 */     1,  180,   24,  991,  979,  980,  981,  982,  983,  984,
 /*   240 */   985,  986,  987,  988,  989,  990,  992,  993,  256,  214,
 /*   250 */    38,  254,  821,  922,  100,  810,  222,  813,  164,  816,
 /*   260 */   192,  211,  139,  138,  137,  221,  809,  254,  821,  329,
 /*   270 */    88,  810, 1143,  813,  246,  816, 1028,   29, 1026,   67,
 /*   280 */    65,   68,   66,   38, 1162,  233,  234,   58,   57,  325,
 /*   290 */  1017,   56,   55,   54,   38,  333,   56,   55,   54, 1026,
 /*   300 */   269,  233,  234,  258,    5,   41,  182,   45,  211,  273,
 /*   310 */   272,  181,  106,  111,  102,  110,  164,   73,  736, 1143,
 /*   320 */   932,  733,  812,  734,  815,  735,  263,  192,  334,  276,
 /*   330 */   309,   80, 1026,   94,   69,  123,  117,  128,  229,  335,
 /*   340 */   362,  960,  127, 1026,  133,  136,  126,  202,  200,  198,
 /*   350 */    69,  260,  261,  130,  197,  143,  142,  141,  140,   74,
 /*   360 */    44,   97,  361,  360,  788,  923,   38,  359,   38,  822,
 /*   370 */   817,  358,  192,  357,  356,   38,  818,   38,   38,  259,
 /*   380 */   811,  257,  814,  332,  331,  822,  817,  264,  125,  298,
 /*   390 */   264,   93,  818,  326, 1012, 1013,   35, 1016,  178,   14,
 /*   400 */   354,  179,  265,   96,  262,  264,  339,  338,  154,  152,
 /*   410 */   151,  336,  749,  340,   81, 1026, 1027, 1026,    3,  193,
 /*   420 */   341,  787,  342,  346, 1026,  278, 1026, 1026,  365,  364,
 /*   430 */   148,   85,   86,   99,   76,  737,  738,  768,    9,   39,
 /*   440 */   778,  779,  722,  819,  301,  724,  216,  303, 1014,  723,
 /*   450 */    34,  159,  844,  823,   70,   26,   39,  253,   39,   70,
 /*   460 */    79,   98,  627,   70,  135,  134,   25,   25,  280,  280,
 /*   470 */    16,  116,   15,  115,   77,   18,   25,   17,  741,    6,
 /*   480 */   742,  274,  739,  304,  740,   20,  122,   19,  121,   22,
 /*   490 */   217,   21,  711, 1100, 1137, 1136, 1135,  825,  231,  156,
 /*   500 */   232,  820,  212,  213,  218,  210, 1099,  219,  220,  224,
 /*   510 */   225,  226,  223,  207, 1154,  243, 1096, 1095,  244,  345,
 /*   520 */  1050, 1061, 1043,   48, 1058, 1059, 1063,  153,  281,  158,
 /*   530 */   163,  292, 1024,  175, 1082,  174, 1081,  279,   84,  285,
 /*   540 */  1022,  310,  176,  240,  177,  171,  167,  937,  306,  307,
 /*   550 */   308,  767,  311,  312, 1040,  165,  166,   46,  287,  289,
 /*   560 */   297,  299,  205,  168,   42,   78,   75,   50,  323,  931,
 /*   570 */   330, 1161,  113, 1160,  295,  169,  293,  291, 1157,  183,
 /*   580 */   337, 1153,  119,  288, 1152, 1149,  184,  957,   43,   40,
 /*   590 */    47,  206,  919,  129,   49,  917,  131,  132,  915,  914,
 /*   600 */   266,  195,  196,  911,  910,  909,  908,  907,  906,  905,
 /*   610 */   199,  201,  902,  900,  898,  896,  203,  893,  204,  889,
 /*   620 */   355,  124,   89,  290, 1083,  347,  348,  349,  350,  351,
 /*   630 */   352,  353,  363,  869,  230,  250,  305,  267,  268,  868,
 /*   640 */   270,  227,  228,  271,  867,  850,  107,  936,  935,  108,
 /*   650 */   849,  275,  280,  300,   10,  282,  744,   87,   30,   90,
 /*   660 */   913,  912,  904,  186,  958,  190,  185,  187,  144,  191,
 /*   670 */   189,  188,  145,  146,  147,  903,  995,  895,    4,  894,
 /*   680 */   959,  769,  160,   33,  780,  170,  172,    2,  161,  162,
 /*   690 */   774,   91,  242,  776,   92, 1005,  294,   11,   12,   31,
 /*   700 */    32,   13,   27,  302,   28,   99,  101,  104,   36,  103,
 /*   710 */   642,   37,  105,  677,  675,  674,  673,  671,  670,  669,
 /*   720 */   666,  321,  109,  632,    7,  826,  824,    8,  327,  328,
 /*   730 */   112,  114,   71,   72,  118,  714,   39,  120,  713,  710,
 /*   740 */   658,  656,  648,  654,  650,  652,  646,  644,  680,  679,
 /*   750 */   678,  676,  672,  668,  667,  194,  630,  594,  873,  872,
 /*   760 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   770 */   872,  149,  150,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   266,    1,  199,  200,  199,  266,  245,    5,  266,    9,
 /*    10 */   249,  197,  198,   13,   14,  253,   16,   17,  247,  277,
 /*    20 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*    30 */    30,  199,    9,  271,  263,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  245,   16,   17,  206,  249,   20,
 /*    50 */    21,   22,   23,  266,   25,   26,   27,   28,   29,   30,
 /*    60 */   223,  247,  225,  226,   35,   36,    5,  230,   39,   40,
 /*    70 */    41,  234,  267,  236,  237,   35,   36,  263,    0,   39,
 /*    80 */    40,   41,   13,   14,  199,   16,   17,  199,   88,   20,
 /*    90 */    21,   22,   23,   84,   25,   26,   27,   28,   29,   30,
 /*   100 */   268,  269,  245,   39,   35,   36,  249,  266,   39,   40,
 /*   110 */    41,  199,   13,   14,   85,   16,   17,  247,  277,   20,
 /*   120 */    21,   22,   23,    1,   25,   26,   27,   28,   29,   30,
 /*   130 */   121,    9,  244,  263,   35,   36,  248,  199,   39,   40,
 /*   140 */    41,   14,  199,   16,   17,  127,  128,   20,   21,   22,
 /*   150 */    23,  250,   25,   26,   27,   28,   29,   30,  199,  274,
 /*   160 */   248,  276,   35,   36,   86,  264,   39,   40,   41,   47,
 /*   170 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   180 */    58,   59,   60,   61,   86,   63,   16,   17,  124,  246,
 /*   190 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*   200 */    30,  206,    9,  244,   92,   35,   36,  248,  266,   39,
 /*   210 */    40,   41,  274,  100,  101,  102,  103,  104,  105,  106,
 /*   220 */   107,  108,  109,  110,  111,  112,  113,  114,    1,  199,
 /*   230 */   208,  209,   46,  223,  224,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,  237,  206,   63,
 /*   250 */   199,    1,    2,  205,  207,    5,   70,    7,  199,    9,
 /*   260 */   212,  266,   76,   77,   78,   79,   39,    1,    2,   83,
 /*   270 */    84,    5,  277,    7,  244,    9,  249,   84,  248,   27,
 /*   280 */    28,   29,   30,  199,  249,   35,   36,   35,   36,   39,
 /*   290 */   243,   39,   40,   41,  199,  244,   39,   40,   41,  248,
 /*   300 */   144,   35,   36,   70,   64,   65,   66,  121,  266,  153,
 /*   310 */   154,   71,   72,   73,   74,   75,  199,   99,    2,  277,
 /*   320 */   205,    5,    5,    7,    7,    9,   70,  212,  244,  143,
 /*   330 */    90,  145,  248,  274,   84,   64,   65,   66,  152,  244,
 /*   340 */   221,  222,   71,  248,   73,   74,   75,   64,   65,   66,
 /*   350 */    84,   35,   36,   82,   71,   72,   73,   74,   75,  141,
 /*   360 */   100,  207,  102,  103,   78,  205,  199,  107,  199,  119,
 /*   370 */   120,  111,  212,  113,  114,  199,  126,  199,  199,  146,
 /*   380 */     5,  148,    7,  150,  151,  119,  120,  199,   80,  272,
 /*   390 */   199,  274,  126,   15,  240,  241,  242,  243,  210,   84,
 /*   400 */    92,  210,  146,   88,  148,  199,  150,  151,   64,   65,
 /*   410 */    66,  244,   99,  244,  207,  248,  210,  248,  203,  204,
 /*   420 */   244,  135,  244,  244,  248,   85,  248,  248,   67,   68,
 /*   430 */    69,   85,   85,  118,   99,  119,  120,   85,  125,   99,
 /*   440 */    85,   85,   85,  126,   85,   85,  266,   85,  241,   85,
 /*   450 */    84,   99,   85,   85,   99,   99,   99,   62,   99,   99,
 /*   460 */    84,   99,   85,   99,   80,   81,   99,   99,  122,  122,
 /*   470 */   147,  147,  149,  149,  139,  147,   99,  149,    5,   84,
 /*   480 */     7,  199,    5,  117,    7,  147,  147,  149,  149,  147,
 /*   490 */   266,  149,  116,  239,  266,  266,  266,  119,  266,  199,
 /*   500 */   266,  126,  266,  266,  266,  266,  239,  266,  266,  266,
 /*   510 */   266,  266,  266,  266,  249,  239,  239,  239,  239,  239,
 /*   520 */   199,  199,  247,  265,  199,  199,  199,   62,  247,  199,
 /*   530 */   199,  199,  247,  199,  275,  251,  275,  201,  201,  270,
 /*   540 */   199,   91,  199,  270,  199,  255,  259,  199,  199,  199,
 /*   550 */   199,  126,  199,  199,  262,  261,  260,  199,  270,  270,
 /*   560 */   136,  133,  199,  258,  199,  138,  140,  137,  199,  199,
 /*   570 */   199,  199,  199,  199,  131,  257,  130,  129,  199,  199,
 /*   580 */   199,  199,  199,  132,  199,  199,  199,  199,  199,  199,
 /*   590 */   199,  199,  199,  199,  142,  199,  199,  199,  199,  199,
 /*   600 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   610 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   620 */   115,   98,  201,  201,  201,   97,   53,   94,   96,   57,
 /*   630 */    95,   93,   86,    5,  201,  201,  201,  155,    5,    5,
 /*   640 */   155,  201,  201,    5,    5,  102,  207,  211,  211,  207,
 /*   650 */   101,  144,  122,  117,   84,   99,   85,  123,   84,   99,
 /*   660 */   201,  201,  201,  218,  220,  216,  219,  214,  202,  213,
 /*   670 */   215,  217,  202,  202,  202,  201,  238,  201,  203,  201,
 /*   680 */   222,   85,   84,  252,   85,  256,  254,  208,   84,   99,
 /*   690 */    85,   84,    1,   85,   84,  238,   84,  134,  134,   99,
 /*   700 */    99,   84,   84,  117,   84,  118,   80,   72,   89,   88,
 /*   710 */     5,   89,   88,    9,    5,    5,    5,    5,    5,    5,
 /*   720 */     5,   15,   80,   87,   84,  119,   85,   84,   26,   61,
 /*   730 */   149,  149,   16,   16,  149,    5,   99,  149,    5,   85,
 /*   740 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   750 */     5,    5,    5,    5,    5,   99,   87,   62,    0,  278,
 /*   760 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   770 */   278,   21,   21,  278,  278,  278,  278,  278,  278,  278,
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
 /*   950 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   960 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
};
#define YY_SHIFT_COUNT    (367)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (758)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   186,  113,  113,  260,  260,   98,  250,  266,  266,  193,
 /*    10 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*    20 */    23,   23,   23,    0,  122,  266,  316,  316,  316,    9,
 /*    30 */     9,   23,   23,   18,   23,   78,   23,   23,   23,   23,
 /*    40 */   308,   98,  112,  112,   61,  773,  773,  773,  266,  266,
 /*    50 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*    60 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*    70 */   316,  316,  316,    2,    2,    2,    2,    2,    2,    2,
 /*    80 */    23,   23,   23,   64,   23,   23,   23,    9,    9,   23,
 /*    90 */    23,   23,   23,  286,  286,  313,    9,   23,   23,   23,
 /*   100 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   110 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   120 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   130 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   140 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   150 */    23,   23,   23,   23,   23,   23,  465,  465,  465,  425,
 /*   160 */   425,  425,  425,  465,  465,  427,  426,  428,  430,  424,
 /*   170 */   443,  446,  448,  451,  452,  465,  465,  465,  450,  450,
 /*   180 */   505,   98,   98,  465,  465,  523,  528,  573,  533,  532,
 /*   190 */   572,  535,  538,  505,   61,  465,  465,  546,  546,  465,
 /*   200 */   546,  465,  546,  465,  465,  773,  773,   29,   69,   69,
 /*   210 */    99,   69,  127,  170,  240,  252,  252,  252,  252,  252,
 /*   220 */   252,  271,  283,   40,   40,   40,   40,  233,  256,  156,
 /*   230 */   315,  257,  257,  317,  375,  361,  344,  340,  346,  347,
 /*   240 */   352,  355,  356,  218,  335,  357,  359,  360,  362,  364,
 /*   250 */   366,  367,  368,  227,  395,  378,  377,  323,  324,  328,
 /*   260 */   473,  477,  338,  339,  376,  342,  384,  628,  482,  633,
 /*   270 */   634,  485,  638,  639,  543,  549,  507,  530,  536,  570,
 /*   280 */   534,  571,  574,  556,  560,  596,  598,  599,  604,  605,
 /*   290 */   590,  607,  608,  610,  691,  612,  600,  563,  601,  564,
 /*   300 */   617,  536,  618,  586,  620,  587,  626,  619,  621,  635,
 /*   310 */   705,  622,  624,  704,  709,  710,  711,  712,  713,  714,
 /*   320 */   715,  636,  706,  642,  640,  641,  606,  643,  702,  668,
 /*   330 */   716,  581,  582,  637,  637,  637,  637,  717,  585,  588,
 /*   340 */   637,  637,  637,  730,  733,  654,  637,  735,  736,  737,
 /*   350 */   738,  739,  740,  741,  742,  743,  744,  745,  746,  747,
 /*   360 */   748,  749,  656,  669,  750,  751,  695,  758,
};
#define YY_REDUCE_COUNT (206)
#define YY_REDUCE_MIN   (-266)
#define YY_REDUCE_MAX   (479)
static const short yy_reduce_ofst[] = {
 /*     0 */  -186,   10,   10, -163, -163,  154, -159,   -5,   42, -168,
 /*    10 */  -112, -115,  117,  -41,   30,   51,   84,   95,  167,  169,
 /*    20 */   176,  178,  179, -195, -197, -258, -239, -201, -143, -229,
 /*    30 */  -130,  -62,   59, -238,  -57,   47,  188,  191,  206,  -88,
 /*    40 */    48,  207,  115,  160,  119,  -99,   22,  215, -266, -261,
 /*    50 */  -213,  -58,  180,  224,  228,  229,  230,  232,  234,  236,
 /*    60 */   237,  238,  239,  241,  242,  243,  244,  245,  246,  247,
 /*    70 */    27,   35,  265,  254,  267,  276,  277,  278,  279,  280,
 /*    80 */   282,  300,  321,  258,  322,  325,  326,  275,  281,  327,
 /*    90 */   330,  331,  332,  259,  261,  284,  285,  334,  341,  343,
 /*   100 */   345,  348,  349,  350,  351,  353,  354,  358,  363,  365,
 /*   110 */   369,  370,  371,  372,  373,  374,  379,  380,  381,  382,
 /*   120 */   383,  385,  386,  387,  388,  389,  390,  391,  392,  393,
 /*   130 */   394,  396,  397,  398,  399,  400,  401,  402,  403,  404,
 /*   140 */   405,  406,  407,  408,  409,  410,  411,  412,  413,  414,
 /*   150 */   415,  416,  417,  418,  419,  420,  336,  337,  421,  269,
 /*   160 */   273,  288,  289,  422,  423,  292,  294,  296,  287,  305,
 /*   170 */   318,  429,  290,  432,  431,  433,  434,  435,  436,  437,
 /*   180 */   438,  439,  442,  440,  441,  444,  447,  445,  453,  454,
 /*   190 */   455,  449,  456,  457,  458,  459,  460,  466,  470,  461,
 /*   200 */   471,  474,  472,  476,  478,  479,  475,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   870,  994,  933, 1004,  920,  930, 1145, 1145, 1145,  870,
 /*    10 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*    20 */   870,  870,  870, 1052,  890, 1145,  870,  870,  870,  870,
 /*    30 */   870,  870,  870, 1067,  870,  930,  870,  870,  870,  870,
 /*    40 */   940,  930,  940,  940,  870, 1047,  978,  996,  870,  870,
 /*    50 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*    60 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*    70 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*    80 */   870,  870,  870, 1054, 1060, 1057,  870,  870,  870, 1062,
 /*    90 */   870,  870,  870, 1086, 1086, 1045,  870,  870,  870,  870,
 /*   100 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   110 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   120 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  918,
 /*   130 */   870,  916,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   140 */   870,  870,  870,  870,  870,  870,  870,  870,  901,  870,
 /*   150 */   870,  870,  870,  870,  870,  888,  892,  892,  892,  870,
 /*   160 */   870,  870,  870,  892,  892, 1093, 1097, 1079, 1091, 1087,
 /*   170 */  1074, 1072, 1070, 1078, 1101,  892,  892,  892,  938,  938,
 /*   180 */   934,  930,  930,  892,  892,  956,  954,  952,  944,  950,
 /*   190 */   946,  948,  942,  921,  870,  892,  892,  928,  928,  892,
 /*   200 */   928,  892,  928,  892,  892,  978,  996,  870, 1102, 1092,
 /*   210 */   870, 1144, 1132, 1131,  870, 1140, 1139, 1138, 1130, 1129,
 /*   220 */  1128,  870,  870, 1124, 1127, 1126, 1125,  870,  870,  870,
 /*   230 */   870, 1134, 1133,  870,  870,  870,  870,  870,  870,  870,
 /*   240 */   870,  870,  870, 1098, 1094,  870,  870,  870,  870,  870,
 /*   250 */   870,  870,  870,  870, 1104,  870,  870,  870,  870,  870,
 /*   260 */   870,  870,  870,  870, 1006,  870,  870,  870,  870,  870,
 /*   270 */   870,  870,  870,  870,  870,  870,  870, 1044,  870,  870,
 /*   280 */   870,  870,  870, 1056, 1055,  870,  870,  870,  870,  870,
 /*   290 */   870,  870,  870,  870,  870,  870, 1088,  870, 1080,  870,
 /*   300 */   870, 1018,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   310 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   320 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   330 */   870,  870,  870, 1163, 1158, 1159, 1156,  870,  870,  870,
 /*   340 */  1155, 1150, 1151,  870,  870,  870, 1148,  870,  870,  870,
 /*   350 */   870,  870,  870,  870,  870,  870,  870,  870,  870,  870,
 /*   360 */   870,  870,  962,  870,  899,  897,  870,  870,
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
    1,  /*     NMATCH => ID */
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
  /*   23 */ "NMATCH",
  /*   24 */ "GLOB",
  /*   25 */ "BETWEEN",
  /*   26 */ "IN",
  /*   27 */ "GT",
  /*   28 */ "GE",
  /*   29 */ "LT",
  /*   30 */ "LE",
  /*   31 */ "BITAND",
  /*   32 */ "BITOR",
  /*   33 */ "LSHIFT",
  /*   34 */ "RSHIFT",
  /*   35 */ "PLUS",
  /*   36 */ "MINUS",
  /*   37 */ "DIVIDE",
  /*   38 */ "TIMES",
  /*   39 */ "STAR",
  /*   40 */ "SLASH",
  /*   41 */ "REM",
  /*   42 */ "CONCAT",
  /*   43 */ "UMINUS",
  /*   44 */ "UPLUS",
  /*   45 */ "BITNOT",
  /*   46 */ "SHOW",
  /*   47 */ "DATABASES",
  /*   48 */ "TOPICS",
  /*   49 */ "FUNCTIONS",
  /*   50 */ "MNODES",
  /*   51 */ "DNODES",
  /*   52 */ "ACCOUNTS",
  /*   53 */ "USERS",
  /*   54 */ "MODULES",
  /*   55 */ "QUERIES",
  /*   56 */ "CONNECTIONS",
  /*   57 */ "STREAMS",
  /*   58 */ "VARIABLES",
  /*   59 */ "SCORES",
  /*   60 */ "GRANTS",
  /*   61 */ "VNODES",
  /*   62 */ "DOT",
  /*   63 */ "CREATE",
  /*   64 */ "TABLE",
  /*   65 */ "STABLE",
  /*   66 */ "DATABASE",
  /*   67 */ "TABLES",
  /*   68 */ "STABLES",
  /*   69 */ "VGROUPS",
  /*   70 */ "DROP",
  /*   71 */ "TOPIC",
  /*   72 */ "FUNCTION",
  /*   73 */ "DNODE",
  /*   74 */ "USER",
  /*   75 */ "ACCOUNT",
  /*   76 */ "USE",
  /*   77 */ "DESCRIBE",
  /*   78 */ "DESC",
  /*   79 */ "ALTER",
  /*   80 */ "PASS",
  /*   81 */ "PRIVILEGE",
  /*   82 */ "LOCAL",
  /*   83 */ "COMPACT",
  /*   84 */ "LP",
  /*   85 */ "RP",
  /*   86 */ "IF",
  /*   87 */ "EXISTS",
  /*   88 */ "AS",
  /*   89 */ "OUTPUTTYPE",
  /*   90 */ "AGGREGATE",
  /*   91 */ "BUFSIZE",
  /*   92 */ "PPS",
  /*   93 */ "TSERIES",
  /*   94 */ "DBS",
  /*   95 */ "STORAGE",
  /*   96 */ "QTIME",
  /*   97 */ "CONNS",
  /*   98 */ "STATE",
  /*   99 */ "COMMA",
  /*  100 */ "KEEP",
  /*  101 */ "CACHE",
  /*  102 */ "REPLICA",
  /*  103 */ "QUORUM",
  /*  104 */ "DAYS",
  /*  105 */ "MINROWS",
  /*  106 */ "MAXROWS",
  /*  107 */ "BLOCKS",
  /*  108 */ "CTIME",
  /*  109 */ "WAL",
  /*  110 */ "FSYNC",
  /*  111 */ "COMP",
  /*  112 */ "PRECISION",
  /*  113 */ "UPDATE",
  /*  114 */ "CACHELAST",
  /*  115 */ "PARTITIONS",
  /*  116 */ "UNSIGNED",
  /*  117 */ "TAGS",
  /*  118 */ "USING",
  /*  119 */ "NULL",
  /*  120 */ "NOW",
  /*  121 */ "SELECT",
  /*  122 */ "UNION",
  /*  123 */ "ALL",
  /*  124 */ "DISTINCT",
  /*  125 */ "FROM",
  /*  126 */ "VARIABLE",
  /*  127 */ "INTERVAL",
  /*  128 */ "EVERY",
  /*  129 */ "SESSION",
  /*  130 */ "STATE_WINDOW",
  /*  131 */ "FILL",
  /*  132 */ "SLIDING",
  /*  133 */ "ORDER",
  /*  134 */ "BY",
  /*  135 */ "ASC",
  /*  136 */ "GROUP",
  /*  137 */ "HAVING",
  /*  138 */ "LIMIT",
  /*  139 */ "OFFSET",
  /*  140 */ "SLIMIT",
  /*  141 */ "SOFFSET",
  /*  142 */ "WHERE",
  /*  143 */ "RESET",
  /*  144 */ "QUERY",
  /*  145 */ "SYNCDB",
  /*  146 */ "ADD",
  /*  147 */ "COLUMN",
  /*  148 */ "MODIFY",
  /*  149 */ "TAG",
  /*  150 */ "CHANGE",
  /*  151 */ "SET",
  /*  152 */ "KILL",
  /*  153 */ "CONNECTION",
  /*  154 */ "STREAM",
  /*  155 */ "COLON",
  /*  156 */ "ABORT",
  /*  157 */ "AFTER",
  /*  158 */ "ATTACH",
  /*  159 */ "BEFORE",
  /*  160 */ "BEGIN",
  /*  161 */ "CASCADE",
  /*  162 */ "CLUSTER",
  /*  163 */ "CONFLICT",
  /*  164 */ "COPY",
  /*  165 */ "DEFERRED",
  /*  166 */ "DELIMITERS",
  /*  167 */ "DETACH",
  /*  168 */ "EACH",
  /*  169 */ "END",
  /*  170 */ "EXPLAIN",
  /*  171 */ "FAIL",
  /*  172 */ "FOR",
  /*  173 */ "IGNORE",
  /*  174 */ "IMMEDIATE",
  /*  175 */ "INITIALLY",
  /*  176 */ "INSTEAD",
  /*  177 */ "KEY",
  /*  178 */ "OF",
  /*  179 */ "RAISE",
  /*  180 */ "REPLACE",
  /*  181 */ "RESTRICT",
  /*  182 */ "ROW",
  /*  183 */ "STATEMENT",
  /*  184 */ "TRIGGER",
  /*  185 */ "VIEW",
  /*  186 */ "IPTOKEN",
  /*  187 */ "SEMI",
  /*  188 */ "NONE",
  /*  189 */ "PREV",
  /*  190 */ "LINEAR",
  /*  191 */ "IMPORT",
  /*  192 */ "TBNAME",
  /*  193 */ "JOIN",
  /*  194 */ "INSERT",
  /*  195 */ "INTO",
  /*  196 */ "VALUES",
  /*  197 */ "program",
  /*  198 */ "cmd",
  /*  199 */ "ids",
  /*  200 */ "dbPrefix",
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
 /* 267 */ "expr ::= expr NMATCH expr",
 /* 268 */ "expr ::= expr IN LP exprlist RP",
 /* 269 */ "exprlist ::= exprlist COMMA expritem",
 /* 270 */ "exprlist ::= expritem",
 /* 271 */ "expritem ::= expr",
 /* 272 */ "expritem ::=",
 /* 273 */ "cmd ::= RESET QUERY CACHE",
 /* 274 */ "cmd ::= SYNCDB ids REPLICA",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 291 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 292 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 293 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 206: /* exprlist */
    case 250: /* selcollist */
    case 264: /* sclp */
{
#line 762 "sql.y"
tSqlExprListDestroy((yypminor->yy421));
#line 1521 "sql.c"
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
#line 256 "sql.y"
taosArrayDestroy((yypminor->yy421));
#line 1537 "sql.c"
}
      break;
    case 242: /* create_table_list */
{
#line 364 "sql.y"
destroyCreateTableSql((yypminor->yy438));
#line 1544 "sql.c"
}
      break;
    case 247: /* select */
{
#line 484 "sql.y"
destroySqlNode((yypminor->yy56));
#line 1551 "sql.c"
}
      break;
    case 251: /* from */
    case 268: /* tablelist */
    case 269: /* sub */
{
#line 539 "sql.y"
destroyRelationInfo((yypminor->yy8));
#line 1560 "sql.c"
}
      break;
    case 252: /* where_opt */
    case 259: /* having_opt */
    case 266: /* expr */
    case 277: /* expritem */
{
#line 691 "sql.y"
tSqlExprDestroy((yypminor->yy439));
#line 1570 "sql.c"
}
      break;
    case 263: /* union */
{
#line 492 "sql.y"
destroyAllSqlNode((yypminor->yy421));
#line 1577 "sql.c"
}
      break;
    case 273: /* sortitem */
{
#line 624 "sql.y"
tVariantDestroy(&(yypminor->yy430));
#line 1584 "sql.c"
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
   197,  /* (0) program ::= cmd */
   198,  /* (1) cmd ::= SHOW DATABASES */
   198,  /* (2) cmd ::= SHOW TOPICS */
   198,  /* (3) cmd ::= SHOW FUNCTIONS */
   198,  /* (4) cmd ::= SHOW MNODES */
   198,  /* (5) cmd ::= SHOW DNODES */
   198,  /* (6) cmd ::= SHOW ACCOUNTS */
   198,  /* (7) cmd ::= SHOW USERS */
   198,  /* (8) cmd ::= SHOW MODULES */
   198,  /* (9) cmd ::= SHOW QUERIES */
   198,  /* (10) cmd ::= SHOW CONNECTIONS */
   198,  /* (11) cmd ::= SHOW STREAMS */
   198,  /* (12) cmd ::= SHOW VARIABLES */
   198,  /* (13) cmd ::= SHOW SCORES */
   198,  /* (14) cmd ::= SHOW GRANTS */
   198,  /* (15) cmd ::= SHOW VNODES */
   198,  /* (16) cmd ::= SHOW VNODES ids */
   200,  /* (17) dbPrefix ::= */
   200,  /* (18) dbPrefix ::= ids DOT */
   201,  /* (19) cpxName ::= */
   201,  /* (20) cpxName ::= DOT ids */
   198,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   198,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   198,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   198,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   198,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   198,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   198,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   198,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   198,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   198,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   198,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   198,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   198,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   198,  /* (34) cmd ::= DROP FUNCTION ids */
   198,  /* (35) cmd ::= DROP DNODE ids */
   198,  /* (36) cmd ::= DROP USER ids */
   198,  /* (37) cmd ::= DROP ACCOUNT ids */
   198,  /* (38) cmd ::= USE ids */
   198,  /* (39) cmd ::= DESCRIBE ids cpxName */
   198,  /* (40) cmd ::= DESC ids cpxName */
   198,  /* (41) cmd ::= ALTER USER ids PASS ids */
   198,  /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
   198,  /* (43) cmd ::= ALTER DNODE ids ids */
   198,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   198,  /* (45) cmd ::= ALTER LOCAL ids */
   198,  /* (46) cmd ::= ALTER LOCAL ids ids */
   198,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   198,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   198,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   198,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   198,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   199,  /* (52) ids ::= ID */
   199,  /* (53) ids ::= STRING */
   202,  /* (54) ifexists ::= IF EXISTS */
   202,  /* (55) ifexists ::= */
   207,  /* (56) ifnotexists ::= IF NOT EXISTS */
   207,  /* (57) ifnotexists ::= */
   198,  /* (58) cmd ::= CREATE DNODE ids */
   198,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   198,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   198,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   198,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   198,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   198,  /* (64) cmd ::= CREATE USER ids PASS ids */
   211,  /* (65) bufsize ::= */
   211,  /* (66) bufsize ::= BUFSIZE INTEGER */
   212,  /* (67) pps ::= */
   212,  /* (68) pps ::= PPS INTEGER */
   213,  /* (69) tseries ::= */
   213,  /* (70) tseries ::= TSERIES INTEGER */
   214,  /* (71) dbs ::= */
   214,  /* (72) dbs ::= DBS INTEGER */
   215,  /* (73) streams ::= */
   215,  /* (74) streams ::= STREAMS INTEGER */
   216,  /* (75) storage ::= */
   216,  /* (76) storage ::= STORAGE INTEGER */
   217,  /* (77) qtime ::= */
   217,  /* (78) qtime ::= QTIME INTEGER */
   218,  /* (79) users ::= */
   218,  /* (80) users ::= USERS INTEGER */
   219,  /* (81) conns ::= */
   219,  /* (82) conns ::= CONNS INTEGER */
   220,  /* (83) state ::= */
   220,  /* (84) state ::= STATE ids */
   205,  /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   221,  /* (86) intitemlist ::= intitemlist COMMA intitem */
   221,  /* (87) intitemlist ::= intitem */
   222,  /* (88) intitem ::= INTEGER */
   223,  /* (89) keep ::= KEEP intitemlist */
   224,  /* (90) cache ::= CACHE INTEGER */
   225,  /* (91) replica ::= REPLICA INTEGER */
   226,  /* (92) quorum ::= QUORUM INTEGER */
   227,  /* (93) days ::= DAYS INTEGER */
   228,  /* (94) minrows ::= MINROWS INTEGER */
   229,  /* (95) maxrows ::= MAXROWS INTEGER */
   230,  /* (96) blocks ::= BLOCKS INTEGER */
   231,  /* (97) ctime ::= CTIME INTEGER */
   232,  /* (98) wal ::= WAL INTEGER */
   233,  /* (99) fsync ::= FSYNC INTEGER */
   234,  /* (100) comp ::= COMP INTEGER */
   235,  /* (101) prec ::= PRECISION STRING */
   236,  /* (102) update ::= UPDATE INTEGER */
   237,  /* (103) cachelast ::= CACHELAST INTEGER */
   238,  /* (104) partitions ::= PARTITIONS INTEGER */
   208,  /* (105) db_optr ::= */
   208,  /* (106) db_optr ::= db_optr cache */
   208,  /* (107) db_optr ::= db_optr replica */
   208,  /* (108) db_optr ::= db_optr quorum */
   208,  /* (109) db_optr ::= db_optr days */
   208,  /* (110) db_optr ::= db_optr minrows */
   208,  /* (111) db_optr ::= db_optr maxrows */
   208,  /* (112) db_optr ::= db_optr blocks */
   208,  /* (113) db_optr ::= db_optr ctime */
   208,  /* (114) db_optr ::= db_optr wal */
   208,  /* (115) db_optr ::= db_optr fsync */
   208,  /* (116) db_optr ::= db_optr comp */
   208,  /* (117) db_optr ::= db_optr prec */
   208,  /* (118) db_optr ::= db_optr keep */
   208,  /* (119) db_optr ::= db_optr update */
   208,  /* (120) db_optr ::= db_optr cachelast */
   209,  /* (121) topic_optr ::= db_optr */
   209,  /* (122) topic_optr ::= topic_optr partitions */
   203,  /* (123) alter_db_optr ::= */
   203,  /* (124) alter_db_optr ::= alter_db_optr replica */
   203,  /* (125) alter_db_optr ::= alter_db_optr quorum */
   203,  /* (126) alter_db_optr ::= alter_db_optr keep */
   203,  /* (127) alter_db_optr ::= alter_db_optr blocks */
   203,  /* (128) alter_db_optr ::= alter_db_optr comp */
   203,  /* (129) alter_db_optr ::= alter_db_optr update */
   203,  /* (130) alter_db_optr ::= alter_db_optr cachelast */
   204,  /* (131) alter_topic_optr ::= alter_db_optr */
   204,  /* (132) alter_topic_optr ::= alter_topic_optr partitions */
   210,  /* (133) typename ::= ids */
   210,  /* (134) typename ::= ids LP signed RP */
   210,  /* (135) typename ::= ids UNSIGNED */
   239,  /* (136) signed ::= INTEGER */
   239,  /* (137) signed ::= PLUS INTEGER */
   239,  /* (138) signed ::= MINUS INTEGER */
   198,  /* (139) cmd ::= CREATE TABLE create_table_args */
   198,  /* (140) cmd ::= CREATE TABLE create_stable_args */
   198,  /* (141) cmd ::= CREATE STABLE create_stable_args */
   198,  /* (142) cmd ::= CREATE TABLE create_table_list */
   242,  /* (143) create_table_list ::= create_from_stable */
   242,  /* (144) create_table_list ::= create_table_list create_from_stable */
   240,  /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   241,  /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   243,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   243,  /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   246,  /* (149) tagNamelist ::= tagNamelist COMMA ids */
   246,  /* (150) tagNamelist ::= ids */
   240,  /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
   244,  /* (152) columnlist ::= columnlist COMMA column */
   244,  /* (153) columnlist ::= column */
   248,  /* (154) column ::= ids typename */
   245,  /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
   245,  /* (156) tagitemlist ::= tagitem */
   249,  /* (157) tagitem ::= INTEGER */
   249,  /* (158) tagitem ::= FLOAT */
   249,  /* (159) tagitem ::= STRING */
   249,  /* (160) tagitem ::= BOOL */
   249,  /* (161) tagitem ::= NULL */
   249,  /* (162) tagitem ::= NOW */
   249,  /* (163) tagitem ::= MINUS INTEGER */
   249,  /* (164) tagitem ::= MINUS FLOAT */
   249,  /* (165) tagitem ::= PLUS INTEGER */
   249,  /* (166) tagitem ::= PLUS FLOAT */
   247,  /* (167) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   247,  /* (168) select ::= LP select RP */
   263,  /* (169) union ::= select */
   263,  /* (170) union ::= union UNION ALL select */
   198,  /* (171) cmd ::= union */
   247,  /* (172) select ::= SELECT selcollist */
   264,  /* (173) sclp ::= selcollist COMMA */
   264,  /* (174) sclp ::= */
   250,  /* (175) selcollist ::= sclp distinct expr as */
   250,  /* (176) selcollist ::= sclp STAR */
   267,  /* (177) as ::= AS ids */
   267,  /* (178) as ::= ids */
   267,  /* (179) as ::= */
   265,  /* (180) distinct ::= DISTINCT */
   265,  /* (181) distinct ::= */
   251,  /* (182) from ::= FROM tablelist */
   251,  /* (183) from ::= FROM sub */
   269,  /* (184) sub ::= LP union RP */
   269,  /* (185) sub ::= LP union RP ids */
   269,  /* (186) sub ::= sub COMMA LP union RP ids */
   268,  /* (187) tablelist ::= ids cpxName */
   268,  /* (188) tablelist ::= ids cpxName ids */
   268,  /* (189) tablelist ::= tablelist COMMA ids cpxName */
   268,  /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
   270,  /* (191) tmvar ::= VARIABLE */
   253,  /* (192) interval_option ::= intervalKey LP tmvar RP */
   253,  /* (193) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   253,  /* (194) interval_option ::= */
   271,  /* (195) intervalKey ::= INTERVAL */
   271,  /* (196) intervalKey ::= EVERY */
   255,  /* (197) session_option ::= */
   255,  /* (198) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   256,  /* (199) windowstate_option ::= */
   256,  /* (200) windowstate_option ::= STATE_WINDOW LP ids RP */
   257,  /* (201) fill_opt ::= */
   257,  /* (202) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   257,  /* (203) fill_opt ::= FILL LP ID RP */
   254,  /* (204) sliding_opt ::= SLIDING LP tmvar RP */
   254,  /* (205) sliding_opt ::= */
   260,  /* (206) orderby_opt ::= */
   260,  /* (207) orderby_opt ::= ORDER BY sortlist */
   272,  /* (208) sortlist ::= sortlist COMMA item sortorder */
   272,  /* (209) sortlist ::= item sortorder */
   274,  /* (210) item ::= ids cpxName */
   275,  /* (211) sortorder ::= ASC */
   275,  /* (212) sortorder ::= DESC */
   275,  /* (213) sortorder ::= */
   258,  /* (214) groupby_opt ::= */
   258,  /* (215) groupby_opt ::= GROUP BY grouplist */
   276,  /* (216) grouplist ::= grouplist COMMA item */
   276,  /* (217) grouplist ::= item */
   259,  /* (218) having_opt ::= */
   259,  /* (219) having_opt ::= HAVING expr */
   262,  /* (220) limit_opt ::= */
   262,  /* (221) limit_opt ::= LIMIT signed */
   262,  /* (222) limit_opt ::= LIMIT signed OFFSET signed */
   262,  /* (223) limit_opt ::= LIMIT signed COMMA signed */
   261,  /* (224) slimit_opt ::= */
   261,  /* (225) slimit_opt ::= SLIMIT signed */
   261,  /* (226) slimit_opt ::= SLIMIT signed SOFFSET signed */
   261,  /* (227) slimit_opt ::= SLIMIT signed COMMA signed */
   252,  /* (228) where_opt ::= */
   252,  /* (229) where_opt ::= WHERE expr */
   266,  /* (230) expr ::= LP expr RP */
   266,  /* (231) expr ::= ID */
   266,  /* (232) expr ::= ID DOT ID */
   266,  /* (233) expr ::= ID DOT STAR */
   266,  /* (234) expr ::= INTEGER */
   266,  /* (235) expr ::= MINUS INTEGER */
   266,  /* (236) expr ::= PLUS INTEGER */
   266,  /* (237) expr ::= FLOAT */
   266,  /* (238) expr ::= MINUS FLOAT */
   266,  /* (239) expr ::= PLUS FLOAT */
   266,  /* (240) expr ::= STRING */
   266,  /* (241) expr ::= NOW */
   266,  /* (242) expr ::= VARIABLE */
   266,  /* (243) expr ::= PLUS VARIABLE */
   266,  /* (244) expr ::= MINUS VARIABLE */
   266,  /* (245) expr ::= BOOL */
   266,  /* (246) expr ::= NULL */
   266,  /* (247) expr ::= ID LP exprlist RP */
   266,  /* (248) expr ::= ID LP STAR RP */
   266,  /* (249) expr ::= expr IS NULL */
   266,  /* (250) expr ::= expr IS NOT NULL */
   266,  /* (251) expr ::= expr LT expr */
   266,  /* (252) expr ::= expr GT expr */
   266,  /* (253) expr ::= expr LE expr */
   266,  /* (254) expr ::= expr GE expr */
   266,  /* (255) expr ::= expr NE expr */
   266,  /* (256) expr ::= expr EQ expr */
   266,  /* (257) expr ::= expr BETWEEN expr AND expr */
   266,  /* (258) expr ::= expr AND expr */
   266,  /* (259) expr ::= expr OR expr */
   266,  /* (260) expr ::= expr PLUS expr */
   266,  /* (261) expr ::= expr MINUS expr */
   266,  /* (262) expr ::= expr STAR expr */
   266,  /* (263) expr ::= expr SLASH expr */
   266,  /* (264) expr ::= expr REM expr */
   266,  /* (265) expr ::= expr LIKE expr */
   266,  /* (266) expr ::= expr MATCH expr */
   266,  /* (267) expr ::= expr NMATCH expr */
   266,  /* (268) expr ::= expr IN LP exprlist RP */
   206,  /* (269) exprlist ::= exprlist COMMA expritem */
   206,  /* (270) exprlist ::= expritem */
   277,  /* (271) expritem ::= expr */
   277,  /* (272) expritem ::= */
   198,  /* (273) cmd ::= RESET QUERY CACHE */
   198,  /* (274) cmd ::= SYNCDB ids REPLICA */
   198,  /* (275) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   198,  /* (276) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   198,  /* (277) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   198,  /* (278) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   198,  /* (279) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   198,  /* (280) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   198,  /* (281) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   198,  /* (282) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   198,  /* (283) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   198,  /* (284) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   198,  /* (285) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   198,  /* (286) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   198,  /* (287) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   198,  /* (288) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   198,  /* (289) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   198,  /* (290) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   198,  /* (291) cmd ::= KILL CONNECTION INTEGER */
   198,  /* (292) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   198,  /* (293) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -3,  /* (267) expr ::= expr NMATCH expr */
   -5,  /* (268) expr ::= expr IN LP exprlist RP */
   -3,  /* (269) exprlist ::= exprlist COMMA expritem */
   -1,  /* (270) exprlist ::= expritem */
   -1,  /* (271) expritem ::= expr */
    0,  /* (272) expritem ::= */
   -3,  /* (273) cmd ::= RESET QUERY CACHE */
   -3,  /* (274) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (275) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (276) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (277) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (278) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (279) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (280) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (281) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (282) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (283) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (284) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (285) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (286) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (287) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (288) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (289) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (290) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (291) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (292) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (293) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
#line 2561 "sql.c"
        break;
      case 1: /* cmd ::= SHOW DATABASES */
#line 66 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
#line 2566 "sql.c"
        break;
      case 2: /* cmd ::= SHOW TOPICS */
#line 67 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
#line 2571 "sql.c"
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
#line 68 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
#line 2576 "sql.c"
        break;
      case 4: /* cmd ::= SHOW MNODES */
#line 69 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
#line 2581 "sql.c"
        break;
      case 5: /* cmd ::= SHOW DNODES */
#line 70 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
#line 2586 "sql.c"
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
#line 71 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
#line 2591 "sql.c"
        break;
      case 7: /* cmd ::= SHOW USERS */
#line 72 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
#line 2596 "sql.c"
        break;
      case 8: /* cmd ::= SHOW MODULES */
#line 74 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
#line 2601 "sql.c"
        break;
      case 9: /* cmd ::= SHOW QUERIES */
#line 75 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
#line 2606 "sql.c"
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
#line 76 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
#line 2611 "sql.c"
        break;
      case 11: /* cmd ::= SHOW STREAMS */
#line 77 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
#line 2616 "sql.c"
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
#line 78 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
#line 2621 "sql.c"
        break;
      case 13: /* cmd ::= SHOW SCORES */
#line 79 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
#line 2626 "sql.c"
        break;
      case 14: /* cmd ::= SHOW GRANTS */
#line 80 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
#line 2631 "sql.c"
        break;
      case 15: /* cmd ::= SHOW VNODES */
#line 82 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
#line 2636 "sql.c"
        break;
      case 16: /* cmd ::= SHOW VNODES ids */
#line 83 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
#line 2641 "sql.c"
        break;
      case 17: /* dbPrefix ::= */
#line 87 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
#line 2646 "sql.c"
        break;
      case 18: /* dbPrefix ::= ids DOT */
#line 88 "sql.y"
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
#line 2651 "sql.c"
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
#line 91 "sql.y"
{yymsp[1].minor.yy0.n = 0;  }
#line 2657 "sql.c"
        break;
      case 20: /* cpxName ::= DOT ids */
#line 92 "sql.y"
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
#line 2662 "sql.c"
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
#line 94 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2670 "sql.c"
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
#line 98 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2678 "sql.c"
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
#line 103 "sql.y"
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
#line 2685 "sql.c"
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
#line 107 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
#line 2692 "sql.c"
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
#line 111 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
#line 2699 "sql.c"
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
#line 115 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
#line 2706 "sql.c"
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
#line 119 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
#line 2715 "sql.c"
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
#line 125 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
#line 2724 "sql.c"
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
#line 131 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
#line 2733 "sql.c"
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
#line 138 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
#line 2741 "sql.c"
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
#line 144 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
#line 2749 "sql.c"
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
#line 149 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
#line 2754 "sql.c"
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
#line 150 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
#line 2759 "sql.c"
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
#line 151 "sql.y"
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
#line 2764 "sql.c"
        break;
      case 35: /* cmd ::= DROP DNODE ids */
#line 153 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
#line 2769 "sql.c"
        break;
      case 36: /* cmd ::= DROP USER ids */
#line 154 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
#line 2774 "sql.c"
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
#line 155 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
#line 2779 "sql.c"
        break;
      case 38: /* cmd ::= USE ids */
#line 158 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
#line 2784 "sql.c"
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
#line 161 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2793 "sql.c"
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
#line 170 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
#line 2798 "sql.c"
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
#line 171 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
#line 2803 "sql.c"
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
#line 172 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2808 "sql.c"
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
#line 173 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
#line 2813 "sql.c"
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
#line 174 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
#line 2818 "sql.c"
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
#line 175 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2823 "sql.c"
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 48: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==48);
#line 176 "sql.y"
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &t);}
#line 2829 "sql.c"
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
#line 179 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy171);}
#line 2834 "sql.c"
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
#line 180 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
#line 2839 "sql.c"
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
#line 184 "sql.y"
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy421);}
#line 2844 "sql.c"
        break;
      case 52: /* ids ::= ID */
      case 53: /* ids ::= STRING */ yytestcase(yyruleno==53);
#line 190 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
#line 2850 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 54: /* ifexists ::= IF EXISTS */
#line 194 "sql.y"
{ yymsp[-1].minor.yy0.n = 1;}
#line 2856 "sql.c"
        break;
      case 55: /* ifexists ::= */
      case 57: /* ifnotexists ::= */ yytestcase(yyruleno==57);
      case 181: /* distinct ::= */ yytestcase(yyruleno==181);
#line 195 "sql.y"
{ yymsp[1].minor.yy0.n = 0;}
#line 2863 "sql.c"
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
#line 198 "sql.y"
{ yymsp[-2].minor.yy0.n = 1;}
#line 2868 "sql.c"
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
#line 203 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
#line 2873 "sql.c"
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
#line 205 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
#line 2878 "sql.c"
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
#line 206 "sql.y"
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &yymsp[-2].minor.yy0);}
#line 2884 "sql.c"
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 208 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy183, &yymsp[0].minor.yy0, 1);}
#line 2889 "sql.c"
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 209 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy183, &yymsp[0].minor.yy0, 2);}
#line 2894 "sql.c"
        break;
      case 64: /* cmd ::= CREATE USER ids PASS ids */
#line 210 "sql.y"
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 2899 "sql.c"
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
#line 2913 "sql.c"
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
#line 2927 "sql.c"
        break;
      case 85: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
#line 243 "sql.y"
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
#line 2942 "sql.c"
  yymsp[-8].minor.yy171 = yylhsminor.yy171;
        break;
      case 86: /* intitemlist ::= intitemlist COMMA intitem */
      case 155: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==155);
#line 259 "sql.y"
{ yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);    }
#line 2949 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
#line 260 "sql.y"
{ yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1); }
#line 2956 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
#line 262 "sql.y"
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
#line 2966 "sql.c"
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 89: /* keep ::= KEEP intitemlist */
#line 266 "sql.y"
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
#line 2972 "sql.c"
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
#line 2991 "sql.c"
        break;
      case 105: /* db_optr ::= */
#line 285 "sql.y"
{setDefaultCreateDbOption(&yymsp[1].minor.yy90); yymsp[1].minor.yy90.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 2996 "sql.c"
        break;
      case 106: /* db_optr ::= db_optr cache */
#line 287 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3001 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 107: /* db_optr ::= db_optr replica */
      case 124: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==124);
#line 288 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3008 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 108: /* db_optr ::= db_optr quorum */
      case 125: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==125);
#line 289 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3015 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 109: /* db_optr ::= db_optr days */
#line 290 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3021 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 110: /* db_optr ::= db_optr minrows */
#line 291 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3027 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 111: /* db_optr ::= db_optr maxrows */
#line 292 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3033 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 112: /* db_optr ::= db_optr blocks */
      case 127: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==127);
#line 293 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3040 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 113: /* db_optr ::= db_optr ctime */
#line 294 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3046 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 114: /* db_optr ::= db_optr wal */
#line 295 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3052 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 115: /* db_optr ::= db_optr fsync */
#line 296 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3058 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 116: /* db_optr ::= db_optr comp */
      case 128: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==128);
#line 297 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3065 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 117: /* db_optr ::= db_optr prec */
#line 298 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.precision = yymsp[0].minor.yy0; }
#line 3071 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 118: /* db_optr ::= db_optr keep */
      case 126: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==126);
#line 299 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.keep = yymsp[0].minor.yy421; }
#line 3078 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 119: /* db_optr ::= db_optr update */
      case 129: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==129);
#line 300 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3085 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 120: /* db_optr ::= db_optr cachelast */
      case 130: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==130);
#line 301 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3092 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 121: /* topic_optr ::= db_optr */
      case 131: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==131);
#line 305 "sql.y"
{ yylhsminor.yy90 = yymsp[0].minor.yy90; yylhsminor.yy90.dbType = TSDB_DB_TYPE_TOPIC; }
#line 3099 "sql.c"
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 122: /* topic_optr ::= topic_optr partitions */
      case 132: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==132);
#line 306 "sql.y"
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3106 "sql.c"
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 123: /* alter_db_optr ::= */
#line 309 "sql.y"
{ setDefaultCreateDbOption(&yymsp[1].minor.yy90); yymsp[1].minor.yy90.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 3112 "sql.c"
        break;
      case 133: /* typename ::= ids */
#line 329 "sql.y"
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy183, &yymsp[0].minor.yy0);
}
#line 3120 "sql.c"
  yymsp[0].minor.yy183 = yylhsminor.yy183;
        break;
      case 134: /* typename ::= ids LP signed RP */
#line 335 "sql.y"
{
  if (yymsp[-1].minor.yy325 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy325;  // negative value of name length
    tSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  }
}
#line 3134 "sql.c"
  yymsp[-3].minor.yy183 = yylhsminor.yy183;
        break;
      case 135: /* typename ::= ids UNSIGNED */
#line 346 "sql.y"
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy183, &yymsp[-1].minor.yy0);
}
#line 3144 "sql.c"
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 136: /* signed ::= INTEGER */
#line 353 "sql.y"
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3150 "sql.c"
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 137: /* signed ::= PLUS INTEGER */
#line 354 "sql.y"
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3156 "sql.c"
        break;
      case 138: /* signed ::= MINUS INTEGER */
#line 355 "sql.y"
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
#line 3161 "sql.c"
        break;
      case 142: /* cmd ::= CREATE TABLE create_table_list */
#line 361 "sql.y"
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
#line 3166 "sql.c"
        break;
      case 143: /* create_table_list ::= create_from_stable */
#line 365 "sql.y"
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy438 = pCreateTable;
}
#line 3178 "sql.c"
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 144: /* create_table_list ::= create_table_list create_from_stable */
#line 374 "sql.y"
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
#line 3187 "sql.c"
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
#line 380 "sql.y"
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
#line 3199 "sql.c"
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 146: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
#line 390 "sql.y"
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3211 "sql.c"
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
#line 401 "sql.y"
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3221 "sql.c"
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
#line 407 "sql.y"
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
#line 3231 "sql.c"
  yymsp[-12].minor.yy152 = yylhsminor.yy152;
        break;
      case 149: /* tagNamelist ::= tagNamelist COMMA ids */
#line 415 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
#line 3237 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 150: /* tagNamelist ::= ids */
#line 416 "sql.y"
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0);}
#line 3243 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 151: /* create_table_args ::= ifnotexists ids cpxName AS select */
#line 420 "sql.y"
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy56, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
#line 3255 "sql.c"
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 152: /* columnlist ::= columnlist COMMA column */
#line 431 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy183); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
#line 3261 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 153: /* columnlist ::= column */
#line 432 "sql.y"
{yylhsminor.yy421 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy183);}
#line 3267 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 154: /* column ::= ids typename */
#line 436 "sql.y"
{
  tSetColumnInfo(&yylhsminor.yy183, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);
}
#line 3275 "sql.c"
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 161: /* tagitem ::= NULL */
#line 451 "sql.y"
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
#line 3281 "sql.c"
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 162: /* tagitem ::= NOW */
#line 452 "sql.y"
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0);}
#line 3287 "sql.c"
  yymsp[0].minor.yy430 = yylhsminor.yy430;
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
    tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
#line 3301 "sql.c"
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
#line 485 "sql.y"
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy421, yymsp[-11].minor.yy8, yymsp[-10].minor.yy439, yymsp[-4].minor.yy421, yymsp[-2].minor.yy421, &yymsp[-9].minor.yy400, &yymsp[-7].minor.yy147, &yymsp[-6].minor.yy40, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, &yymsp[0].minor.yy166, &yymsp[-1].minor.yy166, yymsp[-3].minor.yy439);
}
#line 3309 "sql.c"
  yymsp[-13].minor.yy56 = yylhsminor.yy56;
        break;
      case 168: /* select ::= LP select RP */
#line 489 "sql.y"
{yymsp[-2].minor.yy56 = yymsp[-1].minor.yy56;}
#line 3315 "sql.c"
        break;
      case 169: /* union ::= select */
#line 493 "sql.y"
{ yylhsminor.yy421 = setSubclause(NULL, yymsp[0].minor.yy56); }
#line 3320 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 170: /* union ::= union UNION ALL select */
#line 494 "sql.y"
{ yylhsminor.yy421 = appendSelectClause(yymsp[-3].minor.yy421, yymsp[0].minor.yy56); }
#line 3326 "sql.c"
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 171: /* cmd ::= union */
#line 496 "sql.y"
{ setSqlInfo(pInfo, yymsp[0].minor.yy421, NULL, TSDB_SQL_SELECT); }
#line 3332 "sql.c"
        break;
      case 172: /* select ::= SELECT selcollist */
#line 503 "sql.y"
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy421, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
#line 3339 "sql.c"
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 173: /* sclp ::= selcollist COMMA */
#line 515 "sql.y"
{yylhsminor.yy421 = yymsp[-1].minor.yy421;}
#line 3345 "sql.c"
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 174: /* sclp ::= */
      case 206: /* orderby_opt ::= */ yytestcase(yyruleno==206);
#line 516 "sql.y"
{yymsp[1].minor.yy421 = 0;}
#line 3352 "sql.c"
        break;
      case 175: /* selcollist ::= sclp distinct expr as */
#line 517 "sql.y"
{
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-3].minor.yy421, yymsp[-1].minor.yy439,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
#line 3359 "sql.c"
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 176: /* selcollist ::= sclp STAR */
#line 521 "sql.y"
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-1].minor.yy421, pNode, 0, 0);
}
#line 3368 "sql.c"
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 177: /* as ::= AS ids */
#line 529 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
#line 3374 "sql.c"
        break;
      case 178: /* as ::= ids */
#line 530 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
#line 3379 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* as ::= */
#line 531 "sql.y"
{ yymsp[1].minor.yy0.n = 0;  }
#line 3385 "sql.c"
        break;
      case 180: /* distinct ::= DISTINCT */
#line 534 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
#line 3390 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* from ::= FROM tablelist */
      case 183: /* from ::= FROM sub */ yytestcase(yyruleno==183);
#line 540 "sql.y"
{yymsp[-1].minor.yy8 = yymsp[0].minor.yy8;}
#line 3397 "sql.c"
        break;
      case 184: /* sub ::= LP union RP */
#line 545 "sql.y"
{yymsp[-2].minor.yy8 = addSubqueryElem(NULL, yymsp[-1].minor.yy421, NULL);}
#line 3402 "sql.c"
        break;
      case 185: /* sub ::= LP union RP ids */
#line 546 "sql.y"
{yymsp[-3].minor.yy8 = addSubqueryElem(NULL, yymsp[-2].minor.yy421, &yymsp[0].minor.yy0);}
#line 3407 "sql.c"
        break;
      case 186: /* sub ::= sub COMMA LP union RP ids */
#line 547 "sql.y"
{yylhsminor.yy8 = addSubqueryElem(yymsp[-5].minor.yy8, yymsp[-2].minor.yy421, &yymsp[0].minor.yy0);}
#line 3412 "sql.c"
  yymsp[-5].minor.yy8 = yylhsminor.yy8;
        break;
      case 187: /* tablelist ::= ids cpxName */
#line 551 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
#line 3421 "sql.c"
  yymsp[-1].minor.yy8 = yylhsminor.yy8;
        break;
      case 188: /* tablelist ::= ids cpxName ids */
#line 556 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3430 "sql.c"
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName */
#line 561 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-3].minor.yy8, &yymsp[-1].minor.yy0, NULL);
}
#line 3439 "sql.c"
  yymsp[-3].minor.yy8 = yylhsminor.yy8;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName ids */
#line 566 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-4].minor.yy8, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3448 "sql.c"
  yymsp[-4].minor.yy8 = yylhsminor.yy8;
        break;
      case 191: /* tmvar ::= VARIABLE */
#line 573 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
#line 3454 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* interval_option ::= intervalKey LP tmvar RP */
#line 576 "sql.y"
{yylhsminor.yy400.interval = yymsp[-1].minor.yy0; yylhsminor.yy400.offset.n = 0; yylhsminor.yy400.token = yymsp[-3].minor.yy104;}
#line 3460 "sql.c"
  yymsp[-3].minor.yy400 = yylhsminor.yy400;
        break;
      case 193: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
#line 577 "sql.y"
{yylhsminor.yy400.interval = yymsp[-3].minor.yy0; yylhsminor.yy400.offset = yymsp[-1].minor.yy0;   yylhsminor.yy400.token = yymsp[-5].minor.yy104;}
#line 3466 "sql.c"
  yymsp[-5].minor.yy400 = yylhsminor.yy400;
        break;
      case 194: /* interval_option ::= */
#line 578 "sql.y"
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
#line 3472 "sql.c"
        break;
      case 195: /* intervalKey ::= INTERVAL */
#line 581 "sql.y"
{yymsp[0].minor.yy104 = TK_INTERVAL;}
#line 3477 "sql.c"
        break;
      case 196: /* intervalKey ::= EVERY */
#line 582 "sql.y"
{yymsp[0].minor.yy104 = TK_EVERY;   }
#line 3482 "sql.c"
        break;
      case 197: /* session_option ::= */
#line 585 "sql.y"
{yymsp[1].minor.yy147.col.n = 0; yymsp[1].minor.yy147.gap.n = 0;}
#line 3487 "sql.c"
        break;
      case 198: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
#line 586 "sql.y"
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy147.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy147.gap = yymsp[-1].minor.yy0;
}
#line 3496 "sql.c"
        break;
      case 199: /* windowstate_option ::= */
#line 593 "sql.y"
{ yymsp[1].minor.yy40.col.n = 0; yymsp[1].minor.yy40.col.z = NULL;}
#line 3501 "sql.c"
        break;
      case 200: /* windowstate_option ::= STATE_WINDOW LP ids RP */
#line 594 "sql.y"
{ yymsp[-3].minor.yy40.col = yymsp[-1].minor.yy0; }
#line 3506 "sql.c"
        break;
      case 201: /* fill_opt ::= */
#line 598 "sql.y"
{ yymsp[1].minor.yy421 = 0;     }
#line 3511 "sql.c"
        break;
      case 202: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
#line 599 "sql.y"
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
#line 3523 "sql.c"
        break;
      case 203: /* fill_opt ::= FILL LP ID RP */
#line 608 "sql.y"
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
#line 3531 "sql.c"
        break;
      case 204: /* sliding_opt ::= SLIDING LP tmvar RP */
#line 614 "sql.y"
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
#line 3536 "sql.c"
        break;
      case 205: /* sliding_opt ::= */
#line 615 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
#line 3541 "sql.c"
        break;
      case 207: /* orderby_opt ::= ORDER BY sortlist */
#line 627 "sql.y"
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
#line 3546 "sql.c"
        break;
      case 208: /* sortlist ::= sortlist COMMA item sortorder */
#line 629 "sql.y"
{
    yylhsminor.yy421 = tVariantListAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
#line 3553 "sql.c"
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 209: /* sortlist ::= item sortorder */
#line 633 "sql.y"
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
#line 3561 "sql.c"
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 210: /* item ::= ids cpxName */
#line 638 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
#line 3572 "sql.c"
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 211: /* sortorder ::= ASC */
#line 646 "sql.y"
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
#line 3578 "sql.c"
        break;
      case 212: /* sortorder ::= DESC */
#line 647 "sql.y"
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
#line 3583 "sql.c"
        break;
      case 213: /* sortorder ::= */
#line 648 "sql.y"
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
#line 3588 "sql.c"
        break;
      case 214: /* groupby_opt ::= */
#line 656 "sql.y"
{ yymsp[1].minor.yy421 = 0;}
#line 3593 "sql.c"
        break;
      case 215: /* groupby_opt ::= GROUP BY grouplist */
#line 657 "sql.y"
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
#line 3598 "sql.c"
        break;
      case 216: /* grouplist ::= grouplist COMMA item */
#line 659 "sql.y"
{
  yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);
}
#line 3605 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 217: /* grouplist ::= item */
#line 663 "sql.y"
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1);
}
#line 3613 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 218: /* having_opt ::= */
      case 228: /* where_opt ::= */ yytestcase(yyruleno==228);
      case 272: /* expritem ::= */ yytestcase(yyruleno==272);
#line 670 "sql.y"
{yymsp[1].minor.yy439 = 0;}
#line 3621 "sql.c"
        break;
      case 219: /* having_opt ::= HAVING expr */
      case 229: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==229);
#line 671 "sql.y"
{yymsp[-1].minor.yy439 = yymsp[0].minor.yy439;}
#line 3627 "sql.c"
        break;
      case 220: /* limit_opt ::= */
      case 224: /* slimit_opt ::= */ yytestcase(yyruleno==224);
#line 675 "sql.y"
{yymsp[1].minor.yy166.limit = -1; yymsp[1].minor.yy166.offset = 0;}
#line 3633 "sql.c"
        break;
      case 221: /* limit_opt ::= LIMIT signed */
      case 225: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==225);
#line 676 "sql.y"
{yymsp[-1].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy166.offset = 0;}
#line 3639 "sql.c"
        break;
      case 222: /* limit_opt ::= LIMIT signed OFFSET signed */
#line 678 "sql.y"
{ yymsp[-3].minor.yy166.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[0].minor.yy325;}
#line 3644 "sql.c"
        break;
      case 223: /* limit_opt ::= LIMIT signed COMMA signed */
#line 680 "sql.y"
{ yymsp[-3].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[-2].minor.yy325;}
#line 3649 "sql.c"
        break;
      case 226: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
#line 686 "sql.y"
{yymsp[-3].minor.yy166.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[0].minor.yy325;}
#line 3654 "sql.c"
        break;
      case 227: /* slimit_opt ::= SLIMIT signed COMMA signed */
#line 688 "sql.y"
{yymsp[-3].minor.yy166.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy166.offset = yymsp[-2].minor.yy325;}
#line 3659 "sql.c"
        break;
      case 230: /* expr ::= LP expr RP */
#line 701 "sql.y"
{yylhsminor.yy439 = yymsp[-1].minor.yy439; yylhsminor.yy439->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy439->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
#line 3664 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 231: /* expr ::= ID */
#line 703 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
#line 3670 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 232: /* expr ::= ID DOT ID */
#line 704 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
#line 3676 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 233: /* expr ::= ID DOT STAR */
#line 705 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
#line 3682 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 234: /* expr ::= INTEGER */
#line 707 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
#line 3688 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 235: /* expr ::= MINUS INTEGER */
      case 236: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==236);
#line 708 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
#line 3695 "sql.c"
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 237: /* expr ::= FLOAT */
#line 710 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
#line 3701 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 238: /* expr ::= MINUS FLOAT */
      case 239: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==239);
#line 711 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
#line 3708 "sql.c"
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 240: /* expr ::= STRING */
#line 713 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
#line 3714 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 241: /* expr ::= NOW */
#line 714 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
#line 3720 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 242: /* expr ::= VARIABLE */
#line 715 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
#line 3726 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 243: /* expr ::= PLUS VARIABLE */
      case 244: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==244);
#line 716 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
#line 3733 "sql.c"
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 245: /* expr ::= BOOL */
#line 718 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
#line 3739 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 246: /* expr ::= NULL */
#line 719 "sql.y"
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
#line 3745 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 247: /* expr ::= ID LP exprlist RP */
#line 722 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(yymsp[-1].minor.yy421, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3751 "sql.c"
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 248: /* expr ::= ID LP STAR RP */
#line 725 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3757 "sql.c"
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 249: /* expr ::= expr IS NULL */
#line 728 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, NULL, TK_ISNULL);}
#line 3763 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 250: /* expr ::= expr IS NOT NULL */
#line 729 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-3].minor.yy439, NULL, TK_NOTNULL);}
#line 3769 "sql.c"
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 251: /* expr ::= expr LT expr */
#line 732 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LT);}
#line 3775 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 252: /* expr ::= expr GT expr */
#line 733 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GT);}
#line 3781 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 253: /* expr ::= expr LE expr */
#line 734 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LE);}
#line 3787 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 254: /* expr ::= expr GE expr */
#line 735 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GE);}
#line 3793 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 255: /* expr ::= expr NE expr */
#line 736 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NE);}
#line 3799 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 256: /* expr ::= expr EQ expr */
#line 737 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_EQ);}
#line 3805 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 257: /* expr ::= expr BETWEEN expr AND expr */
#line 739 "sql.y"
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy439); yylhsminor.yy439 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy439, yymsp[-2].minor.yy439, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy439, TK_LE), TK_AND);}
#line 3811 "sql.c"
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 258: /* expr ::= expr AND expr */
#line 741 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_AND);}
#line 3817 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 259: /* expr ::= expr OR expr */
#line 742 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_OR); }
#line 3823 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 260: /* expr ::= expr PLUS expr */
#line 745 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_PLUS);  }
#line 3829 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 261: /* expr ::= expr MINUS expr */
#line 746 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MINUS); }
#line 3835 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 262: /* expr ::= expr STAR expr */
#line 747 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_STAR);  }
#line 3841 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 263: /* expr ::= expr SLASH expr */
#line 748 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_DIVIDE);}
#line 3847 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 264: /* expr ::= expr REM expr */
#line 749 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_REM);   }
#line 3853 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 265: /* expr ::= expr LIKE expr */
#line 752 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LIKE);  }
#line 3859 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 266: /* expr ::= expr MATCH expr */
#line 755 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MATCH);  }
#line 3865 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 267: /* expr ::= expr NMATCH expr */
#line 756 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NMATCH);  }
#line 3871 "sql.c"
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 268: /* expr ::= expr IN LP exprlist RP */
#line 759 "sql.y"
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-4].minor.yy439, (tSqlExpr*)yymsp[-1].minor.yy421, TK_IN); }
#line 3877 "sql.c"
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 269: /* exprlist ::= exprlist COMMA expritem */
#line 767 "sql.y"
{yylhsminor.yy421 = tSqlExprListAppend(yymsp[-2].minor.yy421,yymsp[0].minor.yy439,0, 0);}
#line 3883 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 270: /* exprlist ::= expritem */
#line 768 "sql.y"
{yylhsminor.yy421 = tSqlExprListAppend(0,yymsp[0].minor.yy439,0, 0);}
#line 3889 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 271: /* expritem ::= expr */
#line 769 "sql.y"
{yylhsminor.yy439 = yymsp[0].minor.yy439;}
#line 3895 "sql.c"
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 273: /* cmd ::= RESET QUERY CACHE */
#line 773 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
#line 3901 "sql.c"
        break;
      case 274: /* cmd ::= SYNCDB ids REPLICA */
#line 776 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
#line 3906 "sql.c"
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
#line 779 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3915 "sql.c"
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
#line 785 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3928 "sql.c"
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
#line 795 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3937 "sql.c"
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
#line 802 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3946 "sql.c"
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
#line 807 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3959 "sql.c"
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
#line 817 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3975 "sql.c"
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
#line 830 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3989 "sql.c"
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
#line 841 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3998 "sql.c"
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
#line 848 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4007 "sql.c"
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
#line 854 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4020 "sql.c"
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
#line 864 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4029 "sql.c"
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
#line 871 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4038 "sql.c"
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
#line 876 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4051 "sql.c"
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
#line 886 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4067 "sql.c"
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
#line 899 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4081 "sql.c"
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
#line 910 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4090 "sql.c"
        break;
      case 291: /* cmd ::= KILL CONNECTION INTEGER */
#line 917 "sql.y"
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
#line 4095 "sql.c"
        break;
      case 292: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
#line 918 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
#line 4100 "sql.c"
        break;
      case 293: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
#line 919 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
#line 4105 "sql.c"
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
#line 4190 "sql.c"
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
#line 4217 "sql.c"
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
