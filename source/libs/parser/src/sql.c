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
#include "astGenerator.h"
#include "tmsgtype.h"
#include "ttoken.h"
#include "ttokendef.h"
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
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy78;
  SCreateTableSql* yy110;
  int yy130;
  SArray* yy135;
  SIntervalVal yy160;
  SVariant yy191;
  SLimit yy247;
  SCreateDbInfo yy256;
  SWindowStateVal yy258;
  int32_t yy262;
  SCreateAcctInfo yy277;
  SField yy304;
  SRelationInfo* yy460;
  SSqlNode* yy488;
  SSessionWindowVal yy511;
  tSqlExpr* yy526;
  int64_t yy531;
  SSubclause* yy551;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             368
#define YYNRULE              295
#define YYNTOKEN             197
#define YY_MAX_SHIFT         367
#define YY_MIN_SHIFTREDUCE   577
#define YY_MAX_SHIFTREDUCE   871
#define YY_ERROR_ACTION      872
#define YY_ACCEPT_ACTION     873
#define YY_NO_ACTION         874
#define YY_MIN_REDUCE        875
#define YY_MAX_REDUCE        1169
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
#define YY_ACTTAB_COUNT (776)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    23,  629,  366,  236, 1054,  209,  242,  713,  212,  630,
 /*    10 */  1031,  873,  367,   59,   60,  174,   63,   64, 1044, 1145,
 /*    20 */   256,   53,   52,   51,  629,   62,  324,   67,   65,   68,
 /*    30 */    66,  158,  630,  286,  239,   58,   57,  344,  343,   56,
 /*    40 */    55,   54,   59,   60,  248,   63,   64,  253, 1031,  256,
 /*    50 */    53,   52,   51,  665,   62,  324,   67,   65,   68,   66,
 /*    60 */  1001, 1044,  999, 1000,   58,   57,  210, 1002,   56,   55,
 /*    70 */    54, 1003, 1051, 1004, 1005,   58,   57,  278,  216,   56,
 /*    80 */    55,   54,   59,   60,  165,   63,   64,   38,   83,  256,
 /*    90 */    53,   52,   51,   89,   62,  324,   67,   65,   68,   66,
 /*   100 */   284,  283,  250,  754,   58,   57, 1031,  212,   56,   55,
 /*   110 */    54,   38,   59,   61,  808,   63,   64, 1044, 1146,  256,
 /*   120 */    53,   52,   51,  629,   62,  324,   67,   65,   68,   66,
 /*   130 */    45,  630,  238,  240,   58,   57, 1028,  165,   56,   55,
 /*   140 */    54,   60, 1025,   63,   64,  773,  774,  256,   53,   52,
 /*   150 */    51,   96,   62,  324,   67,   65,   68,   66,  165, 1093,
 /*   160 */  1027,  296,   58,   57,   89,   84,   56,   55,   54,  578,
 /*   170 */   579,  580,  581,  582,  583,  584,  585,  586,  587,  588,
 /*   180 */   589,  590,  591,  156,  322,  237,   63,   64,  758,  249,
 /*   190 */   256,   53,   52,   51,  629,   62,  324,   67,   65,   68,
 /*   200 */    66,   45,  630,   88, 1017,   58,   57,  362,  962,   56,
 /*   210 */    55,   54, 1092,   44,  320,  361,  360,  319,  318,  317,
 /*   220 */   359,  316,  315,  314,  358,  313,  357,  356,  155,  153,
 /*   230 */   152,  298,   24,   94,  993,  981,  982,  983,  984,  985,
 /*   240 */   986,  987,  988,  989,  990,  991,  992,  994,  995,  215,
 /*   250 */   751,  255,  823,  924,  126,  812,  223,  815,  354,  818,
 /*   260 */   193,   98,  140,  139,  138,  222,  354,  255,  823,  329,
 /*   270 */    89,  812,  252,  815,  270,  818,    9,   29,  217,   67,
 /*   280 */    65,   68,   66,  274,  273,  234,  235,   58,   57,  325,
 /*   290 */   322,   56,   55,   54, 1014, 1015,   35, 1018,  814,  218,
 /*   300 */   817,  234,  235,  259,    5,   41,  183,   45,   56,   55,
 /*   310 */    54,  182,  107,  112,  103,  111,   38,  264,  737,   38,
 /*   320 */   934,  734,  742,  735,  743,  736,  165,  193,  257,  277,
 /*   330 */   309,   81,  212,   38,   69,  124,  118,  129,  230,  813,
 /*   340 */  1140,  816,  128, 1146,  134,  137,  127,  203,  201,  199,
 /*   350 */    69,  261,  262,  131,  198,  144,  143,  142,  141,   38,
 /*   360 */    44,  246,  361,  360,  247, 1028,  101,  359, 1028,  824,
 /*   370 */   819,  358,   38,  357,  356,   38,  820,   38,  333,  260,
 /*   380 */    38,  258, 1028,  332,  331,  824,  819,  790,  212,  365,
 /*   390 */   364,  149,  820,  266,   38,  263,   38,  339,  338, 1146,
 /*   400 */   265,   95, 1019,   14,  334,  265,   82,   97, 1028,   86,
 /*   410 */    77,  179,  265,    3,  194,   87,  180,  335,  326,  821,
 /*   420 */   336, 1028,  340, 1029, 1028,  341, 1028,  925,  279, 1028,
 /*   430 */    74,    1,  181,  770,  193,  738,  739,  100,   34,  342,
 /*   440 */  1016,  346,   39, 1028,  789, 1028,   73,  160,  780,  781,
 /*   450 */    78,  723,   73,  301,  725,  303,  724,  810,  254,  846,
 /*   460 */   822,  825,   70,   26,  628,   39,   80,   39,   70,   99,
 /*   470 */    70,  304,   75,   25,   16,   25,   15, 1139,   25,  117,
 /*   480 */     6,  116,   18, 1138,   17,  740,   20,  741,   19,  123,
 /*   490 */   232,  122,   22,  233,   21,  811,  136,  135,  712,  213,
 /*   500 */   275,  214,  219,  211,  220,  221,  225, 1030,  226,  227,
 /*   510 */   224,  208, 1165, 1157, 1103, 1046, 1102,  244, 1099, 1098,
 /*   520 */   245,  345,  827,  157,   48, 1053, 1064, 1085, 1061, 1062,
 /*   530 */  1045, 1084,  281, 1066, 1026,  154,  159,  164,  292,  175,
 /*   540 */   176, 1024,   33,  285,  177,  168,  178,  939,  769,  306,
 /*   550 */   307,  308,  311,  312, 1042,   46,  206,   42,  323,  933,
 /*   560 */   330, 1164,  114,  241,   79, 1163,  287, 1160,  289,   76,
 /*   570 */   166,  299,  184,  337,  167, 1156,   50,  120, 1155,  297,
 /*   580 */  1152,  185,  295,  959,   43,   40,   47,  207,  921,  130,
 /*   590 */   919,  132,  133,  917,  916,  267,  293,  196,  197,  913,
 /*   600 */   912,  911,  910,  291,  909,  908,  907,  200,  202,  904,
 /*   610 */   902,  900,  898,  204,  895,  205,  288,  891,   49,  310,
 /*   620 */   280,   85,   90,  290,  355, 1086,  348,  125,  347,  349,
 /*   630 */   350,  351,  231,  352,  251,  305,  353,  363,  871,  269,
 /*   640 */   870,  268,  271,  228,  229,  108,  938,  937,  109,  272,
 /*   650 */   869,  852,  276,  851,   73,   10,  915,  300,  282,  914,
 /*   660 */   745,  145,  188,  146,  187,  960,  186,  189,  190,  192,
 /*   670 */   191,  906,  905,  961,  147,  997,    2,   30,  148,  897,
 /*   680 */   896,   91,  171,  169,  172,  170,  173, 1007,  771,    4,
 /*   690 */   161,  163,  782,  162,  243,  776,   92,   31,  778,   93,
 /*   700 */   294,   11,   12,   32,   13,   27,  302,   28,  100,  102,
 /*   710 */   105,   36,  104,  643,   37,  106,  678,  676,  675,  674,
 /*   720 */   672,  671,  670,  667,  633,  321,  110,    7,  327,  328,
 /*   730 */   828,   39,  826,    8,  113,   71,  115,   72,  715,  714,
 /*   740 */   119,  711,  659,  121,  657,  649,  655,  651,  653,  647,
 /*   750 */   645,  681,  680,  679,  677,  673,  669,  668,  195,  631,
 /*   760 */   595,  875,  874,  874,  874,  874,  874,  874,  874,  874,
 /*   770 */   874,  874,  874,  874,  150,  151,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   267,    1,  200,  201,  200,  267,  246,    5,  267,    9,
 /*    10 */   250,  198,  199,   13,   14,  254,   16,   17,  248,  278,
 /*    20 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*    30 */    30,  200,    9,  272,  264,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  246,   16,   17,  207,  250,   20,
 /*    50 */    21,   22,   23,    5,   25,   26,   27,   28,   29,   30,
 /*    60 */   224,  248,  226,  227,   35,   36,  267,  231,   39,   40,
 /*    70 */    41,  235,  268,  237,  238,   35,   36,  264,  267,   39,
 /*    80 */    40,   41,   13,   14,  200,   16,   17,  200,   88,   20,
 /*    90 */    21,   22,   23,   84,   25,   26,   27,   28,   29,   30,
 /*   100 */   269,  270,  246,   39,   35,   36,  250,  267,   39,   40,
 /*   110 */    41,  200,   13,   14,   85,   16,   17,  248,  278,   20,
 /*   120 */    21,   22,   23,    1,   25,   26,   27,   28,   29,   30,
 /*   130 */   121,    9,  245,  264,   35,   36,  249,  200,   39,   40,
 /*   140 */    41,   14,  200,   16,   17,  127,  128,   20,   21,   22,
 /*   150 */    23,  251,   25,   26,   27,   28,   29,   30,  200,  275,
 /*   160 */   249,  277,   35,   36,   84,  265,   39,   40,   41,   47,
 /*   170 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   180 */    58,   59,   60,   61,   86,   63,   16,   17,  124,  247,
 /*   190 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*   200 */    30,  121,    9,  123,    0,   35,   36,  222,  223,   39,
 /*   210 */    40,   41,  275,  100,  101,  102,  103,  104,  105,  106,
 /*   220 */   107,  108,  109,  110,  111,  112,  113,  114,   64,   65,
 /*   230 */    66,  273,   46,  275,  224,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,  237,  238,   63,
 /*   250 */    99,    1,    2,  206,   80,    5,   70,    7,   92,    9,
 /*   260 */   213,  208,   76,   77,   78,   79,   92,    1,    2,   83,
 /*   270 */    84,    5,  207,    7,  144,    9,  125,   84,  267,   27,
 /*   280 */    28,   29,   30,  153,  154,   35,   36,   35,   36,   39,
 /*   290 */    86,   39,   40,   41,  241,  242,  243,  244,    5,  267,
 /*   300 */     7,   35,   36,   70,   64,   65,   66,  121,   39,   40,
 /*   310 */    41,   71,   72,   73,   74,   75,  200,   70,    2,  200,
 /*   320 */   206,    5,    5,    7,    7,    9,  200,  213,  207,  143,
 /*   330 */    90,  145,  267,  200,   84,   64,   65,   66,  152,    5,
 /*   340 */   267,    7,   71,  278,   73,   74,   75,   64,   65,   66,
 /*   350 */    84,   35,   36,   82,   71,   72,   73,   74,   75,  200,
 /*   360 */   100,  245,  102,  103,  245,  249,  208,  107,  249,  119,
 /*   370 */   120,  111,  200,  113,  114,  200,  126,  200,  245,  146,
 /*   380 */   200,  148,  249,  150,  151,  119,  120,   78,  267,   67,
 /*   390 */    68,   69,  126,  146,  200,  148,  200,  150,  151,  278,
 /*   400 */   200,  275,  244,   84,  245,  200,  208,   88,  249,   85,
 /*   410 */    99,  211,  200,  204,  205,   85,  211,  245,   15,  126,
 /*   420 */   245,  249,  245,  211,  249,  245,  249,  206,   85,  249,
 /*   430 */    99,  209,  210,   85,  213,  119,  120,  118,   84,  245,
 /*   440 */   242,  245,   99,  249,  135,  249,  122,   99,   85,   85,
 /*   450 */   139,   85,  122,   85,   85,   85,   85,    1,   62,   85,
 /*   460 */   126,   85,   99,   99,   85,   99,   84,   99,   99,   99,
 /*   470 */    99,  117,  141,   99,  147,   99,  149,  267,   99,  147,
 /*   480 */    84,  149,  147,  267,  149,    5,  147,    7,  149,  147,
 /*   490 */   267,  149,  147,  267,  149,   39,   80,   81,  116,  267,
 /*   500 */   200,  267,  267,  267,  267,  267,  267,  250,  267,  267,
 /*   510 */   267,  267,  250,  250,  240,  248,  240,  240,  240,  240,
 /*   520 */   240,  240,  119,  200,  266,  200,  200,  276,  200,  200,
 /*   530 */   248,  276,  248,  200,  248,   62,  200,  200,  200,  252,
 /*   540 */   200,  200,  253,  271,  200,  260,  200,  200,  126,  200,
 /*   550 */   200,  200,  200,  200,  263,  200,  200,  200,  200,  200,
 /*   560 */   200,  200,  200,  271,  138,  200,  271,  200,  271,  140,
 /*   570 */   262,  133,  200,  200,  261,  200,  137,  200,  200,  136,
 /*   580 */   200,  200,  131,  200,  200,  200,  200,  200,  200,  200,
 /*   590 */   200,  200,  200,  200,  200,  200,  130,  200,  200,  200,
 /*   600 */   200,  200,  200,  129,  200,  200,  200,  200,  200,  200,
 /*   610 */   200,  200,  200,  200,  200,  200,  132,  200,  142,   91,
 /*   620 */   202,  202,  202,  202,  115,  202,   53,   98,   97,   94,
 /*   630 */    96,   57,  202,   95,  202,  202,   93,   86,    5,    5,
 /*   640 */     5,  155,  155,  202,  202,  208,  212,  212,  208,    5,
 /*   650 */     5,  102,  144,  101,  122,   84,  202,  117,   99,  202,
 /*   660 */    85,  203,  215,  203,  219,  221,  220,  218,  216,  214,
 /*   670 */   217,  202,  202,  223,  203,  239,  209,   84,  203,  202,
 /*   680 */   202,   99,  257,  259,  256,  258,  255,  239,   85,  204,
 /*   690 */    84,   99,   85,   84,    1,   85,   84,   99,   85,   84,
 /*   700 */    84,  134,  134,   99,   84,   84,  117,   84,  118,   80,
 /*   710 */    72,   89,   88,    5,   89,   88,    9,    5,    5,    5,
 /*   720 */     5,    5,    5,    5,   87,   15,   80,   84,   26,   61,
 /*   730 */   119,   99,   85,   84,  149,   16,  149,   16,    5,    5,
 /*   740 */   149,   85,    5,  149,    5,    5,    5,    5,    5,    5,
 /*   750 */     5,    5,    5,    5,    5,    5,    5,    5,   99,   87,
 /*   760 */    62,    0,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   770 */   279,  279,  279,  279,   21,   21,  279,  279,  279,  279,
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
 /*   950 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   960 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   970 */   279,  279,  279,
};
#define YY_SHIFT_COUNT    (367)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (761)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   186,  113,  113,  260,  260,   98,  250,  266,  266,  193,
 /*    10 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*    20 */    23,   23,   23,    0,  122,  266,  316,  316,  316,    9,
 /*    30 */     9,   23,   23,   18,   23,  204,   23,   23,   23,   23,
 /*    40 */   174,   98,  166,  166,   48,  776,  776,  776,  266,  266,
 /*    50 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*    60 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*    70 */   316,  316,  316,   80,    2,    2,    2,    2,    2,    2,
 /*    80 */     2,   23,   23,   23,   64,   23,   23,   23,    9,    9,
 /*    90 */    23,   23,   23,   23,  309,  309,  151,    9,   23,   23,
 /*   100 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   110 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   120 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   130 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   140 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   150 */    23,   23,   23,   23,   23,   23,   23,  473,  473,  473,
 /*   160 */   422,  422,  422,  422,  473,  473,  426,  429,  438,  439,
 /*   170 */   443,  451,  466,  474,  484,  476,  473,  473,  473,  528,
 /*   180 */   528,  509,   98,   98,  473,  473,  529,  531,  573,  535,
 /*   190 */   534,  574,  538,  543,  509,   48,  473,  473,  551,  551,
 /*   200 */   473,  551,  473,  551,  473,  473,  776,  776,   29,   69,
 /*   210 */    69,   99,   69,  127,  170,  240,  252,  252,  252,  252,
 /*   220 */   252,  252,  271,  283,   40,   40,   40,   40,  233,  247,
 /*   230 */   130,  319,  269,  269,  293,  334,  322,  164,  343,  324,
 /*   240 */   330,  348,  363,  364,  331,  311,  366,  368,  369,  370,
 /*   250 */   371,  354,  374,  376,  456,  396,  403,  379,  327,  332,
 /*   260 */   335,  317,  480,  339,  342,  382,  345,  416,  633,  486,
 /*   270 */   634,  635,  487,  644,  645,  549,  552,  508,  532,  540,
 /*   280 */   571,  575,  593,  559,  582,  603,  606,  607,  609,  610,
 /*   290 */   592,  612,  613,  615,  693,  616,  598,  567,  604,  568,
 /*   300 */   620,  540,  621,  589,  623,  590,  629,  622,  624,  638,
 /*   310 */   708,  625,  627,  707,  712,  713,  714,  715,  716,  717,
 /*   320 */   718,  637,  710,  646,  643,  647,  611,  649,  702,  668,
 /*   330 */   719,  585,  587,  632,  632,  632,  632,  721,  591,  594,
 /*   340 */   632,  632,  632,  733,  734,  656,  632,  737,  739,  740,
 /*   350 */   741,  742,  743,  744,  745,  746,  747,  748,  749,  750,
 /*   360 */   751,  752,  659,  672,  753,  754,  698,  761,
};
#define YY_REDUCE_COUNT (207)
#define YY_REDUCE_MIN   (-267)
#define YY_REDUCE_MAX   (485)
static const short yy_reduce_ofst[] = {
 /*     0 */  -187,   10,   10, -164, -164,   53, -160,   65,  121, -169,
 /*    10 */  -113, -116,  -42,  116,  119,  133,  159,  172,  175,  177,
 /*    20 */   180,  194,  196, -196, -198, -259, -240, -202, -144, -230,
 /*    30 */  -131,  -63,  126, -239,  -58,  158,  200,  205,  212,  -89,
 /*    40 */    47,  198,  114,  221,  -15, -100,  222,  209, -267, -262,
 /*    50 */  -201, -189,   11,   32,   73,  210,  216,  223,  226,  232,
 /*    60 */   234,  235,  236,  237,  238,  239,  241,  242,  243,  244,
 /*    70 */   257,  262,  263,  267,  274,  276,  277,  278,  279,  280,
 /*    80 */   281,  300,  323,  325,  258,  326,  328,  329,  282,  284,
 /*    90 */   333,  336,  337,  338,  251,  255,  287,  286,  340,  341,
 /*   100 */   344,  346,  347,  349,  350,  351,  352,  353,  355,  356,
 /*   110 */   357,  358,  359,  360,  361,  362,  365,  367,  372,  373,
 /*   120 */   375,  377,  378,  380,  381,  383,  384,  385,  386,  387,
 /*   130 */   388,  389,  390,  391,  392,  393,  394,  395,  397,  398,
 /*   140 */   399,  400,  401,  402,  404,  405,  406,  407,  408,  409,
 /*   150 */   410,  411,  412,  413,  414,  415,  417,  418,  419,  420,
 /*   160 */   272,  292,  295,  297,  421,  423,  291,  308,  313,  285,
 /*   170 */   424,  427,  425,  428,  431,  289,  430,  432,  433,  434,
 /*   180 */   435,  436,  437,  440,  441,  442,  444,  446,  445,  447,
 /*   190 */   449,  452,  453,  455,  448,  450,  454,  457,  458,  460,
 /*   200 */   469,  471,  470,  475,  477,  478,  467,  485,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   872,  996,  935, 1006,  922,  932, 1148, 1148, 1148,  872,
 /*    10 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*    20 */   872,  872,  872, 1055,  892, 1148,  872,  872,  872,  872,
 /*    30 */   872,  872,  872, 1070,  872,  932,  872,  872,  872,  872,
 /*    40 */   942,  932,  942,  942,  872, 1050,  980,  998,  872,  872,
 /*    50 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*    60 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*    70 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*    80 */   872,  872,  872,  872, 1057, 1063, 1060,  872,  872,  872,
 /*    90 */  1065,  872,  872,  872, 1089, 1089, 1048,  872,  872,  872,
 /*   100 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   110 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   120 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   130 */   920,  872,  918,  872,  872,  872,  872,  872,  872,  872,
 /*   140 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  903,
 /*   150 */   872,  872,  872,  872,  872,  872,  890,  894,  894,  894,
 /*   160 */   872,  872,  872,  872,  894,  894, 1096, 1100, 1082, 1094,
 /*   170 */  1090, 1077, 1075, 1073, 1081, 1104,  894,  894,  894,  940,
 /*   180 */   940,  936,  932,  932,  894,  894,  958,  956,  954,  946,
 /*   190 */   952,  948,  950,  944,  923,  872,  894,  894,  930,  930,
 /*   200 */   894,  930,  894,  930,  894,  894,  980,  998,  872, 1105,
 /*   210 */  1095,  872, 1147, 1135, 1134,  872, 1143, 1142, 1141, 1133,
 /*   220 */  1132, 1131,  872,  872, 1127, 1130, 1129, 1128,  872,  872,
 /*   230 */   872,  872, 1137, 1136,  872,  872,  872,  872,  872,  872,
 /*   240 */   872,  872,  872,  872, 1101, 1097,  872,  872,  872,  872,
 /*   250 */   872,  872,  872,  872,  872, 1107,  872,  872,  872,  872,
 /*   260 */   872,  872,  872,  872,  872, 1008,  872,  872,  872,  872,
 /*   270 */   872,  872,  872,  872,  872,  872,  872,  872, 1047,  872,
 /*   280 */   872,  872,  872, 1059, 1058,  872,  872,  872,  872,  872,
 /*   290 */   872,  872,  872,  872,  872,  872, 1091,  872, 1083,  872,
 /*   300 */   872, 1020,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   310 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   320 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   330 */   872,  872,  872, 1166, 1161, 1162, 1159,  872,  872,  872,
 /*   340 */  1158, 1153, 1154,  872,  872,  872, 1151,  872,  872,  872,
 /*   350 */   872,  872,  872,  872,  872,  872,  872,  872,  872,  872,
 /*   360 */   872,  872,  964,  872,  901,  899,  872,  872,
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
  /*  197 */ "error",
  /*  198 */ "program",
  /*  199 */ "cmd",
  /*  200 */ "ids",
  /*  201 */ "dbPrefix",
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
 /* 171 */ "union ::= union UNION select",
 /* 172 */ "cmd ::= union",
 /* 173 */ "select ::= SELECT selcollist",
 /* 174 */ "sclp ::= selcollist COMMA",
 /* 175 */ "sclp ::=",
 /* 176 */ "selcollist ::= sclp distinct expr as",
 /* 177 */ "selcollist ::= sclp STAR",
 /* 178 */ "as ::= AS ids",
 /* 179 */ "as ::= ids",
 /* 180 */ "as ::=",
 /* 181 */ "distinct ::= DISTINCT",
 /* 182 */ "distinct ::=",
 /* 183 */ "from ::= FROM tablelist",
 /* 184 */ "from ::= FROM sub",
 /* 185 */ "sub ::= LP union RP",
 /* 186 */ "sub ::= LP union RP ids",
 /* 187 */ "sub ::= sub COMMA LP union RP ids",
 /* 188 */ "tablelist ::= ids cpxName",
 /* 189 */ "tablelist ::= ids cpxName ids",
 /* 190 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 191 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 192 */ "tmvar ::= VARIABLE",
 /* 193 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 194 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 195 */ "interval_option ::=",
 /* 196 */ "intervalKey ::= INTERVAL",
 /* 197 */ "intervalKey ::= EVERY",
 /* 198 */ "session_option ::=",
 /* 199 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 200 */ "windowstate_option ::=",
 /* 201 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 202 */ "fill_opt ::=",
 /* 203 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 204 */ "fill_opt ::= FILL LP ID RP",
 /* 205 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 206 */ "sliding_opt ::=",
 /* 207 */ "orderby_opt ::=",
 /* 208 */ "orderby_opt ::= ORDER BY sortlist",
 /* 209 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 210 */ "sortlist ::= item sortorder",
 /* 211 */ "item ::= ids cpxName",
 /* 212 */ "sortorder ::= ASC",
 /* 213 */ "sortorder ::= DESC",
 /* 214 */ "sortorder ::=",
 /* 215 */ "groupby_opt ::=",
 /* 216 */ "groupby_opt ::= GROUP BY grouplist",
 /* 217 */ "grouplist ::= grouplist COMMA item",
 /* 218 */ "grouplist ::= item",
 /* 219 */ "having_opt ::=",
 /* 220 */ "having_opt ::= HAVING expr",
 /* 221 */ "limit_opt ::=",
 /* 222 */ "limit_opt ::= LIMIT signed",
 /* 223 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 224 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 225 */ "slimit_opt ::=",
 /* 226 */ "slimit_opt ::= SLIMIT signed",
 /* 227 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 228 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 229 */ "where_opt ::=",
 /* 230 */ "where_opt ::= WHERE expr",
 /* 231 */ "expr ::= LP expr RP",
 /* 232 */ "expr ::= ID",
 /* 233 */ "expr ::= ID DOT ID",
 /* 234 */ "expr ::= ID DOT STAR",
 /* 235 */ "expr ::= INTEGER",
 /* 236 */ "expr ::= MINUS INTEGER",
 /* 237 */ "expr ::= PLUS INTEGER",
 /* 238 */ "expr ::= FLOAT",
 /* 239 */ "expr ::= MINUS FLOAT",
 /* 240 */ "expr ::= PLUS FLOAT",
 /* 241 */ "expr ::= STRING",
 /* 242 */ "expr ::= NOW",
 /* 243 */ "expr ::= VARIABLE",
 /* 244 */ "expr ::= PLUS VARIABLE",
 /* 245 */ "expr ::= MINUS VARIABLE",
 /* 246 */ "expr ::= BOOL",
 /* 247 */ "expr ::= NULL",
 /* 248 */ "expr ::= ID LP exprlist RP",
 /* 249 */ "expr ::= ID LP STAR RP",
 /* 250 */ "expr ::= expr IS NULL",
 /* 251 */ "expr ::= expr IS NOT NULL",
 /* 252 */ "expr ::= expr LT expr",
 /* 253 */ "expr ::= expr GT expr",
 /* 254 */ "expr ::= expr LE expr",
 /* 255 */ "expr ::= expr GE expr",
 /* 256 */ "expr ::= expr NE expr",
 /* 257 */ "expr ::= expr EQ expr",
 /* 258 */ "expr ::= expr BETWEEN expr AND expr",
 /* 259 */ "expr ::= expr AND expr",
 /* 260 */ "expr ::= expr OR expr",
 /* 261 */ "expr ::= expr PLUS expr",
 /* 262 */ "expr ::= expr MINUS expr",
 /* 263 */ "expr ::= expr STAR expr",
 /* 264 */ "expr ::= expr SLASH expr",
 /* 265 */ "expr ::= expr REM expr",
 /* 266 */ "expr ::= expr LIKE expr",
 /* 267 */ "expr ::= expr MATCH expr",
 /* 268 */ "expr ::= expr NMATCH expr",
 /* 269 */ "expr ::= expr IN LP exprlist RP",
 /* 270 */ "exprlist ::= exprlist COMMA expritem",
 /* 271 */ "exprlist ::= expritem",
 /* 272 */ "expritem ::= expr",
 /* 273 */ "expritem ::=",
 /* 274 */ "cmd ::= RESET QUERY CACHE",
 /* 275 */ "cmd ::= SYNCDB ids REPLICA",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 292 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 293 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 294 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
destroyAllSqlNode((yypminor->yy551));
}
      break;
    case 274: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy191));
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
  {  199,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  201,    0 }, /* (17) dbPrefix ::= */
  {  201,   -2 }, /* (18) dbPrefix ::= ids DOT */
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
  {  200,   -1 }, /* (52) ids ::= ID */
  {  200,   -1 }, /* (53) ids ::= STRING */
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
  {  264,   -3 }, /* (171) union ::= union UNION select */
  {  199,   -1 }, /* (172) cmd ::= union */
  {  248,   -2 }, /* (173) select ::= SELECT selcollist */
  {  265,   -2 }, /* (174) sclp ::= selcollist COMMA */
  {  265,    0 }, /* (175) sclp ::= */
  {  251,   -4 }, /* (176) selcollist ::= sclp distinct expr as */
  {  251,   -2 }, /* (177) selcollist ::= sclp STAR */
  {  268,   -2 }, /* (178) as ::= AS ids */
  {  268,   -1 }, /* (179) as ::= ids */
  {  268,    0 }, /* (180) as ::= */
  {  266,   -1 }, /* (181) distinct ::= DISTINCT */
  {  266,    0 }, /* (182) distinct ::= */
  {  252,   -2 }, /* (183) from ::= FROM tablelist */
  {  252,   -2 }, /* (184) from ::= FROM sub */
  {  270,   -3 }, /* (185) sub ::= LP union RP */
  {  270,   -4 }, /* (186) sub ::= LP union RP ids */
  {  270,   -6 }, /* (187) sub ::= sub COMMA LP union RP ids */
  {  269,   -2 }, /* (188) tablelist ::= ids cpxName */
  {  269,   -3 }, /* (189) tablelist ::= ids cpxName ids */
  {  269,   -4 }, /* (190) tablelist ::= tablelist COMMA ids cpxName */
  {  269,   -5 }, /* (191) tablelist ::= tablelist COMMA ids cpxName ids */
  {  271,   -1 }, /* (192) tmvar ::= VARIABLE */
  {  254,   -4 }, /* (193) interval_option ::= intervalKey LP tmvar RP */
  {  254,   -6 }, /* (194) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  254,    0 }, /* (195) interval_option ::= */
  {  272,   -1 }, /* (196) intervalKey ::= INTERVAL */
  {  272,   -1 }, /* (197) intervalKey ::= EVERY */
  {  256,    0 }, /* (198) session_option ::= */
  {  256,   -7 }, /* (199) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  257,    0 }, /* (200) windowstate_option ::= */
  {  257,   -4 }, /* (201) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  258,    0 }, /* (202) fill_opt ::= */
  {  258,   -6 }, /* (203) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  258,   -4 }, /* (204) fill_opt ::= FILL LP ID RP */
  {  255,   -4 }, /* (205) sliding_opt ::= SLIDING LP tmvar RP */
  {  255,    0 }, /* (206) sliding_opt ::= */
  {  261,    0 }, /* (207) orderby_opt ::= */
  {  261,   -3 }, /* (208) orderby_opt ::= ORDER BY sortlist */
  {  273,   -4 }, /* (209) sortlist ::= sortlist COMMA item sortorder */
  {  273,   -2 }, /* (210) sortlist ::= item sortorder */
  {  275,   -2 }, /* (211) item ::= ids cpxName */
  {  276,   -1 }, /* (212) sortorder ::= ASC */
  {  276,   -1 }, /* (213) sortorder ::= DESC */
  {  276,    0 }, /* (214) sortorder ::= */
  {  259,    0 }, /* (215) groupby_opt ::= */
  {  259,   -3 }, /* (216) groupby_opt ::= GROUP BY grouplist */
  {  277,   -3 }, /* (217) grouplist ::= grouplist COMMA item */
  {  277,   -1 }, /* (218) grouplist ::= item */
  {  260,    0 }, /* (219) having_opt ::= */
  {  260,   -2 }, /* (220) having_opt ::= HAVING expr */
  {  263,    0 }, /* (221) limit_opt ::= */
  {  263,   -2 }, /* (222) limit_opt ::= LIMIT signed */
  {  263,   -4 }, /* (223) limit_opt ::= LIMIT signed OFFSET signed */
  {  263,   -4 }, /* (224) limit_opt ::= LIMIT signed COMMA signed */
  {  262,    0 }, /* (225) slimit_opt ::= */
  {  262,   -2 }, /* (226) slimit_opt ::= SLIMIT signed */
  {  262,   -4 }, /* (227) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  262,   -4 }, /* (228) slimit_opt ::= SLIMIT signed COMMA signed */
  {  253,    0 }, /* (229) where_opt ::= */
  {  253,   -2 }, /* (230) where_opt ::= WHERE expr */
  {  267,   -3 }, /* (231) expr ::= LP expr RP */
  {  267,   -1 }, /* (232) expr ::= ID */
  {  267,   -3 }, /* (233) expr ::= ID DOT ID */
  {  267,   -3 }, /* (234) expr ::= ID DOT STAR */
  {  267,   -1 }, /* (235) expr ::= INTEGER */
  {  267,   -2 }, /* (236) expr ::= MINUS INTEGER */
  {  267,   -2 }, /* (237) expr ::= PLUS INTEGER */
  {  267,   -1 }, /* (238) expr ::= FLOAT */
  {  267,   -2 }, /* (239) expr ::= MINUS FLOAT */
  {  267,   -2 }, /* (240) expr ::= PLUS FLOAT */
  {  267,   -1 }, /* (241) expr ::= STRING */
  {  267,   -1 }, /* (242) expr ::= NOW */
  {  267,   -1 }, /* (243) expr ::= VARIABLE */
  {  267,   -2 }, /* (244) expr ::= PLUS VARIABLE */
  {  267,   -2 }, /* (245) expr ::= MINUS VARIABLE */
  {  267,   -1 }, /* (246) expr ::= BOOL */
  {  267,   -1 }, /* (247) expr ::= NULL */
  {  267,   -4 }, /* (248) expr ::= ID LP exprlist RP */
  {  267,   -4 }, /* (249) expr ::= ID LP STAR RP */
  {  267,   -3 }, /* (250) expr ::= expr IS NULL */
  {  267,   -4 }, /* (251) expr ::= expr IS NOT NULL */
  {  267,   -3 }, /* (252) expr ::= expr LT expr */
  {  267,   -3 }, /* (253) expr ::= expr GT expr */
  {  267,   -3 }, /* (254) expr ::= expr LE expr */
  {  267,   -3 }, /* (255) expr ::= expr GE expr */
  {  267,   -3 }, /* (256) expr ::= expr NE expr */
  {  267,   -3 }, /* (257) expr ::= expr EQ expr */
  {  267,   -5 }, /* (258) expr ::= expr BETWEEN expr AND expr */
  {  267,   -3 }, /* (259) expr ::= expr AND expr */
  {  267,   -3 }, /* (260) expr ::= expr OR expr */
  {  267,   -3 }, /* (261) expr ::= expr PLUS expr */
  {  267,   -3 }, /* (262) expr ::= expr MINUS expr */
  {  267,   -3 }, /* (263) expr ::= expr STAR expr */
  {  267,   -3 }, /* (264) expr ::= expr SLASH expr */
  {  267,   -3 }, /* (265) expr ::= expr REM expr */
  {  267,   -3 }, /* (266) expr ::= expr LIKE expr */
  {  267,   -3 }, /* (267) expr ::= expr MATCH expr */
  {  267,   -3 }, /* (268) expr ::= expr NMATCH expr */
  {  267,   -5 }, /* (269) expr ::= expr IN LP exprlist RP */
  {  207,   -3 }, /* (270) exprlist ::= exprlist COMMA expritem */
  {  207,   -1 }, /* (271) exprlist ::= expritem */
  {  278,   -1 }, /* (272) expritem ::= expr */
  {  278,    0 }, /* (273) expritem ::= */
  {  199,   -3 }, /* (274) cmd ::= RESET QUERY CACHE */
  {  199,   -3 }, /* (275) cmd ::= SYNCDB ids REPLICA */
  {  199,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (278) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (279) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (281) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (282) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (286) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (287) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (289) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (290) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -3 }, /* (292) cmd ::= KILL CONNECTION INTEGER */
  {  199,   -5 }, /* (293) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  199,   -5 }, /* (294) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 16: /* cmd ::= SHOW VNODES ids */
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
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SToken token;
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
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy256, &t);}
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
      case 182: /* distinct ::= */ yytestcase(yyruleno==182);
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
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy304, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy304, &yymsp[0].minor.yy0, 2);}
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
{ yylhsminor.yy135 = tListItemAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy191, -1);    }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
{ yylhsminor.yy135 = tListItemAppend(NULL, &yymsp[0].minor.yy191, -1); }
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy191, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy191 = yylhsminor.yy191;
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
  tSetColumnType (&yylhsminor.yy304, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy304 = yylhsminor.yy304;
        break;
      case 134: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy531 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy304, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy531;  // negative value of name length
    tSetColumnType(&yylhsminor.yy304, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy304 = yylhsminor.yy304;
        break;
      case 135: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy304, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy304 = yylhsminor.yy304;
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
  pCreateTable->type = TSQL_CREATE_CTABLE;
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
{yylhsminor.yy135 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy135, &yymsp[0].minor.yy0);}
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
{taosArrayPush(yymsp[-2].minor.yy135, &yymsp[0].minor.yy304); yylhsminor.yy135 = yymsp[-2].minor.yy135;  }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 153: /* columnlist ::= column */
{yylhsminor.yy135 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy135, &yymsp[0].minor.yy304);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 154: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy304, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy304);
}
  yymsp[-1].minor.yy304 = yylhsminor.yy304;
        break;
      case 161: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy191, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy191 = yylhsminor.yy191;
        break;
      case 162: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy191, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy191 = yylhsminor.yy191;
        break;
      case 163: /* tagitem ::= MINUS INTEGER */
      case 164: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==166);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy191, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy191 = yylhsminor.yy191;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy488 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy135, yymsp[-11].minor.yy460, yymsp[-10].minor.yy526, yymsp[-4].minor.yy135, yymsp[-2].minor.yy135, &yymsp[-9].minor.yy160, &yymsp[-7].minor.yy511, &yymsp[-6].minor.yy258, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy135, &yymsp[0].minor.yy247, &yymsp[-1].minor.yy247, yymsp[-3].minor.yy526);
}
  yymsp[-13].minor.yy488 = yylhsminor.yy488;
        break;
      case 168: /* select ::= LP select RP */
{yymsp[-2].minor.yy488 = yymsp[-1].minor.yy488;}
        break;
      case 169: /* union ::= select */
{ yylhsminor.yy551 = setSubclause(NULL, yymsp[0].minor.yy488); }
  yymsp[0].minor.yy551 = yylhsminor.yy551;
        break;
      case 170: /* union ::= union UNION ALL select */
{ yylhsminor.yy551 = appendSelectClause(yymsp[-3].minor.yy551, SQL_TYPE_UNIONALL, yymsp[0].minor.yy488);  }
  yymsp[-3].minor.yy551 = yylhsminor.yy551;
        break;
      case 171: /* union ::= union UNION select */
{ yylhsminor.yy551 = appendSelectClause(yymsp[-2].minor.yy551, SQL_TYPE_UNION, yymsp[0].minor.yy488);  }
  yymsp[-2].minor.yy551 = yylhsminor.yy551;
        break;
      case 172: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy551, NULL, TSDB_SQL_SELECT); }
        break;
      case 173: /* select ::= SELECT selcollist */
{
  yylhsminor.yy488 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy135, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 174: /* sclp ::= selcollist COMMA */
{yylhsminor.yy135 = yymsp[-1].minor.yy135;}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 175: /* sclp ::= */
      case 207: /* orderby_opt ::= */ yytestcase(yyruleno==207);
{yymsp[1].minor.yy135 = 0;}
        break;
      case 176: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy135 = tSqlExprListAppend(yymsp[-3].minor.yy135, yymsp[-1].minor.yy526,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 177: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy135 = tSqlExprListAppend(yymsp[-1].minor.yy135, pNode, 0, 0);
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 178: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 179: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 180: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 181: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 183: /* from ::= FROM tablelist */
      case 184: /* from ::= FROM sub */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy460 = yymsp[0].minor.yy460;}
        break;
      case 185: /* sub ::= LP union RP */
{yymsp[-2].minor.yy460 = addSubquery(NULL, yymsp[-1].minor.yy551, NULL);}
        break;
      case 186: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy460 = addSubquery(NULL, yymsp[-2].minor.yy551, &yymsp[0].minor.yy0);}
        break;
      case 187: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy460 = addSubquery(yymsp[-5].minor.yy460, yymsp[-2].minor.yy551, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy460 = yylhsminor.yy460;
        break;
      case 188: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy460 = yylhsminor.yy460;
        break;
      case 189: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy460 = yylhsminor.yy460;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(yymsp[-3].minor.yy460, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy460 = yylhsminor.yy460;
        break;
      case 191: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy460 = setTableNameList(yymsp[-4].minor.yy460, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy460 = yylhsminor.yy460;
        break;
      case 192: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 193: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy160.interval = yymsp[-1].minor.yy0; yylhsminor.yy160.offset.n = 0; yylhsminor.yy160.token = yymsp[-3].minor.yy262;}
  yymsp[-3].minor.yy160 = yylhsminor.yy160;
        break;
      case 194: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy160.interval = yymsp[-3].minor.yy0; yylhsminor.yy160.offset = yymsp[-1].minor.yy0;   yylhsminor.yy160.token = yymsp[-5].minor.yy262;}
  yymsp[-5].minor.yy160 = yylhsminor.yy160;
        break;
      case 195: /* interval_option ::= */
{memset(&yymsp[1].minor.yy160, 0, sizeof(yymsp[1].minor.yy160));}
        break;
      case 196: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy262 = TK_INTERVAL;}
        break;
      case 197: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy262 = TK_EVERY;   }
        break;
      case 198: /* session_option ::= */
{yymsp[1].minor.yy511.col.n = 0; yymsp[1].minor.yy511.gap.n = 0;}
        break;
      case 199: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy511.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy511.gap = yymsp[-1].minor.yy0;
}
        break;
      case 200: /* windowstate_option ::= */
{ yymsp[1].minor.yy258.col.n = 0; yymsp[1].minor.yy258.col.z = NULL;}
        break;
      case 201: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy258.col = yymsp[-1].minor.yy0; }
        break;
      case 202: /* fill_opt ::= */
{ yymsp[1].minor.yy135 = 0;     }
        break;
      case 203: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy135, &A, -1, 0);
    yymsp[-5].minor.yy135 = yymsp[-1].minor.yy135;
}
        break;
      case 204: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy135 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 205: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 206: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 208: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 209: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy135 = tListItemAppend(yymsp[-3].minor.yy135, &yymsp[-1].minor.yy191, yymsp[0].minor.yy130);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 210: /* sortlist ::= item sortorder */
{
  yylhsminor.yy135 = tListItemAppend(NULL, &yymsp[-1].minor.yy191, yymsp[0].minor.yy130);
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 211: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy191, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy191 = yylhsminor.yy191;
        break;
      case 212: /* sortorder ::= ASC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 213: /* sortorder ::= DESC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_DESC;}
        break;
      case 214: /* sortorder ::= */
{ yymsp[1].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 215: /* groupby_opt ::= */
{ yymsp[1].minor.yy135 = 0;}
        break;
      case 216: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 217: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy135 = tListItemAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy191, -1);
}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 218: /* grouplist ::= item */
{
  yylhsminor.yy135 = tListItemAppend(NULL, &yymsp[0].minor.yy191, -1);
}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 219: /* having_opt ::= */
      case 229: /* where_opt ::= */ yytestcase(yyruleno==229);
      case 273: /* expritem ::= */ yytestcase(yyruleno==273);
{yymsp[1].minor.yy526 = 0;}
        break;
      case 220: /* having_opt ::= HAVING expr */
      case 230: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==230);
{yymsp[-1].minor.yy526 = yymsp[0].minor.yy526;}
        break;
      case 221: /* limit_opt ::= */
      case 225: /* slimit_opt ::= */ yytestcase(yyruleno==225);
{yymsp[1].minor.yy247.limit = -1; yymsp[1].minor.yy247.offset = 0;}
        break;
      case 222: /* limit_opt ::= LIMIT signed */
      case 226: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==226);
{yymsp[-1].minor.yy247.limit = yymsp[0].minor.yy531;  yymsp[-1].minor.yy247.offset = 0;}
        break;
      case 223: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy247.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy247.offset = yymsp[0].minor.yy531;}
        break;
      case 224: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy247.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy247.offset = yymsp[-2].minor.yy531;}
        break;
      case 227: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy247.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy247.offset = yymsp[0].minor.yy531;}
        break;
      case 228: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy247.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy247.offset = yymsp[-2].minor.yy531;}
        break;
      case 231: /* expr ::= LP expr RP */
{yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy526->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 232: /* expr ::= ID */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 233: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 234: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 235: /* expr ::= INTEGER */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 236: /* expr ::= MINUS INTEGER */
      case 237: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==237);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 238: /* expr ::= FLOAT */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 239: /* expr ::= MINUS FLOAT */
      case 240: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==240);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 241: /* expr ::= STRING */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 242: /* expr ::= NOW */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 243: /* expr ::= VARIABLE */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 244: /* expr ::= PLUS VARIABLE */
      case 245: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==245);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 246: /* expr ::= BOOL */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 247: /* expr ::= NULL */
{ yylhsminor.yy526 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 248: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy526 = tSqlExprCreateFunction(yymsp[-1].minor.yy135, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 249: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy526 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 250: /* expr ::= expr IS NULL */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 251: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-3].minor.yy526, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy526 = yylhsminor.yy526;
        break;
      case 252: /* expr ::= expr LT expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LT);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 253: /* expr ::= expr GT expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_GT);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 254: /* expr ::= expr LE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 255: /* expr ::= expr GE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_GE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 256: /* expr ::= expr NE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_NE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 257: /* expr ::= expr EQ expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_EQ);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 258: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy526); yylhsminor.yy526 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy526, yymsp[-2].minor.yy526, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy526, TK_LE), TK_AND);}
  yymsp[-4].minor.yy526 = yylhsminor.yy526;
        break;
      case 259: /* expr ::= expr AND expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_AND);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 260: /* expr ::= expr OR expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_OR); }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 261: /* expr ::= expr PLUS expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_PLUS);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 262: /* expr ::= expr MINUS expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_MINUS); }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 263: /* expr ::= expr STAR expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_STAR);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 264: /* expr ::= expr SLASH expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_DIVIDE);}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 265: /* expr ::= expr REM expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_REM);   }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 266: /* expr ::= expr LIKE expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_LIKE);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 267: /* expr ::= expr MATCH expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_MATCH);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 268: /* expr ::= expr NMATCH expr */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-2].minor.yy526, yymsp[0].minor.yy526, TK_NMATCH);  }
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 269: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy526 = tSqlExprCreate(yymsp[-4].minor.yy526, (tSqlExpr*)yymsp[-1].minor.yy135, TK_IN); }
  yymsp[-4].minor.yy526 = yylhsminor.yy526;
        break;
      case 270: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy135 = tSqlExprListAppend(yymsp[-2].minor.yy135,yymsp[0].minor.yy526,0, 0);}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 271: /* exprlist ::= expritem */
{yylhsminor.yy135 = tSqlExprListAppend(0,yymsp[0].minor.yy526,0, 0);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 272: /* expritem ::= expr */
{yylhsminor.yy526 = yymsp[0].minor.yy526;}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 274: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 275: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy191, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy191, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 293: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 294: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
