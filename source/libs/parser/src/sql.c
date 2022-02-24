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
#include "astGenerator.h"
#include "tmsgtype.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "tvariant.h"
#include "parserInt.h"
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
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SVariant yy1;
  SField yy16;
  int yy40;
  SIntervalVal yy52;
  int64_t yy61;
  SSubclause* yy93;
  SWindowStateVal yy112;
  SRelationInfo* yy160;
  SCreatedTableInfo yy184;
  SSqlNode* yy185;
  SArray* yy225;
  tSqlExpr* yy226;
  SCreateDbInfo yy326;
  int32_t yy460;
  SSessionWindowVal yy463;
  SCreateTableSql* yy482;
  SLimit yy495;
  SCreateAcctInfo yy523;
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
#define YYNRULE              304
#define YYNTOKEN             192
#define YY_MAX_SHIFT         367
#define YY_MIN_SHIFTREDUCE   590
#define YY_MAX_SHIFTREDUCE   893
#define YY_ERROR_ACTION      894
#define YY_ACCEPT_ACTION     895
#define YY_NO_ACTION         896
#define YY_MIN_REDUCE        897
#define YY_MAX_REDUCE        1200
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
#define YY_ACTTAB_COUNT (781)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    91,  641,  242, 1085,  676,  249, 1050,   55,   56,  641,
 /*    10 */    59,   60,  895,  367,  252,   49,   48,   47, 1075,   58,
 /*    20 */   325,   63,   61,   64,   62,  641,  641,  366,  230,   54,
 /*    30 */    53,  206,  248,   52,   51,   50,  233,   55,   56,  246,
 /*    40 */    59,   60, 1176, 1050,  252,   49,   48,   47,  104,   58,
 /*    50 */   325,   63,   61,   64,   62, 1022,   21, 1020, 1021,   54,
 /*    60 */    53, 1075, 1023,   52,   51,   50, 1024,  206, 1025, 1026,
 /*    70 */   280,  279, 1082,   55,   56, 1044,   59,   60, 1177,  274,
 /*    80 */   252,   49,   48,   47,   89,   58,  325,   63,   61,   64,
 /*    90 */    62,   39,  236, 1062,  206,   54,   53,  362,  982,   52,
 /*   100 */    51,   50,   27,   55,   57, 1177,   59,   60,  323,  830,
 /*   110 */   252,   49,   48,   47, 1075,   58,  325,   63,   61,   64,
 /*   120 */    62,  243,  294,   80,   81,   54,   53,  795,  796,   52,
 /*   130 */    51,   50,  234,  116,   56,  232,   59,   60,  311, 1047,
 /*   140 */   252,   49,   48,   47,  104,   58,  325,   63,   61,   64,
 /*   150 */    62,   42,  776,  361,  360,   54,   53,  952,  359,   52,
 /*   160 */    51,   50,  358,   43,  357,  356, 1033, 1034,   30, 1037,
 /*   170 */   253,   42,  319,  361,  360,  318,  317,  316,  359,  315,
 /*   180 */   314,  313,  358,  312,  357,  356,  310, 1014, 1002, 1003,
 /*   190 */  1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013,
 /*   200 */  1015, 1016, 1017, 1018,  641,   59,   60,  159,  773,  252,
 /*   210 */    49,   48,   47,  113,   58,  325,   63,   61,   64,   62,
 /*   220 */  1124,  355,  292,  355,   54,   53,  836,  839,   52,   51,
 /*   230 */    50,  282,  206,   54,   53,    7,  321,   52,   51,   50,
 /*   240 */   780,  723,   22, 1177,  591,  592,  593,  594,  595,  596,
 /*   250 */   597,  598,  599,  600,  601,  602,  603,  604,  199,  215,
 /*   260 */   231,  251,  845,  834,  837,  840,  216,  345,  344,  198,
 /*   270 */   195,  193,  175,  174,  172,  217,   80,  321,   83,  330,
 /*   280 */    80,  251,  845,  834,  837,  840,   52,   51,   50,  228,
 /*   290 */   229,  121,   78,  326,   63,   61,   64,   62,  759,  756,
 /*   300 */   757,  758,   54,   53,  835,  838,   52,   51,   50,  228,
 /*   310 */   229,  255,  751,  748,  749,  750,   43, 1061,   79,  203,
 /*   320 */    43,    3,   32,  131,   39,  257,  258, 1038,  104,  129,
 /*   330 */    85,  123,  133,  104,   39,  945,   39,   39,   65,  244,
 /*   340 */   245,  158,  273,   39,   86,   39,  843,  746,  747,  305,
 /*   350 */   260,  224,  189,  186,  183,  149,  142,  162,   65,  181,
 /*   360 */   179,  178,  177,  176,  167,  170,  160,   39,  240,  269,
 /*   370 */    39,   88, 1047,  164,  204,   39,  846,  841,  241,  209,
 /*   380 */   334,  335, 1047,  842, 1047, 1047,  812,  336,   39,  337,
 /*   390 */   256, 1047,  254, 1047,  333,  332,  846,  841,  327,  266,
 /*   400 */    12,  261,   39,  842, 1123, 1035,   84,  261,  270,   82,
 /*   410 */   124,  341,  760,  761,  342, 1047,  127,   92, 1047,  343,
 /*   420 */   365,  364,  190, 1047,  844,   93,  752,  753,   71,  262,
 /*   430 */    35,  259,  347,  340,  339,  275, 1047,  119,  955,  261,
 /*   440 */   792,  946,  802,  803,  158,   74,  811,  158, 1048,  832,
 /*   450 */  1046,   40,  733,  297,  744,  745,   97,   70,   66,   24,
 /*   460 */   735,  299,  734,  868,  847,   70,  300,  250,   40,   40,
 /*   470 */  1049,   72,  640,   14,   77,   13,   67,  117,   67,   23,
 /*   480 */    23,  833,  210,  140,  211,  139,   75, 1171,   23,    4,
 /*   490 */  1170,   16,   18,   15,   17,  764,  765,  762,  763,  147,
 /*   500 */  1134,  146,   20, 1169,   19,  849,  169,  168,  226,  722,
 /*   510 */   227,  207,  208,  212,  205, 1196,  213,  214,  219,  220,
 /*   520 */  1188,  221, 1077,  218,  202, 1133,  238,   44, 1130, 1076,
 /*   530 */   277, 1129,  239,  346,  114, 1116, 1115,  324,  196,  271,
 /*   540 */   791,   76,  281, 1045,  235,  276,   73,   87, 1084,  283,
 /*   550 */   285, 1095,  295,   46,  293,  291,   90,  108,   94, 1092,
 /*   560 */  1093, 1097,   95, 1073,  101,  286,  288, 1117,  105,  106,
 /*   570 */   107,  109,  289,  110,  111,  287,  284,  112,   45,   29,
 /*   580 */   306,  115,  225,  958, 1043,  150,  118,  247,  980,  120,
 /*   590 */   301,  959,  302,  303,  304,  348,  307,  308,  200,  349,
 /*   600 */   151,   38,  322,  350,  954,  957,  130,  953,  331, 1195,
 /*   610 */   137,  351, 1194,  352, 1191,  353,  354,  363,  141,  222,
 /*   620 */   338,  223, 1187,  144, 1186, 1183,  893,  148,  979,  265,
 /*   630 */   264,  892,  268,  891,  874,  873,   41,   31,    8,   70,
 /*   640 */   201,   28,  296,  153,  157,  272,  152,  154,  155,  943,
 /*   650 */   156,  163,  941,  165,  166,  939,  938,  263,    1,  981,
 /*   660 */   937,  171,  936,  173,  935,  934,  933,  932,  931,  930,
 /*   670 */   767,  929,  267,  180,  278,  182,  928,  184,  185,  187,
 /*   680 */   927,  926,  924,  188,  922,  919,  793,   96,   98,  920,
 /*   690 */   194,  917,  804,  197,  918,  913,   99,  100,    2,  798,
 /*   700 */   102,  237,    9,  800,   33,  103,   34,   10,  298,  290,
 /*   710 */    11,   25,   26,  119,  122,  126,  654,  693,  309,  692,
 /*   720 */    36,  125,  689,   37,  687,  128,  686,  685,  683,  682,
 /*   730 */   681,  678,  644,  132,  134,  135,    5,  850,  320,  848,
 /*   740 */     6,  329,  328,   68,   69,  136,  138,  143,  725,   40,
 /*   750 */   145,  724,  721,  670,  668,  660,  666,  662,  664,  658,
 /*   760 */   656,  691,  690,  688,  684,  680,  679,  161,  642,  897,
 /*   770 */   896,  608,  896,  896,  896,  896,  896,  896,  896,  191,
 /*   780 */   192,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   194,    1,  239,  194,    3,  200,  243,    7,    8,    1,
 /*    10 */    10,   11,  192,  193,   14,   15,   16,   17,  241,   19,
 /*    20 */    20,   21,   22,   23,   24,    1,    1,  194,  195,   29,
 /*    30 */    30,  262,  200,   33,   34,   35,  259,    7,    8,  239,
 /*    40 */    10,   11,  273,  243,   14,   15,   16,   17,  194,   19,
 /*    50 */    20,   21,   22,   23,   24,  216,  262,  218,  219,   29,
 /*    60 */    30,  241,  223,   33,   34,   35,  227,  262,  229,  230,
 /*    70 */   264,  265,  263,    7,    8,  194,   10,   11,  273,  259,
 /*    80 */    14,   15,   16,   17,   84,   19,   20,   21,   22,   23,
 /*    90 */    24,  194,  244,  245,  262,   29,   30,  214,  215,   33,
 /*   100 */    34,   35,   78,    7,    8,  273,   10,   11,   83,   79,
 /*   110 */    14,   15,   16,   17,  241,   19,   20,   21,   22,   23,
 /*   120 */    24,  240,  268,   78,  270,   29,   30,  124,  125,   33,
 /*   130 */    34,   35,  259,  201,    8,  238,   10,   11,   63,  242,
 /*   140 */    14,   15,   16,   17,  194,   19,   20,   21,   22,   23,
 /*   150 */    24,   96,   33,   98,   99,   29,   30,    1,  103,   33,
 /*   160 */    34,   35,  107,  118,  109,  110,  234,  235,  236,  237,
 /*   170 */   200,   96,   97,   98,   99,  100,  101,  102,  103,  104,
 /*   180 */   105,  106,  107,  108,  109,  110,  111,  216,  217,  218,
 /*   190 */   219,  220,  221,  222,  223,  224,  225,  226,  227,  228,
 /*   200 */   229,  230,  231,  232,    1,   10,   11,   74,   95,   14,
 /*   210 */    15,   16,   17,  249,   19,   20,   21,   22,   23,   24,
 /*   220 */   270,   88,  272,   88,   29,   30,    3,    4,   33,   34,
 /*   230 */    35,  267,  262,   29,   30,  122,   80,   33,   34,   35,
 /*   240 */   121,    3,   40,  273,   41,   42,   43,   44,   45,   46,
 /*   250 */    47,   48,   49,   50,   51,   52,   53,   54,   55,   57,
 /*   260 */    57,    1,    2,    3,    4,    5,   64,   29,   30,   58,
 /*   270 */    59,   60,   70,   71,   72,   73,   78,   80,  246,   77,
 /*   280 */    78,    1,    2,    3,    4,    5,   33,   34,   35,   29,
 /*   290 */    30,  201,  260,   33,   21,   22,   23,   24,    2,    3,
 /*   300 */     4,    5,   29,   30,    3,    4,   33,   34,   35,   29,
 /*   310 */    30,   64,    2,    3,    4,    5,  118,  245,  120,  262,
 /*   320 */   118,   58,   59,   60,  194,   29,   30,  237,  194,   66,
 /*   330 */    67,   68,   69,  194,  194,  199,  194,  194,   78,   29,
 /*   340 */    30,  205,  140,  194,  142,  194,  123,    3,    4,   86,
 /*   350 */    64,  149,   58,   59,   60,   58,   59,   60,   78,   65,
 /*   360 */    66,   67,   68,   69,   67,   68,   69,  194,  238,  111,
 /*   370 */   194,  201,  242,   76,  262,  194,  116,  117,  238,  262,
 /*   380 */   238,  238,  242,  123,  242,  242,   72,  238,  194,  238,
 /*   390 */   143,  242,  145,  242,  147,  148,  116,  117,    9,  141,
 /*   400 */    78,  194,  194,  123,  270,  235,   84,  194,  150,  270,
 /*   410 */   203,  238,  116,  117,  238,  242,  203,   79,  242,  238,
 /*   420 */    61,   62,   63,  242,  123,   79,  116,  117,   95,  143,
 /*   430 */    78,  145,  238,  147,  148,   79,  242,  115,  199,  194,
 /*   440 */    79,  199,   79,   79,  205,   95,  132,  205,  203,    1,
 /*   450 */   242,   95,   79,   79,    3,    4,   95,  119,   95,   95,
 /*   460 */    79,   79,   79,   79,   79,  119,  114,   56,   95,   95,
 /*   470 */   243,  138,   79,  144,   78,  146,   95,   95,   95,   95,
 /*   480 */    95,   33,  262,  144,  262,  146,  136,  262,   95,   78,
 /*   490 */   262,  144,  144,  146,  146,    3,    4,    3,    4,  144,
 /*   500 */   233,  146,  144,  262,  146,  116,   74,   75,  262,  113,
 /*   510 */   262,  262,  262,  262,  262,  245,  262,  262,  262,  262,
 /*   520 */   245,  262,  241,  262,  262,  233,  233,  261,  233,  241,
 /*   530 */   241,  233,  233,  233,  247,  271,  271,  194,   56,  194,
 /*   540 */   123,  135,  266,  241,  266,  196,  137,  194,  194,  266,
 /*   550 */   266,  194,  130,  134,  133,  128,  196,  254,  196,  194,
 /*   560 */   194,  194,  194,  258,  194,  196,  194,  196,  257,  256,
 /*   570 */   255,  253,  127,  252,  251,  126,  129,  250,  139,  248,
 /*   580 */    87,  194,  196,  204,  194,   94,  194,  196,  213,  194,
 /*   590 */   196,  194,  194,  194,  194,   93,  194,  194,  194,   47,
 /*   600 */   212,  194,  194,   90,  194,  204,  201,  194,  194,  194,
 /*   610 */   194,   92,  194,   51,  194,   91,   89,   80,  194,  196,
 /*   620 */   194,  196,  194,  194,  194,  194,    3,  194,  194,    3,
 /*   630 */   151,    3,    3,    3,   98,   97,  194,  194,   78,  119,
 /*   640 */   194,   78,  114,  207,  206,  141,  211,  210,  208,  194,
 /*   650 */   209,  194,  194,  194,  194,  194,  194,  194,  202,  215,
 /*   660 */   196,  194,  196,  194,  194,  194,  194,  194,  194,  194,
 /*   670 */    79,  194,  151,  197,   95,  197,  196,  194,  197,  194,
 /*   680 */   196,  194,  194,  197,  194,  196,   79,   95,   78,  194,
 /*   690 */   194,  194,   79,  194,  196,  194,   78,   95,  198,   79,
 /*   700 */    78,    1,  131,   79,   95,   78,   95,  131,  114,   78,
 /*   710 */    78,   78,   78,  115,   74,   66,    3,    3,  112,    3,
 /*   720 */    85,   84,    5,   85,    3,   84,    3,    3,    3,    3,
 /*   730 */     3,    3,   81,   74,   82,   82,   78,  116,    9,   79,
 /*   740 */    78,   55,   20,   10,   10,  146,  146,  146,    3,   95,
 /*   750 */   146,    3,   79,    3,    3,    3,    3,    3,    3,    3,
 /*   760 */     3,    3,    3,    3,    3,    3,    3,   95,   81,    0,
 /*   770 */   274,   56,  274,  274,  274,  274,  274,  274,  274,   15,
 /*   780 */    15,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   790 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   800 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   810 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   820 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   830 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   840 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   850 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   860 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   870 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   880 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   890 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   900 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   910 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   920 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   930 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   940 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   950 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   960 */   274,  274,  274,  274,  274,  274,  274,  274,  274,  274,
 /*   970 */   274,  274,  274,
};
#define YY_SHIFT_COUNT    (367)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (769)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   202,   75,   55,  197,  260,  280,  280,   24,    8,    8,
 /*    10 */     8,    8,    8,    8,    8,    8,    8,    8,    8,    8,
 /*    20 */     8,    0,  203,  280,  296,  310,  310,   45,   45,    3,
 /*    30 */   156,  133,  197,    8,    8,    8,    8,    8,  135,    8,
 /*    40 */     8,  135,    1,  781,  280,  280,  280,  280,  280,  280,
 /*    50 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*    60 */   280,  280,  280,  280,  280,  280,  296,  310,  296,  296,
 /*    70 */   198,  238,  238,  238,  238,  238,  238,  238,  119,   45,
 /*    80 */    45,  314,  314,  113,   45,   25,    8,  482,    8,    8,
 /*    90 */     8,  482,    8,    8,    8,  482,    8,  417,  417,  417,
 /*   100 */   417,  482,    8,    8,  482,  406,  409,  422,  419,  421,
 /*   110 */   427,  445,  449,  447,  439,  482,    8,    8,  482,    8,
 /*   120 */   482,    8,    8,    8,  493,    8,    8,  493,    8,    8,
 /*   130 */     8,  197,    8,    8,    8,    8,    8,    8,    8,    8,
 /*   140 */     8,  482,    8,    8,    8,    8,    8,    8,  482,    8,
 /*   150 */     8,  491,  502,  552,  513,  519,  562,  524,  527,    8,
 /*   160 */     8,    1,    8,    8,    8,    8,    8,    8,    8,    8,
 /*   170 */     8,  482,    8,  482,    8,    8,    8,    8,    8,    8,
 /*   180 */     8,  537,    8,  537,  482,    8,  537,  482,    8,  537,
 /*   190 */     8,    8,    8,    8,  482,    8,    8,  482,    8,    8,
 /*   200 */   781,  781,   30,   66,   66,   96,   66,  126,  195,  273,
 /*   210 */   273,  273,  273,  273,  273,  263,  294,  297,  204,  204,
 /*   220 */   204,  204,  247,  286,  258,  322,  253,  253,  223,  301,
 /*   230 */   359,  211,  356,  338,  346,  361,  363,  364,  333,  350,
 /*   240 */   373,  374,  381,  382,  344,  451,  383,  352,  384,  385,
 /*   250 */   448,  411,  389,  393,  329,  339,  347,  492,  494,  348,
 /*   260 */   355,  396,  358,  432,  623,  479,  626,  628,  521,  629,
 /*   270 */   630,  536,  538,  504,  520,  528,  560,  591,  563,  579,
 /*   280 */   592,  607,  610,  613,  618,  620,  602,  622,  624,  627,
 /*   290 */   700,  631,  609,  571,  611,  576,  632,  528,  633,  594,
 /*   300 */   634,  598,  640,  635,  637,  649,  713,  638,  641,  714,
 /*   310 */   606,  716,  717,  721,  723,  724,  725,  726,  727,  728,
 /*   320 */   651,  729,  659,  652,  653,  658,  660,  621,  662,  722,
 /*   330 */   686,  733,  599,  600,  654,  654,  654,  654,  734,  601,
 /*   340 */   604,  654,  654,  654,  745,  748,  673,  654,  750,  751,
 /*   350 */   752,  753,  754,  755,  756,  757,  758,  759,  760,  761,
 /*   360 */   762,  763,  672,  687,  764,  765,  715,  769,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-237)
#define YY_REDUCE_MAX   (501)
static const short yy_reduce_ofst[] = {
 /*     0 */  -180,  -29, -161,  -68, -195, -168,  -30, -194, -103,  -50,
 /*    10 */  -146,  130,  140,  142,  143,  149,  151,  173,  176,  181,
 /*    20 */   194, -191, -167, -231, -152, -237, -200, -223, -127,  -36,
 /*    30 */    90,  136,  170,  134,  139, -119,  207,  213,  239,  245,
 /*    40 */   208,  242, -117,   32, -206,   57,  112,  117,  220,  222,
 /*    50 */   225,  228,  241,  246,  248,  249,  250,  251,  252,  254,
 /*    60 */   255,  256,  257,  259,  261,  262,   72,  227,  270,  275,
 /*    70 */   281,  267,  292,  293,  295,  298,  299,  300,  266,  288,
 /*    80 */   289,  264,  265,  287,  302,  343,  345,  349,  353,  354,
 /*    90 */   357,  360,  365,  366,  367,  362,  368,  276,  278,  283,
 /*   100 */   284,  369,  370,  372,  371,  305,  311,  313,  315,  303,
 /*   110 */   318,  321,  323,  327,  331,  386,  387,  390,  391,  392,
 /*   120 */   394,  395,  397,  398,  379,  399,  400,  401,  402,  403,
 /*   130 */   404,  405,  407,  408,  410,  413,  414,  415,  416,  418,
 /*   140 */   420,  423,  424,  426,  428,  429,  430,  431,  425,  433,
 /*   150 */   434,  375,  388,  435,  436,  437,  440,  441,  438,  442,
 /*   160 */   443,  444,  446,  455,  457,  458,  459,  460,  461,  462,
 /*   170 */   463,  464,  467,  466,  469,  470,  471,  472,  473,  474,
 /*   180 */   475,  476,  477,  478,  480,  483,  481,  484,  485,  486,
 /*   190 */   487,  488,  490,  495,  489,  496,  497,  498,  499,  501,
 /*   200 */   456,  500,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   894,  956,  944,  952, 1179, 1179, 1179,  894,  894,  894,
 /*    10 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*    20 */   894, 1086,  914, 1179,  894,  894,  894,  894,  894, 1101,
 /*    30 */  1036,  962,  952,  894,  894,  894,  894,  894,  962,  894,
 /*    40 */   894,  962,  894, 1081,  894,  894,  894,  894,  894,  894,
 /*    50 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*    60 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*    70 */   894,  894,  894,  894,  894,  894,  894,  894, 1088,  894,
 /*    80 */   894, 1120, 1120, 1079,  894,  894,  894,  916,  894,  894,
 /*    90 */  1094,  916, 1091,  894, 1096,  916,  894,  894,  894,  894,
 /*   100 */   894,  916,  894,  894,  916, 1127, 1131, 1113, 1125, 1121,
 /*   110 */  1108, 1106, 1104, 1112, 1135,  916,  894,  894,  916,  894,
 /*   120 */   916,  894,  894,  894,  960,  894,  894,  960,  894,  894,
 /*   130 */   894,  952,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   140 */   894,  916,  894,  894,  894,  894,  894,  894,  916,  894,
 /*   150 */   894,  978,  976,  974,  966,  972,  968,  970,  964,  894,
 /*   160 */   894,  894,  894,  942,  894,  940,  894,  894,  894,  894,
 /*   170 */   894,  916,  894,  916,  894,  894,  894,  894,  894,  894,
 /*   180 */   894,  950,  894,  950,  916,  894,  950,  916,  894,  950,
 /*   190 */   925,  894,  894,  894,  916,  894,  894,  916,  894,  912,
 /*   200 */  1001, 1019,  894, 1136, 1126,  894, 1178, 1166, 1165, 1174,
 /*   210 */  1173, 1172, 1164, 1163, 1162,  894,  894,  894, 1158, 1161,
 /*   220 */  1160, 1159,  894,  894,  894,  894, 1168, 1167,  894,  894,
 /*   230 */   894,  894,  894,  894,  894,  894,  894,  894, 1132, 1128,
 /*   240 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   250 */   894, 1138,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   260 */   894, 1027,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   270 */   894,  894,  894,  894, 1078,  894,  894,  894,  894, 1090,
 /*   280 */  1089,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   290 */   894,  894, 1122,  894, 1114,  894,  894, 1039,  894,  894,
 /*   300 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   310 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   320 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   330 */   894,  894,  894,  894, 1197, 1192, 1193, 1190,  894,  894,
 /*   340 */   894, 1189, 1184, 1185,  894,  894,  894, 1182,  894,  894,
 /*   350 */   894,  894,  894,  894,  894,  894,  894,  894,  894,  894,
 /*   360 */   894,  894,  984,  894,  923,  921,  894,  894,
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
    1,  /*    INTEGER => ID */
    1,  /*      FLOAT => ID */
    1,  /*     STRING => ID */
    1,  /*  TIMESTAMP => ID */
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
    0,  /*       PORT => nothing */
    1,  /*    IPTOKEN => ID */
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
    0,  /*     STREAM => nothing */
    0,  /*       MODE => nothing */
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
  /*    3 */ "INTEGER",
  /*    4 */ "FLOAT",
  /*    5 */ "STRING",
  /*    6 */ "TIMESTAMP",
  /*    7 */ "OR",
  /*    8 */ "AND",
  /*    9 */ "NOT",
  /*   10 */ "EQ",
  /*   11 */ "NE",
  /*   12 */ "ISNULL",
  /*   13 */ "NOTNULL",
  /*   14 */ "IS",
  /*   15 */ "LIKE",
  /*   16 */ "MATCH",
  /*   17 */ "NMATCH",
  /*   18 */ "GLOB",
  /*   19 */ "BETWEEN",
  /*   20 */ "IN",
  /*   21 */ "GT",
  /*   22 */ "GE",
  /*   23 */ "LT",
  /*   24 */ "LE",
  /*   25 */ "BITAND",
  /*   26 */ "BITOR",
  /*   27 */ "LSHIFT",
  /*   28 */ "RSHIFT",
  /*   29 */ "PLUS",
  /*   30 */ "MINUS",
  /*   31 */ "DIVIDE",
  /*   32 */ "TIMES",
  /*   33 */ "STAR",
  /*   34 */ "SLASH",
  /*   35 */ "REM",
  /*   36 */ "CONCAT",
  /*   37 */ "UMINUS",
  /*   38 */ "UPLUS",
  /*   39 */ "BITNOT",
  /*   40 */ "SHOW",
  /*   41 */ "DATABASES",
  /*   42 */ "TOPICS",
  /*   43 */ "FUNCTIONS",
  /*   44 */ "MNODES",
  /*   45 */ "DNODES",
  /*   46 */ "ACCOUNTS",
  /*   47 */ "USERS",
  /*   48 */ "MODULES",
  /*   49 */ "QUERIES",
  /*   50 */ "CONNECTIONS",
  /*   51 */ "STREAMS",
  /*   52 */ "VARIABLES",
  /*   53 */ "SCORES",
  /*   54 */ "GRANTS",
  /*   55 */ "VNODES",
  /*   56 */ "DOT",
  /*   57 */ "CREATE",
  /*   58 */ "TABLE",
  /*   59 */ "STABLE",
  /*   60 */ "DATABASE",
  /*   61 */ "TABLES",
  /*   62 */ "STABLES",
  /*   63 */ "VGROUPS",
  /*   64 */ "DROP",
  /*   65 */ "TOPIC",
  /*   66 */ "FUNCTION",
  /*   67 */ "DNODE",
  /*   68 */ "USER",
  /*   69 */ "ACCOUNT",
  /*   70 */ "USE",
  /*   71 */ "DESCRIBE",
  /*   72 */ "DESC",
  /*   73 */ "ALTER",
  /*   74 */ "PASS",
  /*   75 */ "PRIVILEGE",
  /*   76 */ "LOCAL",
  /*   77 */ "COMPACT",
  /*   78 */ "LP",
  /*   79 */ "RP",
  /*   80 */ "IF",
  /*   81 */ "EXISTS",
  /*   82 */ "PORT",
  /*   83 */ "IPTOKEN",
  /*   84 */ "AS",
  /*   85 */ "OUTPUTTYPE",
  /*   86 */ "AGGREGATE",
  /*   87 */ "BUFSIZE",
  /*   88 */ "PPS",
  /*   89 */ "TSERIES",
  /*   90 */ "DBS",
  /*   91 */ "STORAGE",
  /*   92 */ "QTIME",
  /*   93 */ "CONNS",
  /*   94 */ "STATE",
  /*   95 */ "COMMA",
  /*   96 */ "KEEP",
  /*   97 */ "CACHE",
  /*   98 */ "REPLICA",
  /*   99 */ "QUORUM",
  /*  100 */ "DAYS",
  /*  101 */ "MINROWS",
  /*  102 */ "MAXROWS",
  /*  103 */ "BLOCKS",
  /*  104 */ "CTIME",
  /*  105 */ "WAL",
  /*  106 */ "FSYNC",
  /*  107 */ "COMP",
  /*  108 */ "PRECISION",
  /*  109 */ "UPDATE",
  /*  110 */ "CACHELAST",
  /*  111 */ "STREAM",
  /*  112 */ "MODE",
  /*  113 */ "UNSIGNED",
  /*  114 */ "TAGS",
  /*  115 */ "USING",
  /*  116 */ "NULL",
  /*  117 */ "NOW",
  /*  118 */ "SELECT",
  /*  119 */ "UNION",
  /*  120 */ "ALL",
  /*  121 */ "DISTINCT",
  /*  122 */ "FROM",
  /*  123 */ "VARIABLE",
  /*  124 */ "INTERVAL",
  /*  125 */ "EVERY",
  /*  126 */ "SESSION",
  /*  127 */ "STATE_WINDOW",
  /*  128 */ "FILL",
  /*  129 */ "SLIDING",
  /*  130 */ "ORDER",
  /*  131 */ "BY",
  /*  132 */ "ASC",
  /*  133 */ "GROUP",
  /*  134 */ "HAVING",
  /*  135 */ "LIMIT",
  /*  136 */ "OFFSET",
  /*  137 */ "SLIMIT",
  /*  138 */ "SOFFSET",
  /*  139 */ "WHERE",
  /*  140 */ "RESET",
  /*  141 */ "QUERY",
  /*  142 */ "SYNCDB",
  /*  143 */ "ADD",
  /*  144 */ "COLUMN",
  /*  145 */ "MODIFY",
  /*  146 */ "TAG",
  /*  147 */ "CHANGE",
  /*  148 */ "SET",
  /*  149 */ "KILL",
  /*  150 */ "CONNECTION",
  /*  151 */ "COLON",
  /*  152 */ "ABORT",
  /*  153 */ "AFTER",
  /*  154 */ "ATTACH",
  /*  155 */ "BEFORE",
  /*  156 */ "BEGIN",
  /*  157 */ "CASCADE",
  /*  158 */ "CLUSTER",
  /*  159 */ "CONFLICT",
  /*  160 */ "COPY",
  /*  161 */ "DEFERRED",
  /*  162 */ "DELIMITERS",
  /*  163 */ "DETACH",
  /*  164 */ "EACH",
  /*  165 */ "END",
  /*  166 */ "EXPLAIN",
  /*  167 */ "FAIL",
  /*  168 */ "FOR",
  /*  169 */ "IGNORE",
  /*  170 */ "IMMEDIATE",
  /*  171 */ "INITIALLY",
  /*  172 */ "INSTEAD",
  /*  173 */ "KEY",
  /*  174 */ "OF",
  /*  175 */ "RAISE",
  /*  176 */ "REPLACE",
  /*  177 */ "RESTRICT",
  /*  178 */ "ROW",
  /*  179 */ "STATEMENT",
  /*  180 */ "TRIGGER",
  /*  181 */ "VIEW",
  /*  182 */ "SEMI",
  /*  183 */ "NONE",
  /*  184 */ "PREV",
  /*  185 */ "LINEAR",
  /*  186 */ "IMPORT",
  /*  187 */ "TBNAME",
  /*  188 */ "JOIN",
  /*  189 */ "INSERT",
  /*  190 */ "INTO",
  /*  191 */ "VALUES",
  /*  192 */ "program",
  /*  193 */ "cmd",
  /*  194 */ "ids",
  /*  195 */ "dbPrefix",
  /*  196 */ "cpxName",
  /*  197 */ "ifexists",
  /*  198 */ "alter_db_optr",
  /*  199 */ "acct_optr",
  /*  200 */ "exprlist",
  /*  201 */ "ifnotexists",
  /*  202 */ "db_optr",
  /*  203 */ "typename",
  /*  204 */ "bufsize",
  /*  205 */ "pps",
  /*  206 */ "tseries",
  /*  207 */ "dbs",
  /*  208 */ "streams",
  /*  209 */ "storage",
  /*  210 */ "qtime",
  /*  211 */ "users",
  /*  212 */ "conns",
  /*  213 */ "state",
  /*  214 */ "intitemlist",
  /*  215 */ "intitem",
  /*  216 */ "keep",
  /*  217 */ "cache",
  /*  218 */ "replica",
  /*  219 */ "quorum",
  /*  220 */ "days",
  /*  221 */ "minrows",
  /*  222 */ "maxrows",
  /*  223 */ "blocks",
  /*  224 */ "ctime",
  /*  225 */ "wal",
  /*  226 */ "fsync",
  /*  227 */ "comp",
  /*  228 */ "prec",
  /*  229 */ "update",
  /*  230 */ "cachelast",
  /*  231 */ "vgroups",
  /*  232 */ "stream_mode",
  /*  233 */ "signed",
  /*  234 */ "create_table_args",
  /*  235 */ "create_stable_args",
  /*  236 */ "create_table_list",
  /*  237 */ "create_from_stable",
  /*  238 */ "columnlist",
  /*  239 */ "tagitemlist1",
  /*  240 */ "tagNamelist",
  /*  241 */ "select",
  /*  242 */ "column",
  /*  243 */ "tagitem1",
  /*  244 */ "tagitemlist",
  /*  245 */ "tagitem",
  /*  246 */ "selcollist",
  /*  247 */ "from",
  /*  248 */ "where_opt",
  /*  249 */ "interval_option",
  /*  250 */ "sliding_opt",
  /*  251 */ "session_option",
  /*  252 */ "windowstate_option",
  /*  253 */ "fill_opt",
  /*  254 */ "groupby_opt",
  /*  255 */ "having_opt",
  /*  256 */ "orderby_opt",
  /*  257 */ "slimit_opt",
  /*  258 */ "limit_opt",
  /*  259 */ "union",
  /*  260 */ "sclp",
  /*  261 */ "distinct",
  /*  262 */ "expr",
  /*  263 */ "as",
  /*  264 */ "tablelist",
  /*  265 */ "sub",
  /*  266 */ "tmvar",
  /*  267 */ "intervalKey",
  /*  268 */ "sortlist",
  /*  269 */ "sortitem",
  /*  270 */ "item",
  /*  271 */ "sortorder",
  /*  272 */ "grouplist",
  /*  273 */ "expritem",
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
 /*  48 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  50 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  51 */ "ids ::= ID",
 /*  52 */ "ifexists ::= IF EXISTS",
 /*  53 */ "ifexists ::=",
 /*  54 */ "ifnotexists ::= IF NOT EXISTS",
 /*  55 */ "ifnotexists ::=",
 /*  56 */ "cmd ::= CREATE DNODE ids PORT ids",
 /*  57 */ "cmd ::= CREATE DNODE IPTOKEN PORT ids",
 /*  58 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  59 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  60 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  61 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE USER ids PASS ids",
 /*  63 */ "bufsize ::=",
 /*  64 */ "bufsize ::= BUFSIZE INTEGER",
 /*  65 */ "pps ::=",
 /*  66 */ "pps ::= PPS INTEGER",
 /*  67 */ "tseries ::=",
 /*  68 */ "tseries ::= TSERIES INTEGER",
 /*  69 */ "dbs ::=",
 /*  70 */ "dbs ::= DBS INTEGER",
 /*  71 */ "streams ::=",
 /*  72 */ "streams ::= STREAMS INTEGER",
 /*  73 */ "storage ::=",
 /*  74 */ "storage ::= STORAGE INTEGER",
 /*  75 */ "qtime ::=",
 /*  76 */ "qtime ::= QTIME INTEGER",
 /*  77 */ "users ::=",
 /*  78 */ "users ::= USERS INTEGER",
 /*  79 */ "conns ::=",
 /*  80 */ "conns ::= CONNS INTEGER",
 /*  81 */ "state ::=",
 /*  82 */ "state ::= STATE ids",
 /*  83 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  84 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  85 */ "intitemlist ::= intitem",
 /*  86 */ "intitem ::= INTEGER",
 /*  87 */ "keep ::= KEEP intitemlist",
 /*  88 */ "cache ::= CACHE INTEGER",
 /*  89 */ "replica ::= REPLICA INTEGER",
 /*  90 */ "quorum ::= QUORUM INTEGER",
 /*  91 */ "days ::= DAYS INTEGER",
 /*  92 */ "minrows ::= MINROWS INTEGER",
 /*  93 */ "maxrows ::= MAXROWS INTEGER",
 /*  94 */ "blocks ::= BLOCKS INTEGER",
 /*  95 */ "ctime ::= CTIME INTEGER",
 /*  96 */ "wal ::= WAL INTEGER",
 /*  97 */ "fsync ::= FSYNC INTEGER",
 /*  98 */ "comp ::= COMP INTEGER",
 /*  99 */ "prec ::= PRECISION STRING",
 /* 100 */ "update ::= UPDATE INTEGER",
 /* 101 */ "cachelast ::= CACHELAST INTEGER",
 /* 102 */ "vgroups ::= VGROUPS INTEGER",
 /* 103 */ "stream_mode ::= STREAM MODE INTEGER",
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
 /* 120 */ "db_optr ::= db_optr vgroups",
 /* 121 */ "db_optr ::= db_optr stream_mode",
 /* 122 */ "alter_db_optr ::=",
 /* 123 */ "alter_db_optr ::= alter_db_optr replica",
 /* 124 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 125 */ "alter_db_optr ::= alter_db_optr keep",
 /* 126 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 127 */ "alter_db_optr ::= alter_db_optr comp",
 /* 128 */ "alter_db_optr ::= alter_db_optr update",
 /* 129 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 130 */ "typename ::= ids",
 /* 131 */ "typename ::= ids LP signed RP",
 /* 132 */ "typename ::= ids UNSIGNED",
 /* 133 */ "signed ::= INTEGER",
 /* 134 */ "signed ::= PLUS INTEGER",
 /* 135 */ "signed ::= MINUS INTEGER",
 /* 136 */ "cmd ::= CREATE TABLE create_table_args",
 /* 137 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 138 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 139 */ "cmd ::= CREATE TABLE create_table_list",
 /* 140 */ "create_table_list ::= create_from_stable",
 /* 141 */ "create_table_list ::= create_table_list create_from_stable",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 143 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 144 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 145 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 146 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 147 */ "tagNamelist ::= ids",
 /* 148 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 149 */ "columnlist ::= columnlist COMMA column",
 /* 150 */ "columnlist ::= column",
 /* 151 */ "column ::= ids typename",
 /* 152 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 153 */ "tagitemlist1 ::= tagitem1",
 /* 154 */ "tagitem1 ::= MINUS INTEGER",
 /* 155 */ "tagitem1 ::= MINUS FLOAT",
 /* 156 */ "tagitem1 ::= PLUS INTEGER",
 /* 157 */ "tagitem1 ::= PLUS FLOAT",
 /* 158 */ "tagitem1 ::= INTEGER",
 /* 159 */ "tagitem1 ::= FLOAT",
 /* 160 */ "tagitem1 ::= STRING",
 /* 161 */ "tagitem1 ::= BOOL",
 /* 162 */ "tagitem1 ::= NULL",
 /* 163 */ "tagitem1 ::= NOW",
 /* 164 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 165 */ "tagitemlist ::= tagitem",
 /* 166 */ "tagitem ::= INTEGER",
 /* 167 */ "tagitem ::= FLOAT",
 /* 168 */ "tagitem ::= STRING",
 /* 169 */ "tagitem ::= BOOL",
 /* 170 */ "tagitem ::= NULL",
 /* 171 */ "tagitem ::= NOW",
 /* 172 */ "tagitem ::= MINUS INTEGER",
 /* 173 */ "tagitem ::= MINUS FLOAT",
 /* 174 */ "tagitem ::= PLUS INTEGER",
 /* 175 */ "tagitem ::= PLUS FLOAT",
 /* 176 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 177 */ "select ::= LP select RP",
 /* 178 */ "union ::= select",
 /* 179 */ "union ::= union UNION ALL select",
 /* 180 */ "union ::= union UNION select",
 /* 181 */ "cmd ::= union",
 /* 182 */ "select ::= SELECT selcollist",
 /* 183 */ "sclp ::= selcollist COMMA",
 /* 184 */ "sclp ::=",
 /* 185 */ "selcollist ::= sclp distinct expr as",
 /* 186 */ "selcollist ::= sclp STAR",
 /* 187 */ "as ::= AS ids",
 /* 188 */ "as ::= ids",
 /* 189 */ "as ::=",
 /* 190 */ "distinct ::= DISTINCT",
 /* 191 */ "distinct ::=",
 /* 192 */ "from ::= FROM tablelist",
 /* 193 */ "from ::= FROM sub",
 /* 194 */ "sub ::= LP union RP",
 /* 195 */ "sub ::= LP union RP ids",
 /* 196 */ "sub ::= sub COMMA LP union RP ids",
 /* 197 */ "tablelist ::= ids cpxName",
 /* 198 */ "tablelist ::= ids cpxName ids",
 /* 199 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 200 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 201 */ "tmvar ::= VARIABLE",
 /* 202 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 203 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 204 */ "interval_option ::=",
 /* 205 */ "intervalKey ::= INTERVAL",
 /* 206 */ "intervalKey ::= EVERY",
 /* 207 */ "session_option ::=",
 /* 208 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 209 */ "windowstate_option ::=",
 /* 210 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 211 */ "fill_opt ::=",
 /* 212 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 213 */ "fill_opt ::= FILL LP ID RP",
 /* 214 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 215 */ "sliding_opt ::=",
 /* 216 */ "orderby_opt ::=",
 /* 217 */ "orderby_opt ::= ORDER BY sortlist",
 /* 218 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 219 */ "sortlist ::= item sortorder",
 /* 220 */ "item ::= ids cpxName",
 /* 221 */ "sortorder ::= ASC",
 /* 222 */ "sortorder ::= DESC",
 /* 223 */ "sortorder ::=",
 /* 224 */ "groupby_opt ::=",
 /* 225 */ "groupby_opt ::= GROUP BY grouplist",
 /* 226 */ "grouplist ::= grouplist COMMA item",
 /* 227 */ "grouplist ::= item",
 /* 228 */ "having_opt ::=",
 /* 229 */ "having_opt ::= HAVING expr",
 /* 230 */ "limit_opt ::=",
 /* 231 */ "limit_opt ::= LIMIT signed",
 /* 232 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 233 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 234 */ "slimit_opt ::=",
 /* 235 */ "slimit_opt ::= SLIMIT signed",
 /* 236 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 237 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 238 */ "where_opt ::=",
 /* 239 */ "where_opt ::= WHERE expr",
 /* 240 */ "expr ::= LP expr RP",
 /* 241 */ "expr ::= ID",
 /* 242 */ "expr ::= ID DOT ID",
 /* 243 */ "expr ::= ID DOT STAR",
 /* 244 */ "expr ::= INTEGER",
 /* 245 */ "expr ::= MINUS INTEGER",
 /* 246 */ "expr ::= PLUS INTEGER",
 /* 247 */ "expr ::= FLOAT",
 /* 248 */ "expr ::= MINUS FLOAT",
 /* 249 */ "expr ::= PLUS FLOAT",
 /* 250 */ "expr ::= STRING",
 /* 251 */ "expr ::= NOW",
 /* 252 */ "expr ::= VARIABLE",
 /* 253 */ "expr ::= PLUS VARIABLE",
 /* 254 */ "expr ::= MINUS VARIABLE",
 /* 255 */ "expr ::= BOOL",
 /* 256 */ "expr ::= NULL",
 /* 257 */ "expr ::= ID LP exprlist RP",
 /* 258 */ "expr ::= ID LP STAR RP",
 /* 259 */ "expr ::= expr IS NULL",
 /* 260 */ "expr ::= expr IS NOT NULL",
 /* 261 */ "expr ::= expr LT expr",
 /* 262 */ "expr ::= expr GT expr",
 /* 263 */ "expr ::= expr LE expr",
 /* 264 */ "expr ::= expr GE expr",
 /* 265 */ "expr ::= expr NE expr",
 /* 266 */ "expr ::= expr EQ expr",
 /* 267 */ "expr ::= expr BETWEEN expr AND expr",
 /* 268 */ "expr ::= expr AND expr",
 /* 269 */ "expr ::= expr OR expr",
 /* 270 */ "expr ::= expr PLUS expr",
 /* 271 */ "expr ::= expr MINUS expr",
 /* 272 */ "expr ::= expr STAR expr",
 /* 273 */ "expr ::= expr SLASH expr",
 /* 274 */ "expr ::= expr REM expr",
 /* 275 */ "expr ::= expr LIKE expr",
 /* 276 */ "expr ::= expr MATCH expr",
 /* 277 */ "expr ::= expr NMATCH expr",
 /* 278 */ "expr ::= expr IN LP exprlist RP",
 /* 279 */ "exprlist ::= exprlist COMMA expritem",
 /* 280 */ "exprlist ::= expritem",
 /* 281 */ "expritem ::= expr",
 /* 282 */ "expritem ::=",
 /* 283 */ "cmd ::= RESET QUERY CACHE",
 /* 284 */ "cmd ::= SYNCDB ids REPLICA",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 290 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 291 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 292 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 298 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 299 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 300 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 301 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 302 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 303 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 200: /* exprlist */
    case 246: /* selcollist */
    case 260: /* sclp */
{
tSqlExprListDestroy((yypminor->yy225));
}
      break;
    case 214: /* intitemlist */
    case 216: /* keep */
    case 238: /* columnlist */
    case 239: /* tagitemlist1 */
    case 240: /* tagNamelist */
    case 244: /* tagitemlist */
    case 253: /* fill_opt */
    case 254: /* groupby_opt */
    case 256: /* orderby_opt */
    case 268: /* sortlist */
    case 272: /* grouplist */
{
taosArrayDestroy((yypminor->yy225));
}
      break;
    case 236: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy482));
}
      break;
    case 241: /* select */
{
destroySqlNode((yypminor->yy185));
}
      break;
    case 247: /* from */
    case 264: /* tablelist */
    case 265: /* sub */
{
destroyRelationInfo((yypminor->yy160));
}
      break;
    case 248: /* where_opt */
    case 255: /* having_opt */
    case 262: /* expr */
    case 273: /* expritem */
{
tSqlExprDestroy((yypminor->yy226));
}
      break;
    case 259: /* union */
{
destroyAllSqlNode((yypminor->yy93));
}
      break;
    case 269: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy1));
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
  {  192,   -1 }, /* (0) program ::= cmd */
  {  193,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  193,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  193,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  193,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  193,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  193,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  193,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  193,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  193,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  193,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  193,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  193,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  193,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  193,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  193,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  193,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  195,    0 }, /* (17) dbPrefix ::= */
  {  195,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  196,    0 }, /* (19) cpxName ::= */
  {  196,   -2 }, /* (20) cpxName ::= DOT ids */
  {  193,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  193,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  193,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  193,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  193,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  193,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  193,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  193,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  193,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  193,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  193,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  193,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  193,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  193,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  193,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  193,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  193,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  193,   -2 }, /* (38) cmd ::= USE ids */
  {  193,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  193,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  193,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  193,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  193,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  193,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  193,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  193,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  193,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  193,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  193,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  193,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  194,   -1 }, /* (51) ids ::= ID */
  {  197,   -2 }, /* (52) ifexists ::= IF EXISTS */
  {  197,    0 }, /* (53) ifexists ::= */
  {  201,   -3 }, /* (54) ifnotexists ::= IF NOT EXISTS */
  {  201,    0 }, /* (55) ifnotexists ::= */
  {  193,   -5 }, /* (56) cmd ::= CREATE DNODE ids PORT ids */
  {  193,   -5 }, /* (57) cmd ::= CREATE DNODE IPTOKEN PORT ids */
  {  193,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  193,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  193,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  204,    0 }, /* (63) bufsize ::= */
  {  204,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  205,    0 }, /* (65) pps ::= */
  {  205,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  206,    0 }, /* (67) tseries ::= */
  {  206,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  207,    0 }, /* (69) dbs ::= */
  {  207,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  208,    0 }, /* (71) streams ::= */
  {  208,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  209,    0 }, /* (73) storage ::= */
  {  209,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  210,    0 }, /* (75) qtime ::= */
  {  210,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  211,    0 }, /* (77) users ::= */
  {  211,   -2 }, /* (78) users ::= USERS INTEGER */
  {  212,    0 }, /* (79) conns ::= */
  {  212,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  213,    0 }, /* (81) state ::= */
  {  213,   -2 }, /* (82) state ::= STATE ids */
  {  199,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  214,   -3 }, /* (84) intitemlist ::= intitemlist COMMA intitem */
  {  214,   -1 }, /* (85) intitemlist ::= intitem */
  {  215,   -1 }, /* (86) intitem ::= INTEGER */
  {  216,   -2 }, /* (87) keep ::= KEEP intitemlist */
  {  217,   -2 }, /* (88) cache ::= CACHE INTEGER */
  {  218,   -2 }, /* (89) replica ::= REPLICA INTEGER */
  {  219,   -2 }, /* (90) quorum ::= QUORUM INTEGER */
  {  220,   -2 }, /* (91) days ::= DAYS INTEGER */
  {  221,   -2 }, /* (92) minrows ::= MINROWS INTEGER */
  {  222,   -2 }, /* (93) maxrows ::= MAXROWS INTEGER */
  {  223,   -2 }, /* (94) blocks ::= BLOCKS INTEGER */
  {  224,   -2 }, /* (95) ctime ::= CTIME INTEGER */
  {  225,   -2 }, /* (96) wal ::= WAL INTEGER */
  {  226,   -2 }, /* (97) fsync ::= FSYNC INTEGER */
  {  227,   -2 }, /* (98) comp ::= COMP INTEGER */
  {  228,   -2 }, /* (99) prec ::= PRECISION STRING */
  {  229,   -2 }, /* (100) update ::= UPDATE INTEGER */
  {  230,   -2 }, /* (101) cachelast ::= CACHELAST INTEGER */
  {  231,   -2 }, /* (102) vgroups ::= VGROUPS INTEGER */
  {  232,   -3 }, /* (103) stream_mode ::= STREAM MODE INTEGER */
  {  202,    0 }, /* (104) db_optr ::= */
  {  202,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  202,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  202,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  202,   -2 }, /* (108) db_optr ::= db_optr days */
  {  202,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  202,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  202,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  202,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  202,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  202,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  202,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  202,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  202,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  202,   -2 }, /* (118) db_optr ::= db_optr update */
  {  202,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  202,   -2 }, /* (120) db_optr ::= db_optr vgroups */
  {  202,   -2 }, /* (121) db_optr ::= db_optr stream_mode */
  {  198,    0 }, /* (122) alter_db_optr ::= */
  {  198,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  198,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  198,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  198,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  198,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  198,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  198,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  203,   -1 }, /* (130) typename ::= ids */
  {  203,   -4 }, /* (131) typename ::= ids LP signed RP */
  {  203,   -2 }, /* (132) typename ::= ids UNSIGNED */
  {  233,   -1 }, /* (133) signed ::= INTEGER */
  {  233,   -2 }, /* (134) signed ::= PLUS INTEGER */
  {  233,   -2 }, /* (135) signed ::= MINUS INTEGER */
  {  193,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_args */
  {  193,   -3 }, /* (137) cmd ::= CREATE TABLE create_stable_args */
  {  193,   -3 }, /* (138) cmd ::= CREATE STABLE create_stable_args */
  {  193,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_list */
  {  236,   -1 }, /* (140) create_table_list ::= create_from_stable */
  {  236,   -2 }, /* (141) create_table_list ::= create_table_list create_from_stable */
  {  234,   -6 }, /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  235,  -10 }, /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  237,  -10 }, /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  237,  -13 }, /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  240,   -3 }, /* (146) tagNamelist ::= tagNamelist COMMA ids */
  {  240,   -1 }, /* (147) tagNamelist ::= ids */
  {  234,   -5 }, /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
  {  238,   -3 }, /* (149) columnlist ::= columnlist COMMA column */
  {  238,   -1 }, /* (150) columnlist ::= column */
  {  242,   -2 }, /* (151) column ::= ids typename */
  {  239,   -3 }, /* (152) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  239,   -1 }, /* (153) tagitemlist1 ::= tagitem1 */
  {  243,   -2 }, /* (154) tagitem1 ::= MINUS INTEGER */
  {  243,   -2 }, /* (155) tagitem1 ::= MINUS FLOAT */
  {  243,   -2 }, /* (156) tagitem1 ::= PLUS INTEGER */
  {  243,   -2 }, /* (157) tagitem1 ::= PLUS FLOAT */
  {  243,   -1 }, /* (158) tagitem1 ::= INTEGER */
  {  243,   -1 }, /* (159) tagitem1 ::= FLOAT */
  {  243,   -1 }, /* (160) tagitem1 ::= STRING */
  {  243,   -1 }, /* (161) tagitem1 ::= BOOL */
  {  243,   -1 }, /* (162) tagitem1 ::= NULL */
  {  243,   -1 }, /* (163) tagitem1 ::= NOW */
  {  244,   -3 }, /* (164) tagitemlist ::= tagitemlist COMMA tagitem */
  {  244,   -1 }, /* (165) tagitemlist ::= tagitem */
  {  245,   -1 }, /* (166) tagitem ::= INTEGER */
  {  245,   -1 }, /* (167) tagitem ::= FLOAT */
  {  245,   -1 }, /* (168) tagitem ::= STRING */
  {  245,   -1 }, /* (169) tagitem ::= BOOL */
  {  245,   -1 }, /* (170) tagitem ::= NULL */
  {  245,   -1 }, /* (171) tagitem ::= NOW */
  {  245,   -2 }, /* (172) tagitem ::= MINUS INTEGER */
  {  245,   -2 }, /* (173) tagitem ::= MINUS FLOAT */
  {  245,   -2 }, /* (174) tagitem ::= PLUS INTEGER */
  {  245,   -2 }, /* (175) tagitem ::= PLUS FLOAT */
  {  241,  -14 }, /* (176) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  241,   -3 }, /* (177) select ::= LP select RP */
  {  259,   -1 }, /* (178) union ::= select */
  {  259,   -4 }, /* (179) union ::= union UNION ALL select */
  {  259,   -3 }, /* (180) union ::= union UNION select */
  {  193,   -1 }, /* (181) cmd ::= union */
  {  241,   -2 }, /* (182) select ::= SELECT selcollist */
  {  260,   -2 }, /* (183) sclp ::= selcollist COMMA */
  {  260,    0 }, /* (184) sclp ::= */
  {  246,   -4 }, /* (185) selcollist ::= sclp distinct expr as */
  {  246,   -2 }, /* (186) selcollist ::= sclp STAR */
  {  263,   -2 }, /* (187) as ::= AS ids */
  {  263,   -1 }, /* (188) as ::= ids */
  {  263,    0 }, /* (189) as ::= */
  {  261,   -1 }, /* (190) distinct ::= DISTINCT */
  {  261,    0 }, /* (191) distinct ::= */
  {  247,   -2 }, /* (192) from ::= FROM tablelist */
  {  247,   -2 }, /* (193) from ::= FROM sub */
  {  265,   -3 }, /* (194) sub ::= LP union RP */
  {  265,   -4 }, /* (195) sub ::= LP union RP ids */
  {  265,   -6 }, /* (196) sub ::= sub COMMA LP union RP ids */
  {  264,   -2 }, /* (197) tablelist ::= ids cpxName */
  {  264,   -3 }, /* (198) tablelist ::= ids cpxName ids */
  {  264,   -4 }, /* (199) tablelist ::= tablelist COMMA ids cpxName */
  {  264,   -5 }, /* (200) tablelist ::= tablelist COMMA ids cpxName ids */
  {  266,   -1 }, /* (201) tmvar ::= VARIABLE */
  {  249,   -4 }, /* (202) interval_option ::= intervalKey LP tmvar RP */
  {  249,   -6 }, /* (203) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  249,    0 }, /* (204) interval_option ::= */
  {  267,   -1 }, /* (205) intervalKey ::= INTERVAL */
  {  267,   -1 }, /* (206) intervalKey ::= EVERY */
  {  251,    0 }, /* (207) session_option ::= */
  {  251,   -7 }, /* (208) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  252,    0 }, /* (209) windowstate_option ::= */
  {  252,   -4 }, /* (210) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  253,    0 }, /* (211) fill_opt ::= */
  {  253,   -6 }, /* (212) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  253,   -4 }, /* (213) fill_opt ::= FILL LP ID RP */
  {  250,   -4 }, /* (214) sliding_opt ::= SLIDING LP tmvar RP */
  {  250,    0 }, /* (215) sliding_opt ::= */
  {  256,    0 }, /* (216) orderby_opt ::= */
  {  256,   -3 }, /* (217) orderby_opt ::= ORDER BY sortlist */
  {  268,   -4 }, /* (218) sortlist ::= sortlist COMMA item sortorder */
  {  268,   -2 }, /* (219) sortlist ::= item sortorder */
  {  270,   -2 }, /* (220) item ::= ids cpxName */
  {  271,   -1 }, /* (221) sortorder ::= ASC */
  {  271,   -1 }, /* (222) sortorder ::= DESC */
  {  271,    0 }, /* (223) sortorder ::= */
  {  254,    0 }, /* (224) groupby_opt ::= */
  {  254,   -3 }, /* (225) groupby_opt ::= GROUP BY grouplist */
  {  272,   -3 }, /* (226) grouplist ::= grouplist COMMA item */
  {  272,   -1 }, /* (227) grouplist ::= item */
  {  255,    0 }, /* (228) having_opt ::= */
  {  255,   -2 }, /* (229) having_opt ::= HAVING expr */
  {  258,    0 }, /* (230) limit_opt ::= */
  {  258,   -2 }, /* (231) limit_opt ::= LIMIT signed */
  {  258,   -4 }, /* (232) limit_opt ::= LIMIT signed OFFSET signed */
  {  258,   -4 }, /* (233) limit_opt ::= LIMIT signed COMMA signed */
  {  257,    0 }, /* (234) slimit_opt ::= */
  {  257,   -2 }, /* (235) slimit_opt ::= SLIMIT signed */
  {  257,   -4 }, /* (236) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  257,   -4 }, /* (237) slimit_opt ::= SLIMIT signed COMMA signed */
  {  248,    0 }, /* (238) where_opt ::= */
  {  248,   -2 }, /* (239) where_opt ::= WHERE expr */
  {  262,   -3 }, /* (240) expr ::= LP expr RP */
  {  262,   -1 }, /* (241) expr ::= ID */
  {  262,   -3 }, /* (242) expr ::= ID DOT ID */
  {  262,   -3 }, /* (243) expr ::= ID DOT STAR */
  {  262,   -1 }, /* (244) expr ::= INTEGER */
  {  262,   -2 }, /* (245) expr ::= MINUS INTEGER */
  {  262,   -2 }, /* (246) expr ::= PLUS INTEGER */
  {  262,   -1 }, /* (247) expr ::= FLOAT */
  {  262,   -2 }, /* (248) expr ::= MINUS FLOAT */
  {  262,   -2 }, /* (249) expr ::= PLUS FLOAT */
  {  262,   -1 }, /* (250) expr ::= STRING */
  {  262,   -1 }, /* (251) expr ::= NOW */
  {  262,   -1 }, /* (252) expr ::= VARIABLE */
  {  262,   -2 }, /* (253) expr ::= PLUS VARIABLE */
  {  262,   -2 }, /* (254) expr ::= MINUS VARIABLE */
  {  262,   -1 }, /* (255) expr ::= BOOL */
  {  262,   -1 }, /* (256) expr ::= NULL */
  {  262,   -4 }, /* (257) expr ::= ID LP exprlist RP */
  {  262,   -4 }, /* (258) expr ::= ID LP STAR RP */
  {  262,   -3 }, /* (259) expr ::= expr IS NULL */
  {  262,   -4 }, /* (260) expr ::= expr IS NOT NULL */
  {  262,   -3 }, /* (261) expr ::= expr LT expr */
  {  262,   -3 }, /* (262) expr ::= expr GT expr */
  {  262,   -3 }, /* (263) expr ::= expr LE expr */
  {  262,   -3 }, /* (264) expr ::= expr GE expr */
  {  262,   -3 }, /* (265) expr ::= expr NE expr */
  {  262,   -3 }, /* (266) expr ::= expr EQ expr */
  {  262,   -5 }, /* (267) expr ::= expr BETWEEN expr AND expr */
  {  262,   -3 }, /* (268) expr ::= expr AND expr */
  {  262,   -3 }, /* (269) expr ::= expr OR expr */
  {  262,   -3 }, /* (270) expr ::= expr PLUS expr */
  {  262,   -3 }, /* (271) expr ::= expr MINUS expr */
  {  262,   -3 }, /* (272) expr ::= expr STAR expr */
  {  262,   -3 }, /* (273) expr ::= expr SLASH expr */
  {  262,   -3 }, /* (274) expr ::= expr REM expr */
  {  262,   -3 }, /* (275) expr ::= expr LIKE expr */
  {  262,   -3 }, /* (276) expr ::= expr MATCH expr */
  {  262,   -3 }, /* (277) expr ::= expr NMATCH expr */
  {  262,   -5 }, /* (278) expr ::= expr IN LP exprlist RP */
  {  200,   -3 }, /* (279) exprlist ::= exprlist COMMA expritem */
  {  200,   -1 }, /* (280) exprlist ::= expritem */
  {  273,   -1 }, /* (281) expritem ::= expr */
  {  273,    0 }, /* (282) expritem ::= */
  {  193,   -3 }, /* (283) cmd ::= RESET QUERY CACHE */
  {  193,   -3 }, /* (284) cmd ::= SYNCDB ids REPLICA */
  {  193,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (286) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (287) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (288) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (289) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (290) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (291) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (292) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (294) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (295) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (296) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (297) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (298) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (299) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (300) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -3 }, /* (301) cmd ::= KILL CONNECTION INTEGER */
  {  193,   -5 }, /* (302) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  193,   -5 }, /* (303) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 136: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==136);
      case 137: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==137);
      case 138: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==138);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNC, 0, 0);}
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
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TRANS, 0, 0);   }
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
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy326, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy523);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy523);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy225);}
        break;
      case 51: /* ids ::= ID */
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 52: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 53: /* ifexists ::= */
      case 55: /* ifnotexists ::= */ yytestcase(yyruleno==55);
      case 191: /* distinct ::= */ yytestcase(yyruleno==191);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 54: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 56: /* cmd ::= CREATE DNODE ids PORT ids */
      case 57: /* cmd ::= CREATE DNODE IPTOKEN PORT ids */ yytestcase(yyruleno==57);
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy523);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy326, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy16, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy16, &yymsp[0].minor.yy0, 2);}
        break;
      case 62: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 63: /* bufsize ::= */
      case 65: /* pps ::= */ yytestcase(yyruleno==65);
      case 67: /* tseries ::= */ yytestcase(yyruleno==67);
      case 69: /* dbs ::= */ yytestcase(yyruleno==69);
      case 71: /* streams ::= */ yytestcase(yyruleno==71);
      case 73: /* storage ::= */ yytestcase(yyruleno==73);
      case 75: /* qtime ::= */ yytestcase(yyruleno==75);
      case 77: /* users ::= */ yytestcase(yyruleno==77);
      case 79: /* conns ::= */ yytestcase(yyruleno==79);
      case 81: /* state ::= */ yytestcase(yyruleno==81);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 64: /* bufsize ::= BUFSIZE INTEGER */
      case 66: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==70);
      case 72: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==76);
      case 78: /* users ::= USERS INTEGER */ yytestcase(yyruleno==78);
      case 80: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==80);
      case 82: /* state ::= STATE ids */ yytestcase(yyruleno==82);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 83: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy523.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy523.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy523.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy523.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy523.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy523.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy523.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy523.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy523.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy523 = yylhsminor.yy523;
        break;
      case 84: /* intitemlist ::= intitemlist COMMA intitem */
      case 164: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==164);
{ yylhsminor.yy225 = tListItemAppend(yymsp[-2].minor.yy225, &yymsp[0].minor.yy1, -1);    }
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 85: /* intitemlist ::= intitem */
      case 165: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==165);
{ yylhsminor.yy225 = tListItemAppend(NULL, &yymsp[0].minor.yy1, -1); }
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 86: /* intitem ::= INTEGER */
      case 166: /* tagitem ::= INTEGER */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= FLOAT */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= STRING */ yytestcase(yyruleno==168);
      case 169: /* tagitem ::= BOOL */ yytestcase(yyruleno==169);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy1, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy1 = yylhsminor.yy1;
        break;
      case 87: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy225 = yymsp[0].minor.yy225; }
        break;
      case 88: /* cache ::= CACHE INTEGER */
      case 89: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==89);
      case 90: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==90);
      case 91: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==91);
      case 92: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==95);
      case 96: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==96);
      case 97: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==97);
      case 98: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==98);
      case 99: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==99);
      case 100: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==100);
      case 101: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==101);
      case 102: /* vgroups ::= VGROUPS INTEGER */ yytestcase(yyruleno==102);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 103: /* stream_mode ::= STREAM MODE INTEGER */
{ yymsp[-2].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 104: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy326);}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.keep = yymsp[0].minor.yy225; }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 120: /* db_optr ::= db_optr vgroups */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.numOfVgroups = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 121: /* db_optr ::= db_optr stream_mode */
{ yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326.streamMode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy326);}
        break;
      case 130: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy16, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy16 = yylhsminor.yy16;
        break;
      case 131: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy61 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy16, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy61;  // negative value of name length
    tSetColumnType(&yylhsminor.yy16, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy16 = yylhsminor.yy16;
        break;
      case 132: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy16, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 133: /* signed ::= INTEGER */
{ yylhsminor.yy61 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy61 = yylhsminor.yy61;
        break;
      case 134: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy61 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 135: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy61 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 139: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy482;}
        break;
      case 140: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy184);
  pCreateTable->type = TSDB_SQL_CREATE_TABLE;
  yylhsminor.yy482 = pCreateTable;
}
  yymsp[0].minor.yy482 = yylhsminor.yy482;
        break;
      case 141: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy482->childTableInfo, &yymsp[0].minor.yy184);
  yylhsminor.yy482 = yymsp[-1].minor.yy482;
}
  yymsp[-1].minor.yy482 = yylhsminor.yy482;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy482 = tSetCreateTableInfo(yymsp[-1].minor.yy225, NULL, NULL, TSDB_SQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy482 = yylhsminor.yy482;
        break;
      case 143: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy482 = tSetCreateTableInfo(yymsp[-5].minor.yy225, yymsp[-1].minor.yy225, NULL, TSDB_SQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_STABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy482 = yylhsminor.yy482;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy184 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy225, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy184 = yylhsminor.yy184;
        break;
      case 145: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy184 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy225, yymsp[-1].minor.yy225, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy184 = yylhsminor.yy184;
        break;
      case 146: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy225, &yymsp[0].minor.yy0); yylhsminor.yy225 = yymsp[-2].minor.yy225;  }
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 147: /* tagNamelist ::= ids */
{yylhsminor.yy225 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy225, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 148: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
//  yylhsminor.yy482 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy185, TSQL_CREATE_STREAM);
//  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_TABLE);
//
//  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
//  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy482 = yylhsminor.yy482;
        break;
      case 149: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy225, &yymsp[0].minor.yy16); yylhsminor.yy225 = yymsp[-2].minor.yy225;  }
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 150: /* columnlist ::= column */
{yylhsminor.yy225 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy225, &yymsp[0].minor.yy16);}
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 151: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy16, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy16);
}
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 152: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy225, &yymsp[0].minor.yy0); yylhsminor.yy225 = yymsp[-2].minor.yy225;}
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 153: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy225 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy225, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 154: /* tagitem1 ::= MINUS INTEGER */
      case 155: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==157);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 158: /* tagitem1 ::= INTEGER */
      case 159: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==159);
      case 160: /* tagitem1 ::= STRING */ yytestcase(yyruleno==160);
      case 161: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==161);
      case 162: /* tagitem1 ::= NULL */ yytestcase(yyruleno==162);
      case 163: /* tagitem1 ::= NOW */ yytestcase(yyruleno==163);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 170: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy1, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy1 = yylhsminor.yy1;
        break;
      case 171: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy1, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy1 = yylhsminor.yy1;
        break;
      case 172: /* tagitem ::= MINUS INTEGER */
      case 173: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==173);
      case 174: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==174);
      case 175: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==175);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy1, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy1 = yylhsminor.yy1;
        break;
      case 176: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy185 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy225, yymsp[-11].minor.yy160, yymsp[-10].minor.yy226, yymsp[-4].minor.yy225, yymsp[-2].minor.yy225, &yymsp[-9].minor.yy52, &yymsp[-7].minor.yy463, &yymsp[-6].minor.yy112, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy225, &yymsp[0].minor.yy495, &yymsp[-1].minor.yy495, yymsp[-3].minor.yy226);
}
  yymsp[-13].minor.yy185 = yylhsminor.yy185;
        break;
      case 177: /* select ::= LP select RP */
{yymsp[-2].minor.yy185 = yymsp[-1].minor.yy185;}
        break;
      case 178: /* union ::= select */
{ yylhsminor.yy93 = setSubclause(NULL, yymsp[0].minor.yy185); }
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 179: /* union ::= union UNION ALL select */
{ yylhsminor.yy93 = appendSelectClause(yymsp[-3].minor.yy93, SQL_TYPE_UNIONALL, yymsp[0].minor.yy185);  }
  yymsp[-3].minor.yy93 = yylhsminor.yy93;
        break;
      case 180: /* union ::= union UNION select */
{ yylhsminor.yy93 = appendSelectClause(yymsp[-2].minor.yy93, SQL_TYPE_UNION, yymsp[0].minor.yy185);  }
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 181: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy93, NULL, TSDB_SQL_SELECT); }
        break;
      case 182: /* select ::= SELECT selcollist */
{
  yylhsminor.yy185 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy225, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy185 = yylhsminor.yy185;
        break;
      case 183: /* sclp ::= selcollist COMMA */
{yylhsminor.yy225 = yymsp[-1].minor.yy225;}
  yymsp[-1].minor.yy225 = yylhsminor.yy225;
        break;
      case 184: /* sclp ::= */
      case 216: /* orderby_opt ::= */ yytestcase(yyruleno==216);
{yymsp[1].minor.yy225 = 0;}
        break;
      case 185: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy225 = tSqlExprListAppend(yymsp[-3].minor.yy225, yymsp[-1].minor.yy226,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy225 = yylhsminor.yy225;
        break;
      case 186: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy225 = tSqlExprListAppend(yymsp[-1].minor.yy225, pNode, 0, 0);
}
  yymsp[-1].minor.yy225 = yylhsminor.yy225;
        break;
      case 187: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 188: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 189: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 190: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* from ::= FROM tablelist */
      case 193: /* from ::= FROM sub */ yytestcase(yyruleno==193);
{yymsp[-1].minor.yy160 = yymsp[0].minor.yy160;}
        break;
      case 194: /* sub ::= LP union RP */
{yymsp[-2].minor.yy160 = addSubquery(NULL, yymsp[-1].minor.yy93, NULL);}
        break;
      case 195: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy160 = addSubquery(NULL, yymsp[-2].minor.yy93, &yymsp[0].minor.yy0);}
        break;
      case 196: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy160 = addSubquery(yymsp[-5].minor.yy160, yymsp[-2].minor.yy93, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy160 = yylhsminor.yy160;
        break;
      case 197: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy160 = yylhsminor.yy160;
        break;
      case 198: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy160 = yylhsminor.yy160;
        break;
      case 199: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(yymsp[-3].minor.yy160, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy160 = yylhsminor.yy160;
        break;
      case 200: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(yymsp[-4].minor.yy160, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy160 = yylhsminor.yy160;
        break;
      case 201: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 202: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy52.interval = yymsp[-1].minor.yy0; yylhsminor.yy52.offset.n = 0; yylhsminor.yy52.token = yymsp[-3].minor.yy460;}
  yymsp[-3].minor.yy52 = yylhsminor.yy52;
        break;
      case 203: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy52.interval = yymsp[-3].minor.yy0; yylhsminor.yy52.offset = yymsp[-1].minor.yy0;   yylhsminor.yy52.token = yymsp[-5].minor.yy460;}
  yymsp[-5].minor.yy52 = yylhsminor.yy52;
        break;
      case 204: /* interval_option ::= */
{memset(&yymsp[1].minor.yy52, 0, sizeof(yymsp[1].minor.yy52));}
        break;
      case 205: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy460 = TK_INTERVAL;}
        break;
      case 206: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy460 = TK_EVERY;   }
        break;
      case 207: /* session_option ::= */
{yymsp[1].minor.yy463.col.n = 0; yymsp[1].minor.yy463.gap.n = 0;}
        break;
      case 208: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy463.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy463.gap = yymsp[-1].minor.yy0;
}
        break;
      case 209: /* windowstate_option ::= */
{ yymsp[1].minor.yy112.col.n = 0; yymsp[1].minor.yy112.col.z = NULL;}
        break;
      case 210: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy112.col = yymsp[-1].minor.yy0; }
        break;
      case 211: /* fill_opt ::= */
{ yymsp[1].minor.yy225 = 0;     }
        break;
      case 212: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy225, &A, -1, 0);
    yymsp[-5].minor.yy225 = yymsp[-1].minor.yy225;
}
        break;
      case 213: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy225 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 214: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 215: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 217: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy225 = yymsp[0].minor.yy225;}
        break;
      case 218: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy225 = tListItemAppend(yymsp[-3].minor.yy225, &yymsp[-1].minor.yy1, yymsp[0].minor.yy40);
}
  yymsp[-3].minor.yy225 = yylhsminor.yy225;
        break;
      case 219: /* sortlist ::= item sortorder */
{
  yylhsminor.yy225 = tListItemAppend(NULL, &yymsp[-1].minor.yy1, yymsp[0].minor.yy40);
}
  yymsp[-1].minor.yy225 = yylhsminor.yy225;
        break;
      case 220: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy1, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy1 = yylhsminor.yy1;
        break;
      case 221: /* sortorder ::= ASC */
{ yymsp[0].minor.yy40 = TSDB_ORDER_ASC; }
        break;
      case 222: /* sortorder ::= DESC */
{ yymsp[0].minor.yy40 = TSDB_ORDER_DESC;}
        break;
      case 223: /* sortorder ::= */
{ yymsp[1].minor.yy40 = TSDB_ORDER_ASC; }
        break;
      case 224: /* groupby_opt ::= */
{ yymsp[1].minor.yy225 = 0;}
        break;
      case 225: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy225 = yymsp[0].minor.yy225;}
        break;
      case 226: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy225 = tListItemAppend(yymsp[-2].minor.yy225, &yymsp[0].minor.yy1, -1);
}
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 227: /* grouplist ::= item */
{
  yylhsminor.yy225 = tListItemAppend(NULL, &yymsp[0].minor.yy1, -1);
}
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 228: /* having_opt ::= */
      case 238: /* where_opt ::= */ yytestcase(yyruleno==238);
      case 282: /* expritem ::= */ yytestcase(yyruleno==282);
{yymsp[1].minor.yy226 = 0;}
        break;
      case 229: /* having_opt ::= HAVING expr */
      case 239: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==239);
{yymsp[-1].minor.yy226 = yymsp[0].minor.yy226;}
        break;
      case 230: /* limit_opt ::= */
      case 234: /* slimit_opt ::= */ yytestcase(yyruleno==234);
{yymsp[1].minor.yy495.limit = -1; yymsp[1].minor.yy495.offset = 0;}
        break;
      case 231: /* limit_opt ::= LIMIT signed */
      case 235: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==235);
{yymsp[-1].minor.yy495.limit = yymsp[0].minor.yy61;  yymsp[-1].minor.yy495.offset = 0;}
        break;
      case 232: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy495.limit = yymsp[-2].minor.yy61;  yymsp[-3].minor.yy495.offset = yymsp[0].minor.yy61;}
        break;
      case 233: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy495.limit = yymsp[0].minor.yy61;  yymsp[-3].minor.yy495.offset = yymsp[-2].minor.yy61;}
        break;
      case 236: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy495.limit = yymsp[-2].minor.yy61;  yymsp[-3].minor.yy495.offset = yymsp[0].minor.yy61;}
        break;
      case 237: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy495.limit = yymsp[0].minor.yy61;  yymsp[-3].minor.yy495.offset = yymsp[-2].minor.yy61;}
        break;
      case 240: /* expr ::= LP expr RP */
{yylhsminor.yy226 = yymsp[-1].minor.yy226; yylhsminor.yy226->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy226->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 241: /* expr ::= ID */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 242: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 243: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 244: /* expr ::= INTEGER */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 245: /* expr ::= MINUS INTEGER */
      case 246: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==246);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 247: /* expr ::= FLOAT */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 248: /* expr ::= MINUS FLOAT */
      case 249: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==249);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 250: /* expr ::= STRING */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 251: /* expr ::= NOW */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 252: /* expr ::= VARIABLE */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 253: /* expr ::= PLUS VARIABLE */
      case 254: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==254);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 255: /* expr ::= BOOL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 256: /* expr ::= NULL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 257: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(yymsp[-1].minor.yy225, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 258: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 259: /* expr ::= expr IS NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 260: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-3].minor.yy226, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 261: /* expr ::= expr LT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 262: /* expr ::= expr GT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 263: /* expr ::= expr LE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 264: /* expr ::= expr GE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 265: /* expr ::= expr NE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_NE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 266: /* expr ::= expr EQ expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_EQ);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 267: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy226); yylhsminor.yy226 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy226, yymsp[-2].minor.yy226, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy226, TK_LE), TK_AND);}
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 268: /* expr ::= expr AND expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_AND);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 269: /* expr ::= expr OR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_OR); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 270: /* expr ::= expr PLUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_PLUS);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 271: /* expr ::= expr MINUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_MINUS); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 272: /* expr ::= expr STAR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_STAR);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 273: /* expr ::= expr SLASH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_DIVIDE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 274: /* expr ::= expr REM expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_REM);   }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 275: /* expr ::= expr LIKE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LIKE);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 276: /* expr ::= expr MATCH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_MATCH);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 277: /* expr ::= expr NMATCH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_NMATCH);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 278: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-4].minor.yy226, (tSqlExpr*)yymsp[-1].minor.yy225, TK_IN); }
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 279: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy225 = tSqlExprListAppend(yymsp[-2].minor.yy225,yymsp[0].minor.yy226,0, 0);}
  yymsp[-2].minor.yy225 = yylhsminor.yy225;
        break;
      case 280: /* exprlist ::= expritem */
{yylhsminor.yy225 = tSqlExprListAppend(0,yymsp[0].minor.yy226,0, 0);}
  yymsp[0].minor.yy225 = yylhsminor.yy225;
        break;
      case 281: /* expritem ::= expr */
{yylhsminor.yy226 = yymsp[0].minor.yy226;}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 283: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 284: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_ADD_TAG, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy1, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_ADD_TAG, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy1, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 300: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy225, NULL, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 301: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 302: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 303: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
