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
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SWindowStateVal yy6;
  SRelationInfo* yy10;
  SCreateDbInfo yy16;
  int32_t yy46;
  int yy47;
  SSessionWindowVal yy97;
  SField yy106;
  SCreatedTableInfo yy150;
  SArray* yy165;
  tSqlExpr* yy202;
  int64_t yy207;
  SCreateAcctInfo yy211;
  SSqlNode* yy278;
  SCreateTableSql* yy326;
  SLimit yy367;
  SVariant yy425;
  SSubclause* yy503;
  SIntervalVal yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             366
#define YYNRULE              303
#define YYNTOKEN             191
#define YY_MAX_SHIFT         365
#define YY_MIN_SHIFTREDUCE   587
#define YY_MAX_SHIFTREDUCE   889
#define YY_ERROR_ACTION      890
#define YY_ACCEPT_ACTION     891
#define YY_NO_ACTION         892
#define YY_MIN_REDUCE        893
#define YY_MAX_REDUCE        1195
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
#define YY_ACTTAB_COUNT (782)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   249,  638,  364,  230,  162,  639,  248,   55,   56,  638,
 /*    10 */    59,   60, 1031,  639,  252,   49,   48,   47,   94,   58,
 /*    20 */   323,   63,   61,   64,   62,  674, 1080,  891,  365,   54,
 /*    30 */    53,  206,   82,   52,   51,   50,  253,   55,   56,  638,
 /*    40 */    59,   60, 1171,  639,  252,   49,   48,   47,   21,   58,
 /*    50 */   323,   63,   61,   64,   62, 1017,  203, 1015, 1016,   54,
 /*    60 */    53,  206, 1018,   52,   51,   50, 1019,  206, 1020, 1021,
 /*    70 */   832,  835, 1172,   55,   56, 1070,   59,   60, 1172, 1118,
 /*    80 */   252,   49,   48,   47,   81,   58,  323,   63,   61,   64,
 /*    90 */    62,  321,  319,  274, 1077,   54,   53,  206,  638,   52,
 /*   100 */    51,   50,  639,   55,   57,  353,   59,   60, 1172,  826,
 /*   110 */   252,   49,   48,   47,  155,   58,  323,   63,   61,   64,
 /*   120 */    62,   42,  319,  359,  358,   54,   53,  162,  357,   52,
 /*   130 */    51,   50,  356,   87,  355,  354,  162,  162,  588,  589,
 /*   140 */   590,  591,  592,  593,  594,  595,  596,  597,  598,  599,
 /*   150 */   600,  601,  153,   56,  231,   59,   60,  236, 1057,  252,
 /*   160 */    49,   48,   47,   87,   58,  323,   63,   61,   64,   62,
 /*   170 */   772,   43,   32,   86,   54,   53,  255,  204,   52,   51,
 /*   180 */    50,  309,  209,  280,  279,   59,   60,  941,  839,  252,
 /*   190 */    49,   48,   47,  188,   58,  323,   63,   61,   64,   62,
 /*   200 */   294,   43,   92,  242,   54,   53,  300, 1045,   52,   51,
 /*   210 */    50, 1119,   93,  292,   42,  317,  359,  358,  316,  315,
 /*   220 */   314,  357,  313,  312,  311,  356,  310,  355,  354,   99,
 /*   230 */    22, 1010,  998,  999, 1000, 1001, 1002, 1003, 1004, 1005,
 /*   240 */  1006, 1007, 1008, 1009, 1011, 1012, 1013,  215,  251,  841,
 /*   250 */   830,  833,  836,  256,  216,  254,  776,  331,  330,  250,
 /*   260 */   137,  136,  135,  217, 1033,  266,  719,  328,   87,  251,
 /*   270 */   841,  830,  833,  836,  270,  269,  228,  229,  831,  834,
 /*   280 */   324,    4,   63,   61,   64,   62,  755,  752,  753,  754,
 /*   290 */    54,   53,  343,  342,   52,   51,   50,  228,  229,  747,
 /*   300 */   744,  745,  746,   52,   51,   50,   43,    3,   39,  178,
 /*   310 */   152,  150,  149,  257,  258,  105,   77,  101,  108, 1039,
 /*   320 */    96,  791,  792,  260,   36,   65,  244,  245,  273,   36,
 /*   330 */    79,  197,  195,  193,   36,  305,  210,  224,  192,  141,
 /*   340 */   140,  139,  138,   36,   36,   36,   65,  122,  116,  126,
 /*   350 */    36,  211, 1028, 1029,   33, 1032,  131,  134,  125,  638,
 /*   360 */    36,  842,  837,  639,  243,  128,   36,  232,  838,   54,
 /*   370 */    53, 1042,  240,   52,   51,   50, 1042,  241,   36,   36,
 /*   380 */    36, 1042,  842,  837, 1166,  261,  332,  333,  334,  838,
 /*   390 */  1042, 1042, 1042,  335,  176,   12,  840, 1042,  756,  757,
 /*   400 */   262,   95,  259,  339,  338,  337,  171, 1042,  363,  362,
 /*   410 */   146,  748,  749, 1041,  246,   80,  808, 1070, 1045, 1070,
 /*   420 */   261,  340,  341,  345,  282, 1042, 1042, 1042,  124,  177,
 /*   430 */    98,  261,  952,  942,  769,  233,   27,  234,  188,  188,
 /*   440 */  1043,  275,  353,  360,  979,   84,   85,  788, 1030,  798,
 /*   450 */   799,   71,   74,  729,  297,  731,  299,   37,  325,    7,
 /*   460 */   742,  743,  730,  157,  828,   66,   24,  740,  741,   37,
 /*   470 */    37,   67,   97,  864,  807,  760,  761,  843,   67,  637,
 /*   480 */    14,   78,   13,   70,   70,  115, 1165,  114,   16,   23,
 /*   490 */    15,   75,   72,   23, 1111,   23,  829,  758,  759,  133,
 /*   500 */   132,   18,  121,   17,  120,   20, 1164,   19, 1129,  226,
 /*   510 */   322,  227,  207, 1056,  718,  208,  212,  205,  213,  214,
 /*   520 */  1044,  219, 1072,  220, 1191,  221,  218,  202, 1128, 1183,
 /*   530 */   238, 1125, 1124,  239,  344,  271,  154,   44,  172, 1079,
 /*   540 */   151, 1110, 1090, 1087, 1088, 1071,  277, 1092, 1040,  156,
 /*   550 */  1068,  281,  235,  283,   31,  161,  285,  165,  288,  173,
 /*   560 */   163,  168,  787,  845,  164,  276, 1038,  174,  166,  169,
 /*   570 */   167,  291,  175,  956,  302,  303,  304,   76,  307,  308,
 /*   580 */   200,   40,  320,  951,  950,  329, 1190,  112,   73, 1189,
 /*   590 */  1186,  179,   46,  336,  295, 1182,  118,  287, 1181, 1178,
 /*   600 */   180,  976,   41,   38,  201,  939,  127,  937,  129,  130,
 /*   610 */   935,  934,  263,  190,  191,  931,  930,  929,  928,  293,
 /*   620 */   927,  926,  925,  194,  196,  922,  920,  918,  916,  198,
 /*   630 */   913,  199,  909,  289,  284,   83,   88,   45,  286, 1112,
 /*   640 */   306,  123,  346,  225,  247,  347,  301,  348,  349,  351,
 /*   650 */   222,  223,  350,  352,  361,  955,  954,  106,  889,  264,
 /*   660 */   265,  888,  267,  268,  887,  870,  933,  869,  272,  183,
 /*   670 */   182,  977,  932,  181,  142,  184,  185,  187,  186,  143,
 /*   680 */   144,  924,  978,  923,    2,   70,  145,  915,  914,  296,
 /*   690 */     8,   28,    1,  763,  278,  170,  789,   89,  158,  160,
 /*   700 */   800,  159,  237,  794,   90,   29,  796,   91,  290,    9,
 /*   710 */    30,   10,   11,   25,   26,  298,  100,   34,   98,  103,
 /*   720 */   652,  102,  690,   35,  104,  687,  685,  684,  683,  681,
 /*   730 */   680,  679,  676,  642,  326,  327,  107,  111,  109,  318,
 /*   740 */   113,  110,    5,   68,  844,  846,    6,   69,  721,   37,
 /*   750 */   117,  119,  720,  717,  668,  666,  658,  664,  660,  662,
 /*   760 */   656,  654,  689,  688,  686,  682,  678,  677,  189,  640,
 /*   770 */   893,  605,  892,  892,  892,  892,  892,  892,  892,  892,
 /*   780 */   147,  148,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   200,    1,  194,  195,  194,    5,  200,    7,    8,    1,
 /*    10 */    10,   11,    0,    5,   14,   15,   16,   17,  245,   19,
 /*    20 */    20,   21,   22,   23,   24,    3,  194,  192,  193,   29,
 /*    30 */    30,  261,  259,   33,   34,   35,  200,    7,    8,    1,
 /*    40 */    10,   11,  272,    5,   14,   15,   16,   17,  261,   19,
 /*    50 */    20,   21,   22,   23,   24,  216,  261,  218,  219,   29,
 /*    60 */    30,  261,  223,   33,   34,   35,  227,  261,  229,  230,
 /*    70 */     3,    4,  272,    7,    8,  240,   10,   11,  272,  269,
 /*    80 */    14,   15,   16,   17,   84,   19,   20,   21,   22,   23,
 /*    90 */    24,   83,   80,  258,  262,   29,   30,  261,    1,   33,
 /*   100 */    34,   35,    5,    7,    8,   88,   10,   11,  272,   79,
 /*   110 */    14,   15,   16,   17,  194,   19,   20,   21,   22,   23,
 /*   120 */    24,   96,   80,   98,   99,   29,   30,  194,  103,   33,
 /*   130 */    34,   35,  107,   78,  109,  110,  194,  194,   41,   42,
 /*   140 */    43,   44,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   150 */    53,   54,   55,    8,   57,   10,   11,  243,  244,   14,
 /*   160 */    15,   16,   17,   78,   19,   20,   21,   22,   23,   24,
 /*   170 */    33,  116,   78,  118,   29,   30,   64,  261,   33,   34,
 /*   180 */    35,   63,  261,  263,  264,   10,   11,  199,  121,   14,
 /*   190 */    15,   16,   17,  205,   19,   20,   21,   22,   23,   24,
 /*   200 */   267,  116,  269,  238,   29,   30,  112,  242,   33,   34,
 /*   210 */    35,  269,  269,  271,   96,   97,   98,   99,  100,  101,
 /*   220 */   102,  103,  104,  105,  106,  107,  108,  109,  110,  201,
 /*   230 */    40,  216,  217,  218,  219,  220,  221,  222,  223,  224,
 /*   240 */   225,  226,  227,  228,  229,  230,  231,   57,    1,    2,
 /*   250 */     3,    4,    5,  141,   64,  143,  119,  145,  146,   56,
 /*   260 */    70,   71,   72,   73,  236,  139,    3,   77,   78,    1,
 /*   270 */     2,    3,    4,    5,  148,  149,   29,   30,    3,    4,
 /*   280 */    33,   78,   21,   22,   23,   24,    2,    3,    4,    5,
 /*   290 */    29,   30,   29,   30,   33,   34,   35,   29,   30,    2,
 /*   300 */     3,    4,    5,   33,   34,   35,  116,   58,   59,   60,
 /*   310 */    58,   59,   60,   29,   30,   66,   67,   68,   69,  194,
 /*   320 */   201,  122,  123,   64,  194,   78,   29,   30,  138,  194,
 /*   330 */   140,   58,   59,   60,  194,   86,  261,  147,   65,   66,
 /*   340 */    67,   68,   69,  194,  194,  194,   78,   58,   59,   60,
 /*   350 */   194,  261,  233,  234,  235,  236,   67,   68,   69,    1,
 /*   360 */   194,  114,  115,    5,  239,   76,  194,  237,  121,   29,
 /*   370 */    30,  241,  237,   33,   34,   35,  241,  237,  194,  194,
 /*   380 */   194,  241,  114,  115,  261,  194,  237,  237,  237,  121,
 /*   390 */   241,  241,  241,  237,  203,   78,  121,  241,  114,  115,
 /*   400 */   141,   84,  143,  237,  145,  146,  248,  241,   61,   62,
 /*   410 */    63,  114,  115,  241,  238,  201,   72,  240,  242,  240,
 /*   420 */   194,  237,  237,  237,  266,  241,  241,  241,   74,  203,
 /*   430 */   113,  194,  199,  199,   95,  258,   78,  258,  205,  205,
 /*   440 */   203,   79,   88,  214,  215,   79,   79,   79,  234,   79,
 /*   450 */    79,   95,   95,   79,   79,   79,   79,   95,    9,  120,
 /*   460 */     3,    4,   79,   95,    1,   95,   95,    3,    4,   95,
 /*   470 */    95,   95,   95,   79,  130,    3,    4,   79,   95,   79,
 /*   480 */   142,   78,  144,  117,  117,  142,  261,  144,  142,   95,
 /*   490 */   144,  134,  136,   95,  270,   95,   33,    3,    4,   74,
 /*   500 */    75,  142,  142,  144,  144,  142,  261,  144,  232,  261,
 /*   510 */   194,  261,  261,  244,  111,  261,  261,  261,  261,  261,
 /*   520 */   242,  261,  240,  261,  244,  261,  261,  261,  232,  244,
 /*   530 */   232,  232,  232,  232,  232,  194,  194,  260,  246,  194,
 /*   540 */    56,  270,  194,  194,  194,  240,  240,  194,  240,  194,
 /*   550 */   257,  265,  265,  265,  247,  194,  265,  254,  194,  194,
 /*   560 */   256,  251,  121,  114,  255,  196,  194,  194,  253,  250,
 /*   570 */   252,  126,  194,  194,  194,  194,  194,  133,  194,  194,
 /*   580 */   194,  194,  194,  194,  194,  194,  194,  194,  135,  194,
 /*   590 */   194,  194,  132,  194,  128,  194,  194,  124,  194,  194,
 /*   600 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   610 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  131,
 /*   620 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   630 */   194,  194,  194,  125,  127,  196,  196,  137,  196,  196,
 /*   640 */    87,   94,   93,  196,  196,   47,  196,   90,   92,   91,
 /*   650 */   196,  196,   51,   89,   80,  204,  204,  201,    3,  150,
 /*   660 */     3,    3,  150,    3,    3,   98,  196,   97,  139,  207,
 /*   670 */   211,  213,  196,  212,  197,  210,  208,  206,  209,  197,
 /*   680 */   197,  196,  215,  196,  198,  117,  197,  196,  196,  112,
 /*   690 */    78,   78,  202,   79,   95,  249,   79,   95,   78,   95,
 /*   700 */    79,   78,    1,   79,   78,   95,   79,   78,   78,  129,
 /*   710 */    95,  129,   78,   78,   78,  112,   74,   85,  113,   66,
 /*   720 */     3,   84,    3,   85,   84,    5,    3,    3,    3,    3,
 /*   730 */     3,    3,    3,   81,   20,   55,   74,  144,   82,    9,
 /*   740 */   144,   82,   78,   10,   79,  114,   78,   10,    3,   95,
 /*   750 */   144,  144,    3,   79,    3,    3,    3,    3,    3,    3,
 /*   760 */     3,    3,    3,    3,    3,    3,    3,    3,   95,   81,
 /*   770 */     0,   56,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   780 */    15,   15,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   790 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   800 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   810 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   820 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   830 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   840 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   850 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   860 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   870 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   880 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   890 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   900 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   910 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   920 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   930 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   940 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   950 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   960 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   970 */   273,  273,  273,
};
#define YY_SHIFT_COUNT    (365)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (770)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   190,  118,   25,   42,  247,  268,  268,  358,   38,   38,
 /*    10 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*    20 */    38,    0,   97,  268,  284,  297,  297,   85,   85,   38,
 /*    30 */    38,  199,   38,   12,   38,   38,   38,   38,  354,   42,
 /*    40 */    17,   17,   22,  782,  268,  268,  268,  268,  268,  268,
 /*    50 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*    60 */   268,  268,  268,  268,  268,  268,  284,  297,  284,  284,
 /*    70 */    55,  263,  263,  263,  263,  263,  263,    8,  263,   38,
 /*    80 */    38,   38,  137,   38,   38,   38,   85,   85,   38,   38,
 /*    90 */    38,   38,  344,  344,  339,   85,   38,   38,   38,   38,
 /*   100 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*   110 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*   120 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*   130 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*   140 */    38,   38,   38,   38,   38,   38,   38,   38,   38,   38,
 /*   150 */    38,   38,   38,   38,  484,  484,  484,  441,  441,  441,
 /*   160 */   441,  484,  484,  444,  453,  466,  460,  488,  445,  508,
 /*   170 */   473,  507,  500,  484,  484,  484,  553,  553,   42,  484,
 /*   180 */   484,  547,  549,  598,  557,  556,  601,  558,  564,   22,
 /*   190 */   484,  484,  574,  574,  484,  574,  484,  574,  484,  484,
 /*   200 */   782,  782,   30,   66,   66,   96,   66,  145,  175,  261,
 /*   210 */   261,  261,  261,  261,  261,  249,  273,  289,  340,  340,
 /*   220 */   340,  340,  112,  259,  126,  317,  270,  270,   67,  275,
 /*   230 */   347,  252,  362,  366,  367,  368,  370,  371,  356,  357,
 /*   240 */   374,  375,  376,  377,  457,  464,  383,   94,  394,  398,
 /*   250 */   463,  203,  449,  400,  338,  343,  346,  472,  494,  359,
 /*   260 */   360,  403,  363,  425,  655,  509,  657,  658,  512,  660,
 /*   270 */   661,  567,  570,  529,  568,  577,  612,  614,  613,  599,
 /*   280 */   602,  617,  620,  621,  623,  624,  604,  626,  627,  629,
 /*   290 */   701,  630,  610,  580,  615,  582,  634,  577,  635,  603,
 /*   300 */   636,  605,  642,  632,  637,  653,  717,  638,  640,  719,
 /*   310 */   720,  723,  724,  725,  726,  727,  728,  729,  652,  730,
 /*   320 */   662,  656,  659,  664,  665,  631,  668,  714,  680,  733,
 /*   330 */   593,  596,  654,  654,  654,  654,  737,  606,  607,  654,
 /*   340 */   654,  654,  745,  749,  674,  654,  751,  752,  753,  754,
 /*   350 */   755,  756,  757,  758,  759,  760,  761,  762,  763,  764,
 /*   360 */   673,  688,  765,  766,  715,  770,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-230)
#define YY_REDUCE_MAX   (492)
static const short yy_reduce_ofst[] = {
 /*     0 */  -165,   15, -161,  119, -200, -194, -164,  -80,  130,  -58,
 /*    10 */   -67,  135,  140,  149,  150,  151,  156,  166,  184,  185,
 /*    20 */   186, -168, -192, -230,  -86,  -35,  176,  177,  179, -190,
 /*    30 */   -57,  158,  125,   28,  191,  226,  237,  172,  -12,  214,
 /*    40 */   233,  234,  229, -227, -213, -205,  -84,  -79,   75,   90,
 /*    50 */   123,  225,  245,  248,  250,  251,  254,  255,  256,  257,
 /*    60 */   258,  260,  262,  264,  265,  266,  269,  278,  280,  285,
 /*    70 */   282,  276,  296,  298,  299,  300,  301,  316,  302,  341,
 /*    80 */   342,  345,  277,  348,  349,  350,  305,  306,  353,  355,
 /*    90 */   361,  364,  224,  271,  292,  308,  365,  372,  373,  378,
 /*   100 */   379,  380,  381,  382,  384,  385,  386,  387,  388,  389,
 /*   110 */   390,  391,  392,  393,  395,  396,  397,  399,  401,  402,
 /*   120 */   404,  405,  406,  407,  408,  409,  410,  411,  412,  413,
 /*   130 */   414,  415,  416,  417,  418,  419,  420,  421,  422,  423,
 /*   140 */   424,  426,  427,  428,  429,  430,  431,  432,  433,  434,
 /*   150 */   435,  436,  437,  438,  369,  439,  440,  286,  287,  288,
 /*   160 */   291,  442,  443,  293,  304,  309,  303,  315,  318,  310,
 /*   170 */   319,  446,  307,  447,  448,  450,  451,  452,  456,  454,
 /*   180 */   455,  458,  461,  459,  462,  465,  468,  469,  471,  467,
 /*   190 */   470,  476,  477,  482,  485,  483,  487,  489,  491,  492,
 /*   200 */   490,  486,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   890,  953,  940,  949, 1174, 1174, 1174,  890,  890,  890,
 /*    10 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*    20 */   890, 1081,  910, 1174,  890,  890,  890,  890,  890,  890,
 /*    30 */   890, 1096,  890,  949,  890,  890,  890,  890,  959,  949,
 /*    40 */   959,  959,  890, 1076,  890,  890,  890,  890,  890,  890,
 /*    50 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*    60 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*    70 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*    80 */   890,  890, 1083, 1089, 1086,  890,  890,  890, 1091,  890,
 /*    90 */   890,  890, 1115, 1115, 1074,  890,  890,  890,  890,  890,
 /*   100 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   110 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   120 */   890,  890,  890,  890,  890,  890,  890,  938,  890,  936,
 /*   130 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   140 */   890,  890,  890,  890,  890,  890,  921,  890,  890,  890,
 /*   150 */   890,  890,  890,  908,  912,  912,  912,  890,  890,  890,
 /*   160 */   890,  912,  912, 1122, 1126, 1108, 1120, 1116, 1103, 1101,
 /*   170 */  1099, 1107, 1130,  912,  912,  912,  957,  957,  949,  912,
 /*   180 */   912,  975,  973,  971,  963,  969,  965,  967,  961,  890,
 /*   190 */   912,  912,  947,  947,  912,  947,  912,  947,  912,  912,
 /*   200 */   997, 1014,  890, 1131, 1121,  890, 1173, 1161, 1160, 1169,
 /*   210 */  1168, 1167, 1159, 1158, 1157,  890,  890,  890, 1153, 1156,
 /*   220 */  1155, 1154,  890,  890,  890,  890, 1163, 1162,  890,  890,
 /*   230 */   890,  890,  890,  890,  890,  890,  890,  890, 1127, 1123,
 /*   240 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   250 */   890, 1133,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   260 */   890, 1022,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   270 */   890,  890,  890,  890, 1073,  890,  890,  890,  890, 1085,
 /*   280 */  1084,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   290 */   890,  890, 1117,  890, 1109,  890,  890, 1034,  890,  890,
 /*   300 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   310 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   320 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   330 */   890,  890, 1192, 1187, 1188, 1185,  890,  890,  890, 1184,
 /*   340 */  1179, 1180,  890,  890,  890, 1177,  890,  890,  890,  890,
 /*   350 */   890,  890,  890,  890,  890,  890,  890,  890,  890,  890,
 /*   360 */   981,  890,  919,  917,  890,  890,
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
  /*  111 */ "UNSIGNED",
  /*  112 */ "TAGS",
  /*  113 */ "USING",
  /*  114 */ "NULL",
  /*  115 */ "NOW",
  /*  116 */ "SELECT",
  /*  117 */ "UNION",
  /*  118 */ "ALL",
  /*  119 */ "DISTINCT",
  /*  120 */ "FROM",
  /*  121 */ "VARIABLE",
  /*  122 */ "INTERVAL",
  /*  123 */ "EVERY",
  /*  124 */ "SESSION",
  /*  125 */ "STATE_WINDOW",
  /*  126 */ "FILL",
  /*  127 */ "SLIDING",
  /*  128 */ "ORDER",
  /*  129 */ "BY",
  /*  130 */ "ASC",
  /*  131 */ "GROUP",
  /*  132 */ "HAVING",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "SLIMIT",
  /*  136 */ "SOFFSET",
  /*  137 */ "WHERE",
  /*  138 */ "RESET",
  /*  139 */ "QUERY",
  /*  140 */ "SYNCDB",
  /*  141 */ "ADD",
  /*  142 */ "COLUMN",
  /*  143 */ "MODIFY",
  /*  144 */ "TAG",
  /*  145 */ "CHANGE",
  /*  146 */ "SET",
  /*  147 */ "KILL",
  /*  148 */ "CONNECTION",
  /*  149 */ "STREAM",
  /*  150 */ "COLON",
  /*  151 */ "ABORT",
  /*  152 */ "AFTER",
  /*  153 */ "ATTACH",
  /*  154 */ "BEFORE",
  /*  155 */ "BEGIN",
  /*  156 */ "CASCADE",
  /*  157 */ "CLUSTER",
  /*  158 */ "CONFLICT",
  /*  159 */ "COPY",
  /*  160 */ "DEFERRED",
  /*  161 */ "DELIMITERS",
  /*  162 */ "DETACH",
  /*  163 */ "EACH",
  /*  164 */ "END",
  /*  165 */ "EXPLAIN",
  /*  166 */ "FAIL",
  /*  167 */ "FOR",
  /*  168 */ "IGNORE",
  /*  169 */ "IMMEDIATE",
  /*  170 */ "INITIALLY",
  /*  171 */ "INSTEAD",
  /*  172 */ "KEY",
  /*  173 */ "OF",
  /*  174 */ "RAISE",
  /*  175 */ "REPLACE",
  /*  176 */ "RESTRICT",
  /*  177 */ "ROW",
  /*  178 */ "STATEMENT",
  /*  179 */ "TRIGGER",
  /*  180 */ "VIEW",
  /*  181 */ "SEMI",
  /*  182 */ "NONE",
  /*  183 */ "PREV",
  /*  184 */ "LINEAR",
  /*  185 */ "IMPORT",
  /*  186 */ "TBNAME",
  /*  187 */ "JOIN",
  /*  188 */ "INSERT",
  /*  189 */ "INTO",
  /*  190 */ "VALUES",
  /*  191 */ "error",
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
  /*  232 */ "signed",
  /*  233 */ "create_table_args",
  /*  234 */ "create_stable_args",
  /*  235 */ "create_table_list",
  /*  236 */ "create_from_stable",
  /*  237 */ "columnlist",
  /*  238 */ "tagitemlist1",
  /*  239 */ "tagNamelist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem1",
  /*  243 */ "tagitemlist",
  /*  244 */ "tagitem",
  /*  245 */ "selcollist",
  /*  246 */ "from",
  /*  247 */ "where_opt",
  /*  248 */ "interval_option",
  /*  249 */ "sliding_opt",
  /*  250 */ "session_option",
  /*  251 */ "windowstate_option",
  /*  252 */ "fill_opt",
  /*  253 */ "groupby_opt",
  /*  254 */ "having_opt",
  /*  255 */ "orderby_opt",
  /*  256 */ "slimit_opt",
  /*  257 */ "limit_opt",
  /*  258 */ "union",
  /*  259 */ "sclp",
  /*  260 */ "distinct",
  /*  261 */ "expr",
  /*  262 */ "as",
  /*  263 */ "tablelist",
  /*  264 */ "sub",
  /*  265 */ "tmvar",
  /*  266 */ "intervalKey",
  /*  267 */ "sortlist",
  /*  268 */ "sortitem",
  /*  269 */ "item",
  /*  270 */ "sortorder",
  /*  271 */ "grouplist",
  /*  272 */ "expritem",
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
 /*  52 */ "ids ::= STRING",
 /*  53 */ "ifexists ::= IF EXISTS",
 /*  54 */ "ifexists ::=",
 /*  55 */ "ifnotexists ::= IF NOT EXISTS",
 /*  56 */ "ifnotexists ::=",
 /*  57 */ "cmd ::= CREATE DNODE ids PORT ids",
 /*  58 */ "cmd ::= CREATE DNODE IPTOKEN PORT ids",
 /*  59 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  60 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
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
 /* 103 */ "vgroups ::= VGROUPS INTEGER",
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
 /* 121 */ "alter_db_optr ::=",
 /* 122 */ "alter_db_optr ::= alter_db_optr replica",
 /* 123 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 124 */ "alter_db_optr ::= alter_db_optr keep",
 /* 125 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 126 */ "alter_db_optr ::= alter_db_optr comp",
 /* 127 */ "alter_db_optr ::= alter_db_optr update",
 /* 128 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 129 */ "typename ::= ids",
 /* 130 */ "typename ::= ids LP signed RP",
 /* 131 */ "typename ::= ids UNSIGNED",
 /* 132 */ "signed ::= INTEGER",
 /* 133 */ "signed ::= PLUS INTEGER",
 /* 134 */ "signed ::= MINUS INTEGER",
 /* 135 */ "cmd ::= CREATE TABLE create_table_args",
 /* 136 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 137 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 138 */ "cmd ::= CREATE TABLE create_table_list",
 /* 139 */ "create_table_list ::= create_from_stable",
 /* 140 */ "create_table_list ::= create_table_list create_from_stable",
 /* 141 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 142 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 143 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 144 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 145 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 146 */ "tagNamelist ::= ids",
 /* 147 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 148 */ "columnlist ::= columnlist COMMA column",
 /* 149 */ "columnlist ::= column",
 /* 150 */ "column ::= ids typename",
 /* 151 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 152 */ "tagitemlist1 ::= tagitem1",
 /* 153 */ "tagitem1 ::= MINUS INTEGER",
 /* 154 */ "tagitem1 ::= MINUS FLOAT",
 /* 155 */ "tagitem1 ::= PLUS INTEGER",
 /* 156 */ "tagitem1 ::= PLUS FLOAT",
 /* 157 */ "tagitem1 ::= INTEGER",
 /* 158 */ "tagitem1 ::= FLOAT",
 /* 159 */ "tagitem1 ::= STRING",
 /* 160 */ "tagitem1 ::= BOOL",
 /* 161 */ "tagitem1 ::= NULL",
 /* 162 */ "tagitem1 ::= NOW",
 /* 163 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 164 */ "tagitemlist ::= tagitem",
 /* 165 */ "tagitem ::= INTEGER",
 /* 166 */ "tagitem ::= FLOAT",
 /* 167 */ "tagitem ::= STRING",
 /* 168 */ "tagitem ::= BOOL",
 /* 169 */ "tagitem ::= NULL",
 /* 170 */ "tagitem ::= NOW",
 /* 171 */ "tagitem ::= MINUS INTEGER",
 /* 172 */ "tagitem ::= MINUS FLOAT",
 /* 173 */ "tagitem ::= PLUS INTEGER",
 /* 174 */ "tagitem ::= PLUS FLOAT",
 /* 175 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 176 */ "select ::= LP select RP",
 /* 177 */ "union ::= select",
 /* 178 */ "union ::= union UNION ALL select",
 /* 179 */ "union ::= union UNION select",
 /* 180 */ "cmd ::= union",
 /* 181 */ "select ::= SELECT selcollist",
 /* 182 */ "sclp ::= selcollist COMMA",
 /* 183 */ "sclp ::=",
 /* 184 */ "selcollist ::= sclp distinct expr as",
 /* 185 */ "selcollist ::= sclp STAR",
 /* 186 */ "as ::= AS ids",
 /* 187 */ "as ::= ids",
 /* 188 */ "as ::=",
 /* 189 */ "distinct ::= DISTINCT",
 /* 190 */ "distinct ::=",
 /* 191 */ "from ::= FROM tablelist",
 /* 192 */ "from ::= FROM sub",
 /* 193 */ "sub ::= LP union RP",
 /* 194 */ "sub ::= LP union RP ids",
 /* 195 */ "sub ::= sub COMMA LP union RP ids",
 /* 196 */ "tablelist ::= ids cpxName",
 /* 197 */ "tablelist ::= ids cpxName ids",
 /* 198 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 199 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 200 */ "tmvar ::= VARIABLE",
 /* 201 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 202 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 203 */ "interval_option ::=",
 /* 204 */ "intervalKey ::= INTERVAL",
 /* 205 */ "intervalKey ::= EVERY",
 /* 206 */ "session_option ::=",
 /* 207 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 208 */ "windowstate_option ::=",
 /* 209 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 210 */ "fill_opt ::=",
 /* 211 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 212 */ "fill_opt ::= FILL LP ID RP",
 /* 213 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 214 */ "sliding_opt ::=",
 /* 215 */ "orderby_opt ::=",
 /* 216 */ "orderby_opt ::= ORDER BY sortlist",
 /* 217 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 218 */ "sortlist ::= item sortorder",
 /* 219 */ "item ::= ids cpxName",
 /* 220 */ "sortorder ::= ASC",
 /* 221 */ "sortorder ::= DESC",
 /* 222 */ "sortorder ::=",
 /* 223 */ "groupby_opt ::=",
 /* 224 */ "groupby_opt ::= GROUP BY grouplist",
 /* 225 */ "grouplist ::= grouplist COMMA item",
 /* 226 */ "grouplist ::= item",
 /* 227 */ "having_opt ::=",
 /* 228 */ "having_opt ::= HAVING expr",
 /* 229 */ "limit_opt ::=",
 /* 230 */ "limit_opt ::= LIMIT signed",
 /* 231 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 232 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 233 */ "slimit_opt ::=",
 /* 234 */ "slimit_opt ::= SLIMIT signed",
 /* 235 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 236 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 237 */ "where_opt ::=",
 /* 238 */ "where_opt ::= WHERE expr",
 /* 239 */ "expr ::= LP expr RP",
 /* 240 */ "expr ::= ID",
 /* 241 */ "expr ::= ID DOT ID",
 /* 242 */ "expr ::= ID DOT STAR",
 /* 243 */ "expr ::= INTEGER",
 /* 244 */ "expr ::= MINUS INTEGER",
 /* 245 */ "expr ::= PLUS INTEGER",
 /* 246 */ "expr ::= FLOAT",
 /* 247 */ "expr ::= MINUS FLOAT",
 /* 248 */ "expr ::= PLUS FLOAT",
 /* 249 */ "expr ::= STRING",
 /* 250 */ "expr ::= NOW",
 /* 251 */ "expr ::= VARIABLE",
 /* 252 */ "expr ::= PLUS VARIABLE",
 /* 253 */ "expr ::= MINUS VARIABLE",
 /* 254 */ "expr ::= BOOL",
 /* 255 */ "expr ::= NULL",
 /* 256 */ "expr ::= ID LP exprlist RP",
 /* 257 */ "expr ::= ID LP STAR RP",
 /* 258 */ "expr ::= expr IS NULL",
 /* 259 */ "expr ::= expr IS NOT NULL",
 /* 260 */ "expr ::= expr LT expr",
 /* 261 */ "expr ::= expr GT expr",
 /* 262 */ "expr ::= expr LE expr",
 /* 263 */ "expr ::= expr GE expr",
 /* 264 */ "expr ::= expr NE expr",
 /* 265 */ "expr ::= expr EQ expr",
 /* 266 */ "expr ::= expr BETWEEN expr AND expr",
 /* 267 */ "expr ::= expr AND expr",
 /* 268 */ "expr ::= expr OR expr",
 /* 269 */ "expr ::= expr PLUS expr",
 /* 270 */ "expr ::= expr MINUS expr",
 /* 271 */ "expr ::= expr STAR expr",
 /* 272 */ "expr ::= expr SLASH expr",
 /* 273 */ "expr ::= expr REM expr",
 /* 274 */ "expr ::= expr LIKE expr",
 /* 275 */ "expr ::= expr MATCH expr",
 /* 276 */ "expr ::= expr NMATCH expr",
 /* 277 */ "expr ::= expr IN LP exprlist RP",
 /* 278 */ "exprlist ::= exprlist COMMA expritem",
 /* 279 */ "exprlist ::= expritem",
 /* 280 */ "expritem ::= expr",
 /* 281 */ "expritem ::=",
 /* 282 */ "cmd ::= RESET QUERY CACHE",
 /* 283 */ "cmd ::= SYNCDB ids REPLICA",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 290 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 291 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 298 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 299 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 300 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 301 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 302 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 200: /* exprlist */
    case 245: /* selcollist */
    case 259: /* sclp */
{
tSqlExprListDestroy((yypminor->yy165));
}
      break;
    case 214: /* intitemlist */
    case 216: /* keep */
    case 237: /* columnlist */
    case 238: /* tagitemlist1 */
    case 239: /* tagNamelist */
    case 243: /* tagitemlist */
    case 252: /* fill_opt */
    case 253: /* groupby_opt */
    case 255: /* orderby_opt */
    case 267: /* sortlist */
    case 271: /* grouplist */
{
taosArrayDestroy((yypminor->yy165));
}
      break;
    case 235: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy326));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy278));
}
      break;
    case 246: /* from */
    case 263: /* tablelist */
    case 264: /* sub */
{
destroyRelationInfo((yypminor->yy10));
}
      break;
    case 247: /* where_opt */
    case 254: /* having_opt */
    case 261: /* expr */
    case 272: /* expritem */
{
tSqlExprDestroy((yypminor->yy202));
}
      break;
    case 258: /* union */
{
destroyAllSqlNode((yypminor->yy503));
}
      break;
    case 268: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy425));
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
  {  194,   -1 }, /* (52) ids ::= STRING */
  {  197,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  197,    0 }, /* (54) ifexists ::= */
  {  201,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  201,    0 }, /* (56) ifnotexists ::= */
  {  193,   -5 }, /* (57) cmd ::= CREATE DNODE ids PORT ids */
  {  193,   -5 }, /* (58) cmd ::= CREATE DNODE IPTOKEN PORT ids */
  {  193,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  193,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  193,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  204,    0 }, /* (64) bufsize ::= */
  {  204,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  205,    0 }, /* (66) pps ::= */
  {  205,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  206,    0 }, /* (68) tseries ::= */
  {  206,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  207,    0 }, /* (70) dbs ::= */
  {  207,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  208,    0 }, /* (72) streams ::= */
  {  208,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  209,    0 }, /* (74) storage ::= */
  {  209,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  210,    0 }, /* (76) qtime ::= */
  {  210,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  211,    0 }, /* (78) users ::= */
  {  211,   -2 }, /* (79) users ::= USERS INTEGER */
  {  212,    0 }, /* (80) conns ::= */
  {  212,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  213,    0 }, /* (82) state ::= */
  {  213,   -2 }, /* (83) state ::= STATE ids */
  {  199,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  214,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  214,   -1 }, /* (86) intitemlist ::= intitem */
  {  215,   -1 }, /* (87) intitem ::= INTEGER */
  {  216,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  217,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  218,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  219,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  220,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  221,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  222,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  223,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  224,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  225,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  226,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  227,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  228,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  229,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  230,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  231,   -2 }, /* (103) vgroups ::= VGROUPS INTEGER */
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
  {  198,    0 }, /* (121) alter_db_optr ::= */
  {  198,   -2 }, /* (122) alter_db_optr ::= alter_db_optr replica */
  {  198,   -2 }, /* (123) alter_db_optr ::= alter_db_optr quorum */
  {  198,   -2 }, /* (124) alter_db_optr ::= alter_db_optr keep */
  {  198,   -2 }, /* (125) alter_db_optr ::= alter_db_optr blocks */
  {  198,   -2 }, /* (126) alter_db_optr ::= alter_db_optr comp */
  {  198,   -2 }, /* (127) alter_db_optr ::= alter_db_optr update */
  {  198,   -2 }, /* (128) alter_db_optr ::= alter_db_optr cachelast */
  {  203,   -1 }, /* (129) typename ::= ids */
  {  203,   -4 }, /* (130) typename ::= ids LP signed RP */
  {  203,   -2 }, /* (131) typename ::= ids UNSIGNED */
  {  232,   -1 }, /* (132) signed ::= INTEGER */
  {  232,   -2 }, /* (133) signed ::= PLUS INTEGER */
  {  232,   -2 }, /* (134) signed ::= MINUS INTEGER */
  {  193,   -3 }, /* (135) cmd ::= CREATE TABLE create_table_args */
  {  193,   -3 }, /* (136) cmd ::= CREATE TABLE create_stable_args */
  {  193,   -3 }, /* (137) cmd ::= CREATE STABLE create_stable_args */
  {  193,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_list */
  {  235,   -1 }, /* (139) create_table_list ::= create_from_stable */
  {  235,   -2 }, /* (140) create_table_list ::= create_table_list create_from_stable */
  {  233,   -6 }, /* (141) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  234,  -10 }, /* (142) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  236,  -10 }, /* (143) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  236,  -13 }, /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  239,   -3 }, /* (145) tagNamelist ::= tagNamelist COMMA ids */
  {  239,   -1 }, /* (146) tagNamelist ::= ids */
  {  233,   -5 }, /* (147) create_table_args ::= ifnotexists ids cpxName AS select */
  {  237,   -3 }, /* (148) columnlist ::= columnlist COMMA column */
  {  237,   -1 }, /* (149) columnlist ::= column */
  {  241,   -2 }, /* (150) column ::= ids typename */
  {  238,   -3 }, /* (151) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  238,   -1 }, /* (152) tagitemlist1 ::= tagitem1 */
  {  242,   -2 }, /* (153) tagitem1 ::= MINUS INTEGER */
  {  242,   -2 }, /* (154) tagitem1 ::= MINUS FLOAT */
  {  242,   -2 }, /* (155) tagitem1 ::= PLUS INTEGER */
  {  242,   -2 }, /* (156) tagitem1 ::= PLUS FLOAT */
  {  242,   -1 }, /* (157) tagitem1 ::= INTEGER */
  {  242,   -1 }, /* (158) tagitem1 ::= FLOAT */
  {  242,   -1 }, /* (159) tagitem1 ::= STRING */
  {  242,   -1 }, /* (160) tagitem1 ::= BOOL */
  {  242,   -1 }, /* (161) tagitem1 ::= NULL */
  {  242,   -1 }, /* (162) tagitem1 ::= NOW */
  {  243,   -3 }, /* (163) tagitemlist ::= tagitemlist COMMA tagitem */
  {  243,   -1 }, /* (164) tagitemlist ::= tagitem */
  {  244,   -1 }, /* (165) tagitem ::= INTEGER */
  {  244,   -1 }, /* (166) tagitem ::= FLOAT */
  {  244,   -1 }, /* (167) tagitem ::= STRING */
  {  244,   -1 }, /* (168) tagitem ::= BOOL */
  {  244,   -1 }, /* (169) tagitem ::= NULL */
  {  244,   -1 }, /* (170) tagitem ::= NOW */
  {  244,   -2 }, /* (171) tagitem ::= MINUS INTEGER */
  {  244,   -2 }, /* (172) tagitem ::= MINUS FLOAT */
  {  244,   -2 }, /* (173) tagitem ::= PLUS INTEGER */
  {  244,   -2 }, /* (174) tagitem ::= PLUS FLOAT */
  {  240,  -14 }, /* (175) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  240,   -3 }, /* (176) select ::= LP select RP */
  {  258,   -1 }, /* (177) union ::= select */
  {  258,   -4 }, /* (178) union ::= union UNION ALL select */
  {  258,   -3 }, /* (179) union ::= union UNION select */
  {  193,   -1 }, /* (180) cmd ::= union */
  {  240,   -2 }, /* (181) select ::= SELECT selcollist */
  {  259,   -2 }, /* (182) sclp ::= selcollist COMMA */
  {  259,    0 }, /* (183) sclp ::= */
  {  245,   -4 }, /* (184) selcollist ::= sclp distinct expr as */
  {  245,   -2 }, /* (185) selcollist ::= sclp STAR */
  {  262,   -2 }, /* (186) as ::= AS ids */
  {  262,   -1 }, /* (187) as ::= ids */
  {  262,    0 }, /* (188) as ::= */
  {  260,   -1 }, /* (189) distinct ::= DISTINCT */
  {  260,    0 }, /* (190) distinct ::= */
  {  246,   -2 }, /* (191) from ::= FROM tablelist */
  {  246,   -2 }, /* (192) from ::= FROM sub */
  {  264,   -3 }, /* (193) sub ::= LP union RP */
  {  264,   -4 }, /* (194) sub ::= LP union RP ids */
  {  264,   -6 }, /* (195) sub ::= sub COMMA LP union RP ids */
  {  263,   -2 }, /* (196) tablelist ::= ids cpxName */
  {  263,   -3 }, /* (197) tablelist ::= ids cpxName ids */
  {  263,   -4 }, /* (198) tablelist ::= tablelist COMMA ids cpxName */
  {  263,   -5 }, /* (199) tablelist ::= tablelist COMMA ids cpxName ids */
  {  265,   -1 }, /* (200) tmvar ::= VARIABLE */
  {  248,   -4 }, /* (201) interval_option ::= intervalKey LP tmvar RP */
  {  248,   -6 }, /* (202) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  248,    0 }, /* (203) interval_option ::= */
  {  266,   -1 }, /* (204) intervalKey ::= INTERVAL */
  {  266,   -1 }, /* (205) intervalKey ::= EVERY */
  {  250,    0 }, /* (206) session_option ::= */
  {  250,   -7 }, /* (207) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  251,    0 }, /* (208) windowstate_option ::= */
  {  251,   -4 }, /* (209) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  252,    0 }, /* (210) fill_opt ::= */
  {  252,   -6 }, /* (211) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  252,   -4 }, /* (212) fill_opt ::= FILL LP ID RP */
  {  249,   -4 }, /* (213) sliding_opt ::= SLIDING LP tmvar RP */
  {  249,    0 }, /* (214) sliding_opt ::= */
  {  255,    0 }, /* (215) orderby_opt ::= */
  {  255,   -3 }, /* (216) orderby_opt ::= ORDER BY sortlist */
  {  267,   -4 }, /* (217) sortlist ::= sortlist COMMA item sortorder */
  {  267,   -2 }, /* (218) sortlist ::= item sortorder */
  {  269,   -2 }, /* (219) item ::= ids cpxName */
  {  270,   -1 }, /* (220) sortorder ::= ASC */
  {  270,   -1 }, /* (221) sortorder ::= DESC */
  {  270,    0 }, /* (222) sortorder ::= */
  {  253,    0 }, /* (223) groupby_opt ::= */
  {  253,   -3 }, /* (224) groupby_opt ::= GROUP BY grouplist */
  {  271,   -3 }, /* (225) grouplist ::= grouplist COMMA item */
  {  271,   -1 }, /* (226) grouplist ::= item */
  {  254,    0 }, /* (227) having_opt ::= */
  {  254,   -2 }, /* (228) having_opt ::= HAVING expr */
  {  257,    0 }, /* (229) limit_opt ::= */
  {  257,   -2 }, /* (230) limit_opt ::= LIMIT signed */
  {  257,   -4 }, /* (231) limit_opt ::= LIMIT signed OFFSET signed */
  {  257,   -4 }, /* (232) limit_opt ::= LIMIT signed COMMA signed */
  {  256,    0 }, /* (233) slimit_opt ::= */
  {  256,   -2 }, /* (234) slimit_opt ::= SLIMIT signed */
  {  256,   -4 }, /* (235) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  256,   -4 }, /* (236) slimit_opt ::= SLIMIT signed COMMA signed */
  {  247,    0 }, /* (237) where_opt ::= */
  {  247,   -2 }, /* (238) where_opt ::= WHERE expr */
  {  261,   -3 }, /* (239) expr ::= LP expr RP */
  {  261,   -1 }, /* (240) expr ::= ID */
  {  261,   -3 }, /* (241) expr ::= ID DOT ID */
  {  261,   -3 }, /* (242) expr ::= ID DOT STAR */
  {  261,   -1 }, /* (243) expr ::= INTEGER */
  {  261,   -2 }, /* (244) expr ::= MINUS INTEGER */
  {  261,   -2 }, /* (245) expr ::= PLUS INTEGER */
  {  261,   -1 }, /* (246) expr ::= FLOAT */
  {  261,   -2 }, /* (247) expr ::= MINUS FLOAT */
  {  261,   -2 }, /* (248) expr ::= PLUS FLOAT */
  {  261,   -1 }, /* (249) expr ::= STRING */
  {  261,   -1 }, /* (250) expr ::= NOW */
  {  261,   -1 }, /* (251) expr ::= VARIABLE */
  {  261,   -2 }, /* (252) expr ::= PLUS VARIABLE */
  {  261,   -2 }, /* (253) expr ::= MINUS VARIABLE */
  {  261,   -1 }, /* (254) expr ::= BOOL */
  {  261,   -1 }, /* (255) expr ::= NULL */
  {  261,   -4 }, /* (256) expr ::= ID LP exprlist RP */
  {  261,   -4 }, /* (257) expr ::= ID LP STAR RP */
  {  261,   -3 }, /* (258) expr ::= expr IS NULL */
  {  261,   -4 }, /* (259) expr ::= expr IS NOT NULL */
  {  261,   -3 }, /* (260) expr ::= expr LT expr */
  {  261,   -3 }, /* (261) expr ::= expr GT expr */
  {  261,   -3 }, /* (262) expr ::= expr LE expr */
  {  261,   -3 }, /* (263) expr ::= expr GE expr */
  {  261,   -3 }, /* (264) expr ::= expr NE expr */
  {  261,   -3 }, /* (265) expr ::= expr EQ expr */
  {  261,   -5 }, /* (266) expr ::= expr BETWEEN expr AND expr */
  {  261,   -3 }, /* (267) expr ::= expr AND expr */
  {  261,   -3 }, /* (268) expr ::= expr OR expr */
  {  261,   -3 }, /* (269) expr ::= expr PLUS expr */
  {  261,   -3 }, /* (270) expr ::= expr MINUS expr */
  {  261,   -3 }, /* (271) expr ::= expr STAR expr */
  {  261,   -3 }, /* (272) expr ::= expr SLASH expr */
  {  261,   -3 }, /* (273) expr ::= expr REM expr */
  {  261,   -3 }, /* (274) expr ::= expr LIKE expr */
  {  261,   -3 }, /* (275) expr ::= expr MATCH expr */
  {  261,   -3 }, /* (276) expr ::= expr NMATCH expr */
  {  261,   -5 }, /* (277) expr ::= expr IN LP exprlist RP */
  {  200,   -3 }, /* (278) exprlist ::= exprlist COMMA expritem */
  {  200,   -1 }, /* (279) exprlist ::= expritem */
  {  272,   -1 }, /* (280) expritem ::= expr */
  {  272,    0 }, /* (281) expritem ::= */
  {  193,   -3 }, /* (282) cmd ::= RESET QUERY CACHE */
  {  193,   -3 }, /* (283) cmd ::= SYNCDB ids REPLICA */
  {  193,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (286) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (287) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (288) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (289) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (290) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (291) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (294) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (295) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (296) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (297) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (298) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (299) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -3 }, /* (300) cmd ::= KILL CONNECTION INTEGER */
  {  193,   -5 }, /* (301) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  193,   -5 }, /* (302) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 135: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==135);
      case 136: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==136);
      case 137: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==137);
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
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy16, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy211);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy165);}
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
      case 190: /* distinct ::= */ yytestcase(yyruleno==190);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids PORT ids */
      case 58: /* cmd ::= CREATE DNODE IPTOKEN PORT ids */ yytestcase(yyruleno==58);
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy16, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy106, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy106, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy211.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy211.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy211.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy211.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy211.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy211.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy211.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy211 = yylhsminor.yy211;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 163: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==163);
{ yylhsminor.yy165 = tListItemAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy425, -1);    }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 86: /* intitemlist ::= intitem */
      case 164: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==164);
{ yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[0].minor.yy425, -1); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 87: /* intitem ::= INTEGER */
      case 165: /* tagitem ::= INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= FLOAT */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= STRING */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= BOOL */ yytestcase(yyruleno==168);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy165 = yymsp[0].minor.yy165; }
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
      case 103: /* vgroups ::= VGROUPS INTEGER */ yytestcase(yyruleno==103);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 104: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy16);}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 122: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==122);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 123: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==123);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 125: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==125);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 126: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==126);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 124: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==124);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.keep = yymsp[0].minor.yy165; }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 127: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==127);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 128: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==128);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 120: /* db_optr ::= db_optr vgroups */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.numOfVgroups = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 121: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy16);}
        break;
      case 129: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy106, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy106 = yylhsminor.yy106;
        break;
      case 130: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy207 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy106, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy207;  // negative value of name length
    tSetColumnType(&yylhsminor.yy106, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy106 = yylhsminor.yy106;
        break;
      case 131: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy106, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 132: /* signed ::= INTEGER */
{ yylhsminor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 133: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 134: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 138: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy326;}
        break;
      case 139: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy150);
  pCreateTable->type = TSQL_CREATE_CTABLE;
  yylhsminor.yy326 = pCreateTable;
}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 140: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy326->childTableInfo, &yymsp[0].minor.yy150);
  yylhsminor.yy326 = yymsp[-1].minor.yy326;
}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy326 = tSetCreateTableInfo(yymsp[-1].minor.yy165, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy326 = yylhsminor.yy326;
        break;
      case 142: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy326 = tSetCreateTableInfo(yymsp[-5].minor.yy165, yymsp[-1].minor.yy165, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy326 = yylhsminor.yy326;
        break;
      case 143: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy150 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy165, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy150 = yylhsminor.yy150;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy150 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy165, yymsp[-1].minor.yy165, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy150 = yylhsminor.yy150;
        break;
      case 145: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy0); yylhsminor.yy165 = yymsp[-2].minor.yy165;  }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 146: /* tagNamelist ::= ids */
{yylhsminor.yy165 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 147: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy326 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy278, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy326 = yylhsminor.yy326;
        break;
      case 148: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy106); yylhsminor.yy165 = yymsp[-2].minor.yy165;  }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 149: /* columnlist ::= column */
{yylhsminor.yy165 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy106);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 150: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy106, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy106);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 151: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy0); yylhsminor.yy165 = yymsp[-2].minor.yy165;}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 152: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy165 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 153: /* tagitem1 ::= MINUS INTEGER */
      case 154: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 157: /* tagitem1 ::= INTEGER */
      case 158: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem1 ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==160);
      case 161: /* tagitem1 ::= NULL */ yytestcase(yyruleno==161);
      case 162: /* tagitem1 ::= NOW */ yytestcase(yyruleno==162);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 170: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 171: /* tagitem ::= MINUS INTEGER */
      case 172: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==172);
      case 173: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==173);
      case 174: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==174);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy425, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy425 = yylhsminor.yy425;
        break;
      case 175: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy278 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy165, yymsp[-11].minor.yy10, yymsp[-10].minor.yy202, yymsp[-4].minor.yy165, yymsp[-2].minor.yy165, &yymsp[-9].minor.yy532, &yymsp[-7].minor.yy97, &yymsp[-6].minor.yy6, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy165, &yymsp[0].minor.yy367, &yymsp[-1].minor.yy367, yymsp[-3].minor.yy202);
}
  yymsp[-13].minor.yy278 = yylhsminor.yy278;
        break;
      case 176: /* select ::= LP select RP */
{yymsp[-2].minor.yy278 = yymsp[-1].minor.yy278;}
        break;
      case 177: /* union ::= select */
{ yylhsminor.yy503 = setSubclause(NULL, yymsp[0].minor.yy278); }
  yymsp[0].minor.yy503 = yylhsminor.yy503;
        break;
      case 178: /* union ::= union UNION ALL select */
{ yylhsminor.yy503 = appendSelectClause(yymsp[-3].minor.yy503, SQL_TYPE_UNIONALL, yymsp[0].minor.yy278);  }
  yymsp[-3].minor.yy503 = yylhsminor.yy503;
        break;
      case 179: /* union ::= union UNION select */
{ yylhsminor.yy503 = appendSelectClause(yymsp[-2].minor.yy503, SQL_TYPE_UNION, yymsp[0].minor.yy278);  }
  yymsp[-2].minor.yy503 = yylhsminor.yy503;
        break;
      case 180: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy503, NULL, TSDB_SQL_SELECT); }
        break;
      case 181: /* select ::= SELECT selcollist */
{
  yylhsminor.yy278 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy165, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy278 = yylhsminor.yy278;
        break;
      case 182: /* sclp ::= selcollist COMMA */
{yylhsminor.yy165 = yymsp[-1].minor.yy165;}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 183: /* sclp ::= */
      case 215: /* orderby_opt ::= */ yytestcase(yyruleno==215);
{yymsp[1].minor.yy165 = 0;}
        break;
      case 184: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy165 = tSqlExprListAppend(yymsp[-3].minor.yy165, yymsp[-1].minor.yy202,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 185: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy165 = tSqlExprListAppend(yymsp[-1].minor.yy165, pNode, 0, 0);
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 186: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 187: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 188: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 189: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* from ::= FROM tablelist */
      case 192: /* from ::= FROM sub */ yytestcase(yyruleno==192);
{yymsp[-1].minor.yy10 = yymsp[0].minor.yy10;}
        break;
      case 193: /* sub ::= LP union RP */
{yymsp[-2].minor.yy10 = addSubquery(NULL, yymsp[-1].minor.yy503, NULL);}
        break;
      case 194: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy10 = addSubquery(NULL, yymsp[-2].minor.yy503, &yymsp[0].minor.yy0);}
        break;
      case 195: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy10 = addSubquery(yymsp[-5].minor.yy10, yymsp[-2].minor.yy503, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy10 = yylhsminor.yy10;
        break;
      case 196: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 197: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 198: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(yymsp[-3].minor.yy10, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy10 = yylhsminor.yy10;
        break;
      case 199: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(yymsp[-4].minor.yy10, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy10 = yylhsminor.yy10;
        break;
      case 200: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 201: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy532.interval = yymsp[-1].minor.yy0; yylhsminor.yy532.offset.n = 0; yylhsminor.yy532.token = yymsp[-3].minor.yy46;}
  yymsp[-3].minor.yy532 = yylhsminor.yy532;
        break;
      case 202: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy532.interval = yymsp[-3].minor.yy0; yylhsminor.yy532.offset = yymsp[-1].minor.yy0;   yylhsminor.yy532.token = yymsp[-5].minor.yy46;}
  yymsp[-5].minor.yy532 = yylhsminor.yy532;
        break;
      case 203: /* interval_option ::= */
{memset(&yymsp[1].minor.yy532, 0, sizeof(yymsp[1].minor.yy532));}
        break;
      case 204: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy46 = TK_INTERVAL;}
        break;
      case 205: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy46 = TK_EVERY;   }
        break;
      case 206: /* session_option ::= */
{yymsp[1].minor.yy97.col.n = 0; yymsp[1].minor.yy97.gap.n = 0;}
        break;
      case 207: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy97.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy97.gap = yymsp[-1].minor.yy0;
}
        break;
      case 208: /* windowstate_option ::= */
{ yymsp[1].minor.yy6.col.n = 0; yymsp[1].minor.yy6.col.z = NULL;}
        break;
      case 209: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy6.col = yymsp[-1].minor.yy0; }
        break;
      case 210: /* fill_opt ::= */
{ yymsp[1].minor.yy165 = 0;     }
        break;
      case 211: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy165, &A, -1, 0);
    yymsp[-5].minor.yy165 = yymsp[-1].minor.yy165;
}
        break;
      case 212: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy165 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 213: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 214: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 216: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 217: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy165 = tListItemAppend(yymsp[-3].minor.yy165, &yymsp[-1].minor.yy425, yymsp[0].minor.yy47);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 218: /* sortlist ::= item sortorder */
{
  yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[-1].minor.yy425, yymsp[0].minor.yy47);
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 219: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy425, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy425 = yylhsminor.yy425;
        break;
      case 220: /* sortorder ::= ASC */
{ yymsp[0].minor.yy47 = TSDB_ORDER_ASC; }
        break;
      case 221: /* sortorder ::= DESC */
{ yymsp[0].minor.yy47 = TSDB_ORDER_DESC;}
        break;
      case 222: /* sortorder ::= */
{ yymsp[1].minor.yy47 = TSDB_ORDER_ASC; }
        break;
      case 223: /* groupby_opt ::= */
{ yymsp[1].minor.yy165 = 0;}
        break;
      case 224: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 225: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy165 = tListItemAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy425, -1);
}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 226: /* grouplist ::= item */
{
  yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[0].minor.yy425, -1);
}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 227: /* having_opt ::= */
      case 237: /* where_opt ::= */ yytestcase(yyruleno==237);
      case 281: /* expritem ::= */ yytestcase(yyruleno==281);
{yymsp[1].minor.yy202 = 0;}
        break;
      case 228: /* having_opt ::= HAVING expr */
      case 238: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==238);
{yymsp[-1].minor.yy202 = yymsp[0].minor.yy202;}
        break;
      case 229: /* limit_opt ::= */
      case 233: /* slimit_opt ::= */ yytestcase(yyruleno==233);
{yymsp[1].minor.yy367.limit = -1; yymsp[1].minor.yy367.offset = 0;}
        break;
      case 230: /* limit_opt ::= LIMIT signed */
      case 234: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==234);
{yymsp[-1].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-1].minor.yy367.offset = 0;}
        break;
      case 231: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy367.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[0].minor.yy207;}
        break;
      case 232: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[-2].minor.yy207;}
        break;
      case 235: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy367.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[0].minor.yy207;}
        break;
      case 236: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[-2].minor.yy207;}
        break;
      case 239: /* expr ::= LP expr RP */
{yylhsminor.yy202 = yymsp[-1].minor.yy202; yylhsminor.yy202->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy202->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 240: /* expr ::= ID */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 241: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 242: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 243: /* expr ::= INTEGER */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 244: /* expr ::= MINUS INTEGER */
      case 245: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==245);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 246: /* expr ::= FLOAT */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 247: /* expr ::= MINUS FLOAT */
      case 248: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==248);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 249: /* expr ::= STRING */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 250: /* expr ::= NOW */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 251: /* expr ::= VARIABLE */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 252: /* expr ::= PLUS VARIABLE */
      case 253: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==253);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 254: /* expr ::= BOOL */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 255: /* expr ::= NULL */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 256: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy202 = tSqlExprCreateFunction(yymsp[-1].minor.yy165, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 257: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy202 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 258: /* expr ::= expr IS NULL */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 259: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-3].minor.yy202, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 260: /* expr ::= expr LT expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LT);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 261: /* expr ::= expr GT expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_GT);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 262: /* expr ::= expr LE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 263: /* expr ::= expr GE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_GE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 264: /* expr ::= expr NE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_NE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 265: /* expr ::= expr EQ expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_EQ);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 266: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy202); yylhsminor.yy202 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy202, yymsp[-2].minor.yy202, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy202, TK_LE), TK_AND);}
  yymsp[-4].minor.yy202 = yylhsminor.yy202;
        break;
      case 267: /* expr ::= expr AND expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_AND);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 268: /* expr ::= expr OR expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_OR); }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 269: /* expr ::= expr PLUS expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_PLUS);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 270: /* expr ::= expr MINUS expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_MINUS); }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 271: /* expr ::= expr STAR expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_STAR);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 272: /* expr ::= expr SLASH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_DIVIDE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 273: /* expr ::= expr REM expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_REM);   }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 274: /* expr ::= expr LIKE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LIKE);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 275: /* expr ::= expr MATCH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_MATCH);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 276: /* expr ::= expr NMATCH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_NMATCH);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 277: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-4].minor.yy202, (tSqlExpr*)yymsp[-1].minor.yy165, TK_IN); }
  yymsp[-4].minor.yy202 = yylhsminor.yy202;
        break;
      case 278: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy165 = tSqlExprListAppend(yymsp[-2].minor.yy165,yymsp[0].minor.yy202,0, 0);}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 279: /* exprlist ::= expritem */
{yylhsminor.yy165 = tSqlExprListAppend(0,yymsp[0].minor.yy202,0, 0);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 280: /* expritem ::= expr */
{yylhsminor.yy202 = yymsp[0].minor.yy202;}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 282: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 283: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 290: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy425, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 298: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy425, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 300: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 301: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 302: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
