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
#define YYNOCODE 279
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SRelationInfo* yy8;
  SWindowStateVal yy40;
  SSqlNode* yy56;
  SVariant yy69;
  SCreateDbInfo yy90;
  int yy96;
  SField yy100;
  int32_t yy104;
  SSessionWindowVal yy147;
  SSubclause* yy149;
  SCreatedTableInfo yy152;
  SCreateAcctInfo yy171;
  SLimit yy231;
  int64_t yy325;
  SIntervalVal yy400;
  SArray* yy421;
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
#define YYNSTATE             365
#define YYNRULE              301
#define YYNTOKEN             197
#define YY_MAX_SHIFT         364
#define YY_MIN_SHIFTREDUCE   584
#define YY_MAX_SHIFTREDUCE   884
#define YY_ERROR_ACTION      885
#define YY_ACCEPT_ACTION     886
#define YY_NO_ACTION         887
#define YY_MIN_REDUCE        888
#define YY_MAX_REDUCE        1188
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
#define YY_ACTTAB_COUNT (783)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    96,  635,  249,   21,  635,  203,  248,  714,  206,  636,
 /*    10 */   363,  230,  636,   55,   56, 1073,   59,   60, 1024, 1164,
 /*    20 */   252,   49,   48,   47,  671,   58,  322,   63,   61,   64,
 /*    30 */    62, 1021, 1022,   33, 1025,   54,   53,  342,  341,   52,
 /*    40 */    51,   50,   55,   56,  261,   59,   60,  236, 1050,  252,
 /*    50 */    49,   48,   47,  176,   58,  322,   63,   61,   64,   62,
 /*    60 */   155,  827,  206,  830,   54,   53,  206,  204,   52,   51,
 /*    70 */    50,   55,   56, 1165,   59,   60,   99, 1165,  252,   49,
 /*    80 */    48,   47, 1070,   58,  322,   63,   61,   64,   62,  162,
 /*    90 */    81,   36,  635,   54,   53,  318,  162,   52,   51,   50,
 /*   100 */   636,   54,   53,  162,  318,   52,   51,   50,   55,   57,
 /*   110 */  1026,   59,   60,  253,  821,  252,   49,   48,   47,  635,
 /*   120 */    58,  322,   63,   61,   64,   62,  936,  636,  280,  279,
 /*   130 */    54,   53,  188,  232,   52,   51,   50, 1035,  585,  586,
 /*   140 */   587,  588,  589,  590,  591,  592,  593,  594,  595,  596,
 /*   150 */   597,  598,  153,   56,  231,   59,   60,  162,   74,  252,
 /*   160 */    49,   48,   47, 1111,   58,  322,   63,   61,   64,   62,
 /*   170 */  1112, 1063,  292,  206,   54,   53,  255,   93,   52,   51,
 /*   180 */    50,   59,   60,  834, 1165,  252,   49,   48,   47,  233,
 /*   190 */    58,  322,   63,   61,   64,   62,   42,   75,  358,  357,
 /*   200 */    54,   53,   27,  356,   52,   51,   50,  355,  250,  354,
 /*   210 */   353,   42,  316,  358,  357,  315,  314,  313,  356,  312,
 /*   220 */   311,  310,  355,  309,  354,  353,  886,  364,  352,  294,
 /*   230 */     4,   92, 1004,  992,  993,  994,  995,  996,  997,  998,
 /*   240 */   999, 1000, 1001, 1002, 1003, 1005, 1006,   22,  251,  836,
 /*   250 */    87,  260,  825,  256,  828,  254,  831,  330,  329,  947,
 /*   260 */   635,   52,   51,   50,  215,  188,  251,  836,  636,   36,
 /*   270 */   825,  216,  828, 1063,  831,  786,  787,  137,  136,  135,
 /*   280 */   217,  209,  228,  229,  327,   87,  323, 1063,   43,  210,
 /*   290 */    86,  274,   36,   36,   63,   61,   64,   62,   36,   87,
 /*   300 */   228,  229,   54,   53,  211,  234,   52,   51,   50,  750,
 /*   310 */    36,  240,  747,   36,  748, 1035,  749,  742, 1159,  826,
 /*   320 */   739,  829,  740,   43,  741,  362,  361,  146,  262, 1032,
 /*   330 */   259,   65,  337,  336,  241,  331,   36,   43, 1035, 1035,
 /*   340 */   332,   36,  257,  258, 1035,  273, 1158,   79,  320,   65,
 /*   350 */   244,  245,  333,   36,  224,  334, 1035, 1049,   12, 1035,
 /*   360 */  1184,    3,   39,  178,   95, 1157,  266,  837,  832,  105,
 /*   370 */    77,  101,  108,  243,  833,  270,  269, 1010,  338, 1008,
 /*   380 */  1009,  767, 1035,  339, 1011,  837,  832, 1035, 1012,  305,
 /*   390 */  1013, 1014,  833,   98,  803,  340,  197,  195,  193, 1035,
 /*   400 */    36,  226,   36,  192,  141,  140,  139,  138,  122,  116,
 /*   410 */   126,  242,  152,  150,  149, 1038,  246,  131,  134,  125,
 /*   420 */  1038,   80,  171,  261,  261,  124,  128,  751,  752,   94,
 /*   430 */   764,  937,  177, 1036,   84,  743,  744,  188,  275,  352,
 /*   440 */   282,  835,  344,   82,   85,  783, 1035,  793, 1034,  794,
 /*   450 */   359,  974,  802, 1023,   37,    7,   71,  724,  297,  726,
 /*   460 */   299,  157,  737,   66,  738,   24,  735,  771,  736,  725,
 /*   470 */    32,  823,   70,   37,   37,   67,   97,  859,  838,  324,
 /*   480 */   634,   14,   70,   13,  115,   67,  114,   16,  755,   15,
 /*   490 */   756,   78, 1037,   23,   23,  227,   23,   72,   18,  753,
 /*   500 */    17,  754,  133,  132,  300,  121,  207,  120,  208,  824,
 /*   510 */   212,   20,  205,   19,  213,  214,  219,  220,  221,  218,
 /*   520 */   202, 1176, 1065, 1122,  713, 1121,  238, 1118, 1117,  239,
 /*   530 */   321,  343, 1064,   44,  271,  154, 1104, 1072, 1083, 1103,
 /*   540 */  1080, 1081,  151,  277,  172, 1033, 1085,  156,  281,  235,
 /*   550 */   283,  161,  288,  285,  173,  165, 1031, 1061,  174,  164,
 /*   560 */   782,  175,  163,  166,  168,  951,  302,  303,  304,  307,
 /*   570 */   308,  200,  295,  291,  293,   76,   40,  319,  946,  945,
 /*   580 */   328, 1183,  112, 1182,  840, 1179,   73,  179,  335, 1175,
 /*   590 */   118, 1174,   46,  289, 1171,  287,  180,  971,   41,   38,
 /*   600 */   201,  934,  127,  932,  129,  130,  930,  284,  929,  263,
 /*   610 */   190,  191,  926,  925,  924,  923,  922,  921,  920,  194,
 /*   620 */   196,  917,   45,  915,  913,  911,  198,  908,  199,  904,
 /*   630 */   306,  123,  276,   83,   88,  345,  286, 1105,  346,  347,
 /*   640 */   348,  349,  350,  351,  360,  884,  225,  264,  247,  301,
 /*   650 */   265,  883,  267,  222,  223,  268,  882,  106,  950,  949,
 /*   660 */   865,  272,  864,   70,  296,    8,  278,  758,   89,  183,
 /*   670 */   928,  927,  972,  181,  186,  182,  184,  185,  187,  142,
 /*   680 */   143,  144,   28,  919,  918,  784,  158,  145,  973,  910,
 /*   690 */   909,  795,  159,    1,   31,  169,  167,  170,  789,    2,
 /*   700 */   160,   90,  237,  791,   91,  290,   29,    9,   30,   10,
 /*   710 */    11,   25,  298,   26,   98,  100,   34,  649,  102,  103,
 /*   720 */   684,   35,  104,  682,  681,  680,  678,  677,  676,  673,
 /*   730 */   639,  317,  107,  325,  841,  326,  109,  110,    5,  111,
 /*   740 */   839,    6,   68,  113,   69,   37,  117,  119,  716,  715,
 /*   750 */   712,  665,  663,  655,  661,  657,  659,  653,  651,  686,
 /*   760 */   685,  683,  679,  675,  674,  189,  637,  602,  888,  887,
 /*   770 */   887,  887,  887,  887,  887,  887,  887,  887,  887,  887,
 /*   780 */   887,  147,  148,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   207,    1,  206,  266,    1,  266,  206,    5,  266,    9,
 /*    10 */   200,  201,    9,   13,   14,  200,   16,   17,    0,  277,
 /*    20 */    20,   21,   22,   23,    5,   25,   26,   27,   28,   29,
 /*    30 */    30,  238,  239,  240,  241,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  200,   16,   17,  248,  249,   20,
 /*    50 */    21,   22,   23,  209,   25,   26,   27,   28,   29,   30,
 /*    60 */   200,    5,  266,    7,   35,   36,  266,  266,   39,   40,
 /*    70 */    41,   13,   14,  277,   16,   17,  207,  277,   20,   21,
 /*    80 */    22,   23,  267,   25,   26,   27,   28,   29,   30,  200,
 /*    90 */    90,  200,    1,   35,   36,   86,  200,   39,   40,   41,
 /*   100 */     9,   35,   36,  200,   86,   39,   40,   41,   13,   14,
 /*   110 */   241,   16,   17,  206,   85,   20,   21,   22,   23,    1,
 /*   120 */    25,   26,   27,   28,   29,   30,  205,    9,  268,  269,
 /*   130 */    35,   36,  211,  242,   39,   40,   41,  246,   47,   48,
 /*   140 */    49,   50,   51,   52,   53,   54,   55,   56,   57,   58,
 /*   150 */    59,   60,   61,   14,   63,   16,   17,  200,  101,   20,
 /*   160 */    21,   22,   23,  274,   25,   26,   27,   28,   29,   30,
 /*   170 */   274,  245,  276,  266,   35,   36,   70,  274,   39,   40,
 /*   180 */    41,   16,   17,  127,  277,   20,   21,   22,   23,  263,
 /*   190 */    25,   26,   27,   28,   29,   30,  102,  140,  104,  105,
 /*   200 */    35,   36,   84,  109,   39,   40,   41,  113,   62,  115,
 /*   210 */   116,  102,  103,  104,  105,  106,  107,  108,  109,  110,
 /*   220 */   111,  112,  113,  114,  115,  116,  198,  199,   94,  272,
 /*   230 */    84,  274,  222,  223,  224,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,   46,    1,    2,
 /*   250 */    84,   70,    5,  147,    7,  149,    9,  151,  152,  205,
 /*   260 */     1,   39,   40,   41,   63,  211,    1,    2,    9,  200,
 /*   270 */     5,   70,    7,  245,    9,  128,  129,   76,   77,   78,
 /*   280 */    79,  266,   35,   36,   83,   84,   39,  245,  122,  266,
 /*   290 */   124,  263,  200,  200,   27,   28,   29,   30,  200,   84,
 /*   300 */    35,   36,   35,   36,  266,  263,   39,   40,   41,    2,
 /*   310 */   200,  242,    5,  200,    7,  246,    9,    2,  266,    5,
 /*   320 */     5,    7,    7,  122,    9,   67,   68,   69,  147,  200,
 /*   330 */   149,   84,  151,  152,  242,  242,  200,  122,  246,  246,
 /*   340 */   242,  200,   35,   36,  246,  144,  266,  146,   89,   84,
 /*   350 */    35,   36,  242,  200,  153,  242,  246,  249,   84,  246,
 /*   360 */   249,   64,   65,   66,   90,  266,  145,  120,  121,   72,
 /*   370 */    73,   74,   75,  244,  127,  154,  155,  222,  242,  224,
 /*   380 */   225,   39,  246,  242,  229,  120,  121,  246,  233,   92,
 /*   390 */   235,  236,  127,  119,   78,  242,   64,   65,   66,  246,
 /*   400 */   200,  266,  200,   71,   72,   73,   74,   75,   64,   65,
 /*   410 */    66,  243,   64,   65,   66,  247,  243,   73,   74,   75,
 /*   420 */   247,  207,  253,  200,  200,   80,   82,  120,  121,  250,
 /*   430 */   101,  205,  209,  209,   85,  120,  121,  211,   85,   94,
 /*   440 */   271,  127,  242,  264,   85,   85,  246,   85,  246,   85,
 /*   450 */   220,  221,  136,  239,  101,  126,  101,   85,   85,   85,
 /*   460 */    85,  101,    5,  101,    7,  101,    5,  125,    7,   85,
 /*   470 */    84,    1,  123,  101,  101,  101,  101,   85,   85,   15,
 /*   480 */    85,  148,  123,  150,  148,  101,  150,  148,    5,  150,
 /*   490 */     7,   84,  247,  101,  101,  266,  101,  142,  148,    5,
 /*   500 */   150,    7,   80,   81,  118,  148,  266,  150,  266,   39,
 /*   510 */   266,  148,  266,  150,  266,  266,  266,  266,  266,  266,
 /*   520 */   266,  249,  245,  237,  117,  237,  237,  237,  237,  237,
 /*   530 */   200,  237,  245,  265,  200,  200,  275,  200,  200,  275,
 /*   540 */   200,  200,   62,  245,  251,  245,  200,  200,  270,  270,
 /*   550 */   270,  200,  200,  270,  200,  259,  200,  262,  200,  260,
 /*   560 */   127,  200,  261,  258,  256,  200,  200,  200,  200,  200,
 /*   570 */   200,  200,  134,  132,  137,  139,  200,  200,  200,  200,
 /*   580 */   200,  200,  200,  200,  120,  200,  141,  200,  200,  200,
 /*   590 */   200,  200,  138,  131,  200,  130,  200,  200,  200,  200,
 /*   600 */   200,  200,  200,  200,  200,  200,  200,  133,  200,  200,
 /*   610 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   620 */   200,  200,  143,  200,  200,  200,  200,  200,  200,  200,
 /*   630 */    93,  100,  202,  202,  202,   99,  202,  202,   53,   96,
 /*   640 */    98,   57,   97,   95,   86,    5,  202,  156,  202,  202,
 /*   650 */     5,    5,  156,  202,  202,    5,    5,  207,  210,  210,
 /*   660 */   104,  145,  103,  123,  118,   84,  101,   85,  101,  213,
 /*   670 */   202,  202,  219,  218,  215,  217,  216,  214,  212,  203,
 /*   680 */   203,  203,   84,  202,  202,   85,   84,  203,  221,  202,
 /*   690 */   202,   85,   84,  208,  252,  255,  257,  254,   85,  204,
 /*   700 */   101,   84,    1,   85,   84,   84,  101,  135,  101,  135,
 /*   710 */    84,   84,  118,   84,  119,   80,   91,    5,   90,   72,
 /*   720 */     9,   91,   90,    5,    5,    5,    5,    5,    5,    5,
 /*   730 */    87,   15,   80,   26,  120,   61,   88,   88,   84,  150,
 /*   740 */    85,   84,   16,  150,   16,  101,  150,  150,    5,    5,
 /*   750 */    85,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   760 */     5,    5,    5,    5,    5,  101,   87,   62,    0,  278,
 /*   770 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   780 */   278,   21,   21,  278,  278,  278,  278,  278,  278,  278,
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
 /*   970 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
};
#define YY_SHIFT_COUNT    (364)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (768)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   201,  109,   94,    9,  247,  265,  265,  118,    3,    3,
 /*    10 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    20 */     3,    0,   91,  265,  307,  315,  315,  215,  215,    3,
 /*    30 */     3,  147,    3,   18,    3,    3,    3,    3,  345,    9,
 /*    40 */   134,  134,   19,  783,  265,  265,  265,  265,  265,  265,
 /*    50 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*    60 */   265,  265,  265,  265,  265,  265,  307,  315,  307,  307,
 /*    70 */   166,    2,    2,    2,    2,    2,    2,  259,    2,    3,
 /*    80 */     3,    3,  342,    3,    3,    3,  215,  215,    3,    3,
 /*    90 */     3,    3,  316,  316,  329,  215,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   110 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   120 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   130 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   140 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   150 */     3,    3,    3,    3,  480,  480,  480,  433,  433,  433,
 /*   160 */   433,  480,  480,  436,  445,  438,  454,  437,  441,  462,
 /*   170 */   465,  474,  479,  480,  480,  480,  537,  537,    9,  480,
 /*   180 */   480,  531,  536,  585,  543,  542,  584,  545,  548,   19,
 /*   190 */   480,  480,  558,  558,  480,  558,  480,  558,  480,  480,
 /*   200 */   783,  783,   29,   58,   58,   95,   58,  139,  165,  267,
 /*   210 */   267,  267,  267,  267,  267,  297,  332,  344,   66,   66,
 /*   220 */    66,   66,  106,  181,  221,  274,  222,  222,   56,  314,
 /*   230 */   258,  348,  353,  349,  359,  360,  362,  364,  355,   57,
 /*   240 */   372,  373,  374,  375,  457,  461,  384,  386,  392,  393,
 /*   250 */   470,  146,  464,  395,  333,  336,  339,  483,  494,  350,
 /*   260 */   357,  407,  363,  422,  640,  491,  645,  646,  496,  650,
 /*   270 */   651,  556,  559,  516,  540,  546,  581,  582,  598,  565,
 /*   280 */   567,  600,  602,  606,  608,  613,  599,  617,  618,  620,
 /*   290 */   701,  621,  605,  572,  607,  574,  626,  546,  627,  594,
 /*   300 */   629,  595,  635,  625,  628,  647,  712,  630,  632,  711,
 /*   310 */   718,  719,  720,  721,  722,  723,  724,  643,  716,  652,
 /*   320 */   648,  649,  654,  655,  614,  657,  707,  674,  726,  589,
 /*   330 */   593,  644,  644,  644,  644,  728,  596,  597,  644,  644,
 /*   340 */   644,  743,  744,  665,  644,  746,  747,  748,  749,  750,
 /*   350 */   751,  752,  753,  754,  755,  756,  757,  758,  759,  664,
 /*   360 */   679,  760,  761,  705,  768,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-263)
#define YY_REDUCE_MAX   (495)
static const short yy_reduce_ofst[] = {
 /*     0 */    28,   10,  155, -207, -204, -200,  -93, -140, -109, -104,
 /*    10 */   -43,   69,   92,   93,   98,  110,  113,  136,  141,  153,
 /*    20 */   200, -185, -190, -258, -201,  168,  173,  -74,   42, -111,
 /*    30 */   -97,  169,  129, -131, -156,  223,  224,  202,  -79,  214,
 /*    40 */    54,  226,  230,  179, -263, -261, -199,   15,   23,   38,
 /*    50 */    52,   80,   99,  135,  229,  240,  242,  244,  246,  248,
 /*    60 */   249,  250,  251,  252,  253,  254,  108,  245,  111,  272,
 /*    70 */   277,  286,  288,  289,  290,  291,  292,  330,  294,  334,
 /*    80 */   335,  337,  268,  338,  340,  341,  287,  298,  346,  347,
 /*    90 */   351,  352,  261,  264,  293,  300,  354,  356,  358,  361,
 /*   100 */   365,  366,  367,  368,  369,  370,  371,  376,  377,  378,
 /*   110 */   379,  380,  381,  382,  383,  385,  387,  388,  389,  390,
 /*   120 */   391,  394,  396,  397,  398,  399,  400,  401,  402,  403,
 /*   130 */   404,  405,  406,  408,  409,  410,  411,  412,  413,  414,
 /*   140 */   415,  416,  417,  418,  419,  420,  421,  423,  424,  425,
 /*   150 */   426,  427,  428,  429,  430,  431,  432,  278,  279,  280,
 /*   160 */   283,  434,  435,  295,  301,  299,  296,  305,  439,  308,
 /*   170 */   440,  443,  442,  444,  446,  447,  448,  449,  450,  451,
 /*   180 */   452,  453,  455,  458,  456,  460,  463,  459,  466,  467,
 /*   190 */   468,  469,  476,  477,  481,  478,  482,  484,  487,  488,
 /*   200 */   485,  495,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   885,  948,  935,  944, 1167, 1167, 1167,  885,  885,  885,
 /*    10 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    20 */   885, 1074,  905, 1167,  885,  885,  885,  885,  885,  885,
 /*    30 */   885, 1089,  885,  944,  885,  885,  885,  885,  954,  944,
 /*    40 */   954,  954,  885, 1069,  885,  885,  885,  885,  885,  885,
 /*    50 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    60 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    70 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    80 */   885,  885, 1076, 1082, 1079,  885,  885,  885, 1084,  885,
 /*    90 */   885,  885, 1108, 1108, 1067,  885,  885,  885,  885,  885,
 /*   100 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   110 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   120 */   885,  885,  885,  885,  885,  885,  885,  933,  885,  931,
 /*   130 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   140 */   885,  885,  885,  885,  885,  885,  916,  885,  885,  885,
 /*   150 */   885,  885,  885,  903,  907,  907,  907,  885,  885,  885,
 /*   160 */   885,  907,  907, 1115, 1119, 1101, 1113, 1109, 1096, 1094,
 /*   170 */  1092, 1100, 1123,  907,  907,  907,  952,  952,  944,  907,
 /*   180 */   907,  970,  968,  966,  958,  964,  960,  962,  956,  885,
 /*   190 */   907,  907,  942,  942,  907,  942,  907,  942,  907,  907,
 /*   200 */   991, 1007,  885, 1124, 1114,  885, 1166, 1154, 1153, 1162,
 /*   210 */  1161, 1160, 1152, 1151, 1150,  885,  885,  885, 1146, 1149,
 /*   220 */  1148, 1147,  885,  885,  885,  885, 1156, 1155,  885,  885,
 /*   230 */   885,  885,  885,  885,  885,  885,  885,  885, 1120, 1116,
 /*   240 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   250 */   885, 1126,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   260 */   885, 1015,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   270 */   885,  885,  885,  885, 1066,  885,  885,  885,  885, 1078,
 /*   280 */  1077,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   290 */   885,  885, 1110,  885, 1102,  885,  885, 1027,  885,  885,
 /*   300 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   310 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   320 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   330 */   885, 1185, 1180, 1181, 1178,  885,  885,  885, 1177, 1172,
 /*   340 */  1173,  885,  885,  885, 1170,  885,  885,  885,  885,  885,
 /*   350 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  976,
 /*   360 */   885,  914,  912,  885,  885,
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
  /*   88 */ "PORT",
  /*   89 */ "IPTOKEN",
  /*   90 */ "AS",
  /*   91 */ "OUTPUTTYPE",
  /*   92 */ "AGGREGATE",
  /*   93 */ "BUFSIZE",
  /*   94 */ "PPS",
  /*   95 */ "TSERIES",
  /*   96 */ "DBS",
  /*   97 */ "STORAGE",
  /*   98 */ "QTIME",
  /*   99 */ "CONNS",
  /*  100 */ "STATE",
  /*  101 */ "COMMA",
  /*  102 */ "KEEP",
  /*  103 */ "CACHE",
  /*  104 */ "REPLICA",
  /*  105 */ "QUORUM",
  /*  106 */ "DAYS",
  /*  107 */ "MINROWS",
  /*  108 */ "MAXROWS",
  /*  109 */ "BLOCKS",
  /*  110 */ "CTIME",
  /*  111 */ "WAL",
  /*  112 */ "FSYNC",
  /*  113 */ "COMP",
  /*  114 */ "PRECISION",
  /*  115 */ "UPDATE",
  /*  116 */ "CACHELAST",
  /*  117 */ "UNSIGNED",
  /*  118 */ "TAGS",
  /*  119 */ "USING",
  /*  120 */ "NULL",
  /*  121 */ "NOW",
  /*  122 */ "SELECT",
  /*  123 */ "UNION",
  /*  124 */ "ALL",
  /*  125 */ "DISTINCT",
  /*  126 */ "FROM",
  /*  127 */ "VARIABLE",
  /*  128 */ "INTERVAL",
  /*  129 */ "EVERY",
  /*  130 */ "SESSION",
  /*  131 */ "STATE_WINDOW",
  /*  132 */ "FILL",
  /*  133 */ "SLIDING",
  /*  134 */ "ORDER",
  /*  135 */ "BY",
  /*  136 */ "ASC",
  /*  137 */ "GROUP",
  /*  138 */ "HAVING",
  /*  139 */ "LIMIT",
  /*  140 */ "OFFSET",
  /*  141 */ "SLIMIT",
  /*  142 */ "SOFFSET",
  /*  143 */ "WHERE",
  /*  144 */ "RESET",
  /*  145 */ "QUERY",
  /*  146 */ "SYNCDB",
  /*  147 */ "ADD",
  /*  148 */ "COLUMN",
  /*  149 */ "MODIFY",
  /*  150 */ "TAG",
  /*  151 */ "CHANGE",
  /*  152 */ "SET",
  /*  153 */ "KILL",
  /*  154 */ "CONNECTION",
  /*  155 */ "STREAM",
  /*  156 */ "COLON",
  /*  157 */ "ABORT",
  /*  158 */ "AFTER",
  /*  159 */ "ATTACH",
  /*  160 */ "BEFORE",
  /*  161 */ "BEGIN",
  /*  162 */ "CASCADE",
  /*  163 */ "CLUSTER",
  /*  164 */ "CONFLICT",
  /*  165 */ "COPY",
  /*  166 */ "DEFERRED",
  /*  167 */ "DELIMITERS",
  /*  168 */ "DETACH",
  /*  169 */ "EACH",
  /*  170 */ "END",
  /*  171 */ "EXPLAIN",
  /*  172 */ "FAIL",
  /*  173 */ "FOR",
  /*  174 */ "IGNORE",
  /*  175 */ "IMMEDIATE",
  /*  176 */ "INITIALLY",
  /*  177 */ "INSTEAD",
  /*  178 */ "KEY",
  /*  179 */ "OF",
  /*  180 */ "RAISE",
  /*  181 */ "REPLACE",
  /*  182 */ "RESTRICT",
  /*  183 */ "ROW",
  /*  184 */ "STATEMENT",
  /*  185 */ "TRIGGER",
  /*  186 */ "VIEW",
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
  /*  205 */ "acct_optr",
  /*  206 */ "exprlist",
  /*  207 */ "ifnotexists",
  /*  208 */ "db_optr",
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
  /*  237 */ "signed",
  /*  238 */ "create_table_args",
  /*  239 */ "create_stable_args",
  /*  240 */ "create_table_list",
  /*  241 */ "create_from_stable",
  /*  242 */ "columnlist",
  /*  243 */ "tagitemlist1",
  /*  244 */ "tagNamelist",
  /*  245 */ "select",
  /*  246 */ "column",
  /*  247 */ "tagitem1",
  /*  248 */ "tagitemlist",
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
 /* 103 */ "db_optr ::=",
 /* 104 */ "db_optr ::= db_optr cache",
 /* 105 */ "db_optr ::= db_optr replica",
 /* 106 */ "db_optr ::= db_optr quorum",
 /* 107 */ "db_optr ::= db_optr days",
 /* 108 */ "db_optr ::= db_optr minrows",
 /* 109 */ "db_optr ::= db_optr maxrows",
 /* 110 */ "db_optr ::= db_optr blocks",
 /* 111 */ "db_optr ::= db_optr ctime",
 /* 112 */ "db_optr ::= db_optr wal",
 /* 113 */ "db_optr ::= db_optr fsync",
 /* 114 */ "db_optr ::= db_optr comp",
 /* 115 */ "db_optr ::= db_optr prec",
 /* 116 */ "db_optr ::= db_optr keep",
 /* 117 */ "db_optr ::= db_optr update",
 /* 118 */ "db_optr ::= db_optr cachelast",
 /* 119 */ "alter_db_optr ::=",
 /* 120 */ "alter_db_optr ::= alter_db_optr replica",
 /* 121 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 122 */ "alter_db_optr ::= alter_db_optr keep",
 /* 123 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 124 */ "alter_db_optr ::= alter_db_optr comp",
 /* 125 */ "alter_db_optr ::= alter_db_optr update",
 /* 126 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 127 */ "typename ::= ids",
 /* 128 */ "typename ::= ids LP signed RP",
 /* 129 */ "typename ::= ids UNSIGNED",
 /* 130 */ "signed ::= INTEGER",
 /* 131 */ "signed ::= PLUS INTEGER",
 /* 132 */ "signed ::= MINUS INTEGER",
 /* 133 */ "cmd ::= CREATE TABLE create_table_args",
 /* 134 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE TABLE create_table_list",
 /* 137 */ "create_table_list ::= create_from_stable",
 /* 138 */ "create_table_list ::= create_table_list create_from_stable",
 /* 139 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 140 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 143 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 144 */ "tagNamelist ::= ids",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 146 */ "columnlist ::= columnlist COMMA column",
 /* 147 */ "columnlist ::= column",
 /* 148 */ "column ::= ids typename",
 /* 149 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 150 */ "tagitemlist1 ::= tagitem1",
 /* 151 */ "tagitem1 ::= MINUS INTEGER",
 /* 152 */ "tagitem1 ::= MINUS FLOAT",
 /* 153 */ "tagitem1 ::= PLUS INTEGER",
 /* 154 */ "tagitem1 ::= PLUS FLOAT",
 /* 155 */ "tagitem1 ::= INTEGER",
 /* 156 */ "tagitem1 ::= FLOAT",
 /* 157 */ "tagitem1 ::= STRING",
 /* 158 */ "tagitem1 ::= BOOL",
 /* 159 */ "tagitem1 ::= NULL",
 /* 160 */ "tagitem1 ::= NOW",
 /* 161 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 162 */ "tagitemlist ::= tagitem",
 /* 163 */ "tagitem ::= INTEGER",
 /* 164 */ "tagitem ::= FLOAT",
 /* 165 */ "tagitem ::= STRING",
 /* 166 */ "tagitem ::= BOOL",
 /* 167 */ "tagitem ::= NULL",
 /* 168 */ "tagitem ::= NOW",
 /* 169 */ "tagitem ::= MINUS INTEGER",
 /* 170 */ "tagitem ::= MINUS FLOAT",
 /* 171 */ "tagitem ::= PLUS INTEGER",
 /* 172 */ "tagitem ::= PLUS FLOAT",
 /* 173 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 174 */ "select ::= LP select RP",
 /* 175 */ "union ::= select",
 /* 176 */ "union ::= union UNION ALL select",
 /* 177 */ "union ::= union UNION select",
 /* 178 */ "cmd ::= union",
 /* 179 */ "select ::= SELECT selcollist",
 /* 180 */ "sclp ::= selcollist COMMA",
 /* 181 */ "sclp ::=",
 /* 182 */ "selcollist ::= sclp distinct expr as",
 /* 183 */ "selcollist ::= sclp STAR",
 /* 184 */ "as ::= AS ids",
 /* 185 */ "as ::= ids",
 /* 186 */ "as ::=",
 /* 187 */ "distinct ::= DISTINCT",
 /* 188 */ "distinct ::=",
 /* 189 */ "from ::= FROM tablelist",
 /* 190 */ "from ::= FROM sub",
 /* 191 */ "sub ::= LP union RP",
 /* 192 */ "sub ::= LP union RP ids",
 /* 193 */ "sub ::= sub COMMA LP union RP ids",
 /* 194 */ "tablelist ::= ids cpxName",
 /* 195 */ "tablelist ::= ids cpxName ids",
 /* 196 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 197 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 198 */ "tmvar ::= VARIABLE",
 /* 199 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 200 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 201 */ "interval_option ::=",
 /* 202 */ "intervalKey ::= INTERVAL",
 /* 203 */ "intervalKey ::= EVERY",
 /* 204 */ "session_option ::=",
 /* 205 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 206 */ "windowstate_option ::=",
 /* 207 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 208 */ "fill_opt ::=",
 /* 209 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 210 */ "fill_opt ::= FILL LP ID RP",
 /* 211 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 212 */ "sliding_opt ::=",
 /* 213 */ "orderby_opt ::=",
 /* 214 */ "orderby_opt ::= ORDER BY sortlist",
 /* 215 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 216 */ "sortlist ::= item sortorder",
 /* 217 */ "item ::= ids cpxName",
 /* 218 */ "sortorder ::= ASC",
 /* 219 */ "sortorder ::= DESC",
 /* 220 */ "sortorder ::=",
 /* 221 */ "groupby_opt ::=",
 /* 222 */ "groupby_opt ::= GROUP BY grouplist",
 /* 223 */ "grouplist ::= grouplist COMMA item",
 /* 224 */ "grouplist ::= item",
 /* 225 */ "having_opt ::=",
 /* 226 */ "having_opt ::= HAVING expr",
 /* 227 */ "limit_opt ::=",
 /* 228 */ "limit_opt ::= LIMIT signed",
 /* 229 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 230 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 231 */ "slimit_opt ::=",
 /* 232 */ "slimit_opt ::= SLIMIT signed",
 /* 233 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 234 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 235 */ "where_opt ::=",
 /* 236 */ "where_opt ::= WHERE expr",
 /* 237 */ "expr ::= LP expr RP",
 /* 238 */ "expr ::= ID",
 /* 239 */ "expr ::= ID DOT ID",
 /* 240 */ "expr ::= ID DOT STAR",
 /* 241 */ "expr ::= INTEGER",
 /* 242 */ "expr ::= MINUS INTEGER",
 /* 243 */ "expr ::= PLUS INTEGER",
 /* 244 */ "expr ::= FLOAT",
 /* 245 */ "expr ::= MINUS FLOAT",
 /* 246 */ "expr ::= PLUS FLOAT",
 /* 247 */ "expr ::= STRING",
 /* 248 */ "expr ::= NOW",
 /* 249 */ "expr ::= VARIABLE",
 /* 250 */ "expr ::= PLUS VARIABLE",
 /* 251 */ "expr ::= MINUS VARIABLE",
 /* 252 */ "expr ::= BOOL",
 /* 253 */ "expr ::= NULL",
 /* 254 */ "expr ::= ID LP exprlist RP",
 /* 255 */ "expr ::= ID LP STAR RP",
 /* 256 */ "expr ::= expr IS NULL",
 /* 257 */ "expr ::= expr IS NOT NULL",
 /* 258 */ "expr ::= expr LT expr",
 /* 259 */ "expr ::= expr GT expr",
 /* 260 */ "expr ::= expr LE expr",
 /* 261 */ "expr ::= expr GE expr",
 /* 262 */ "expr ::= expr NE expr",
 /* 263 */ "expr ::= expr EQ expr",
 /* 264 */ "expr ::= expr BETWEEN expr AND expr",
 /* 265 */ "expr ::= expr AND expr",
 /* 266 */ "expr ::= expr OR expr",
 /* 267 */ "expr ::= expr PLUS expr",
 /* 268 */ "expr ::= expr MINUS expr",
 /* 269 */ "expr ::= expr STAR expr",
 /* 270 */ "expr ::= expr SLASH expr",
 /* 271 */ "expr ::= expr REM expr",
 /* 272 */ "expr ::= expr LIKE expr",
 /* 273 */ "expr ::= expr MATCH expr",
 /* 274 */ "expr ::= expr NMATCH expr",
 /* 275 */ "expr ::= expr IN LP exprlist RP",
 /* 276 */ "exprlist ::= exprlist COMMA expritem",
 /* 277 */ "exprlist ::= expritem",
 /* 278 */ "expritem ::= expr",
 /* 279 */ "expritem ::=",
 /* 280 */ "cmd ::= RESET QUERY CACHE",
 /* 281 */ "cmd ::= SYNCDB ids REPLICA",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 298 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 299 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 300 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 220: /* intitemlist */
    case 222: /* keep */
    case 242: /* columnlist */
    case 243: /* tagitemlist1 */
    case 244: /* tagNamelist */
    case 248: /* tagitemlist */
    case 257: /* fill_opt */
    case 258: /* groupby_opt */
    case 260: /* orderby_opt */
    case 272: /* sortlist */
    case 276: /* grouplist */
{
taosArrayDestroy((yypminor->yy421));
}
      break;
    case 240: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy438));
}
      break;
    case 245: /* select */
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
destroyAllSqlNode((yypminor->yy149));
}
      break;
    case 273: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy69));
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
  {  199,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  199,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  199,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  200,   -1 }, /* (51) ids ::= ID */
  {  200,   -1 }, /* (52) ids ::= STRING */
  {  203,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  203,    0 }, /* (54) ifexists ::= */
  {  207,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  207,    0 }, /* (56) ifnotexists ::= */
  {  199,   -5 }, /* (57) cmd ::= CREATE DNODE ids PORT ids */
  {  199,   -5 }, /* (58) cmd ::= CREATE DNODE IPTOKEN PORT ids */
  {  199,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  199,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  199,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
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
  {  205,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
  {  208,    0 }, /* (103) db_optr ::= */
  {  208,   -2 }, /* (104) db_optr ::= db_optr cache */
  {  208,   -2 }, /* (105) db_optr ::= db_optr replica */
  {  208,   -2 }, /* (106) db_optr ::= db_optr quorum */
  {  208,   -2 }, /* (107) db_optr ::= db_optr days */
  {  208,   -2 }, /* (108) db_optr ::= db_optr minrows */
  {  208,   -2 }, /* (109) db_optr ::= db_optr maxrows */
  {  208,   -2 }, /* (110) db_optr ::= db_optr blocks */
  {  208,   -2 }, /* (111) db_optr ::= db_optr ctime */
  {  208,   -2 }, /* (112) db_optr ::= db_optr wal */
  {  208,   -2 }, /* (113) db_optr ::= db_optr fsync */
  {  208,   -2 }, /* (114) db_optr ::= db_optr comp */
  {  208,   -2 }, /* (115) db_optr ::= db_optr prec */
  {  208,   -2 }, /* (116) db_optr ::= db_optr keep */
  {  208,   -2 }, /* (117) db_optr ::= db_optr update */
  {  208,   -2 }, /* (118) db_optr ::= db_optr cachelast */
  {  204,    0 }, /* (119) alter_db_optr ::= */
  {  204,   -2 }, /* (120) alter_db_optr ::= alter_db_optr replica */
  {  204,   -2 }, /* (121) alter_db_optr ::= alter_db_optr quorum */
  {  204,   -2 }, /* (122) alter_db_optr ::= alter_db_optr keep */
  {  204,   -2 }, /* (123) alter_db_optr ::= alter_db_optr blocks */
  {  204,   -2 }, /* (124) alter_db_optr ::= alter_db_optr comp */
  {  204,   -2 }, /* (125) alter_db_optr ::= alter_db_optr update */
  {  204,   -2 }, /* (126) alter_db_optr ::= alter_db_optr cachelast */
  {  209,   -1 }, /* (127) typename ::= ids */
  {  209,   -4 }, /* (128) typename ::= ids LP signed RP */
  {  209,   -2 }, /* (129) typename ::= ids UNSIGNED */
  {  237,   -1 }, /* (130) signed ::= INTEGER */
  {  237,   -2 }, /* (131) signed ::= PLUS INTEGER */
  {  237,   -2 }, /* (132) signed ::= MINUS INTEGER */
  {  199,   -3 }, /* (133) cmd ::= CREATE TABLE create_table_args */
  {  199,   -3 }, /* (134) cmd ::= CREATE TABLE create_stable_args */
  {  199,   -3 }, /* (135) cmd ::= CREATE STABLE create_stable_args */
  {  199,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_list */
  {  240,   -1 }, /* (137) create_table_list ::= create_from_stable */
  {  240,   -2 }, /* (138) create_table_list ::= create_table_list create_from_stable */
  {  238,   -6 }, /* (139) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  239,  -10 }, /* (140) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  241,  -10 }, /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  241,  -13 }, /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  244,   -3 }, /* (143) tagNamelist ::= tagNamelist COMMA ids */
  {  244,   -1 }, /* (144) tagNamelist ::= ids */
  {  238,   -5 }, /* (145) create_table_args ::= ifnotexists ids cpxName AS select */
  {  242,   -3 }, /* (146) columnlist ::= columnlist COMMA column */
  {  242,   -1 }, /* (147) columnlist ::= column */
  {  246,   -2 }, /* (148) column ::= ids typename */
  {  243,   -3 }, /* (149) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  243,   -1 }, /* (150) tagitemlist1 ::= tagitem1 */
  {  247,   -2 }, /* (151) tagitem1 ::= MINUS INTEGER */
  {  247,   -2 }, /* (152) tagitem1 ::= MINUS FLOAT */
  {  247,   -2 }, /* (153) tagitem1 ::= PLUS INTEGER */
  {  247,   -2 }, /* (154) tagitem1 ::= PLUS FLOAT */
  {  247,   -1 }, /* (155) tagitem1 ::= INTEGER */
  {  247,   -1 }, /* (156) tagitem1 ::= FLOAT */
  {  247,   -1 }, /* (157) tagitem1 ::= STRING */
  {  247,   -1 }, /* (158) tagitem1 ::= BOOL */
  {  247,   -1 }, /* (159) tagitem1 ::= NULL */
  {  247,   -1 }, /* (160) tagitem1 ::= NOW */
  {  248,   -3 }, /* (161) tagitemlist ::= tagitemlist COMMA tagitem */
  {  248,   -1 }, /* (162) tagitemlist ::= tagitem */
  {  249,   -1 }, /* (163) tagitem ::= INTEGER */
  {  249,   -1 }, /* (164) tagitem ::= FLOAT */
  {  249,   -1 }, /* (165) tagitem ::= STRING */
  {  249,   -1 }, /* (166) tagitem ::= BOOL */
  {  249,   -1 }, /* (167) tagitem ::= NULL */
  {  249,   -1 }, /* (168) tagitem ::= NOW */
  {  249,   -2 }, /* (169) tagitem ::= MINUS INTEGER */
  {  249,   -2 }, /* (170) tagitem ::= MINUS FLOAT */
  {  249,   -2 }, /* (171) tagitem ::= PLUS INTEGER */
  {  249,   -2 }, /* (172) tagitem ::= PLUS FLOAT */
  {  245,  -14 }, /* (173) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  245,   -3 }, /* (174) select ::= LP select RP */
  {  263,   -1 }, /* (175) union ::= select */
  {  263,   -4 }, /* (176) union ::= union UNION ALL select */
  {  263,   -3 }, /* (177) union ::= union UNION select */
  {  199,   -1 }, /* (178) cmd ::= union */
  {  245,   -2 }, /* (179) select ::= SELECT selcollist */
  {  264,   -2 }, /* (180) sclp ::= selcollist COMMA */
  {  264,    0 }, /* (181) sclp ::= */
  {  250,   -4 }, /* (182) selcollist ::= sclp distinct expr as */
  {  250,   -2 }, /* (183) selcollist ::= sclp STAR */
  {  267,   -2 }, /* (184) as ::= AS ids */
  {  267,   -1 }, /* (185) as ::= ids */
  {  267,    0 }, /* (186) as ::= */
  {  265,   -1 }, /* (187) distinct ::= DISTINCT */
  {  265,    0 }, /* (188) distinct ::= */
  {  251,   -2 }, /* (189) from ::= FROM tablelist */
  {  251,   -2 }, /* (190) from ::= FROM sub */
  {  269,   -3 }, /* (191) sub ::= LP union RP */
  {  269,   -4 }, /* (192) sub ::= LP union RP ids */
  {  269,   -6 }, /* (193) sub ::= sub COMMA LP union RP ids */
  {  268,   -2 }, /* (194) tablelist ::= ids cpxName */
  {  268,   -3 }, /* (195) tablelist ::= ids cpxName ids */
  {  268,   -4 }, /* (196) tablelist ::= tablelist COMMA ids cpxName */
  {  268,   -5 }, /* (197) tablelist ::= tablelist COMMA ids cpxName ids */
  {  270,   -1 }, /* (198) tmvar ::= VARIABLE */
  {  253,   -4 }, /* (199) interval_option ::= intervalKey LP tmvar RP */
  {  253,   -6 }, /* (200) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  253,    0 }, /* (201) interval_option ::= */
  {  271,   -1 }, /* (202) intervalKey ::= INTERVAL */
  {  271,   -1 }, /* (203) intervalKey ::= EVERY */
  {  255,    0 }, /* (204) session_option ::= */
  {  255,   -7 }, /* (205) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  256,    0 }, /* (206) windowstate_option ::= */
  {  256,   -4 }, /* (207) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  257,    0 }, /* (208) fill_opt ::= */
  {  257,   -6 }, /* (209) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  257,   -4 }, /* (210) fill_opt ::= FILL LP ID RP */
  {  254,   -4 }, /* (211) sliding_opt ::= SLIDING LP tmvar RP */
  {  254,    0 }, /* (212) sliding_opt ::= */
  {  260,    0 }, /* (213) orderby_opt ::= */
  {  260,   -3 }, /* (214) orderby_opt ::= ORDER BY sortlist */
  {  272,   -4 }, /* (215) sortlist ::= sortlist COMMA item sortorder */
  {  272,   -2 }, /* (216) sortlist ::= item sortorder */
  {  274,   -2 }, /* (217) item ::= ids cpxName */
  {  275,   -1 }, /* (218) sortorder ::= ASC */
  {  275,   -1 }, /* (219) sortorder ::= DESC */
  {  275,    0 }, /* (220) sortorder ::= */
  {  258,    0 }, /* (221) groupby_opt ::= */
  {  258,   -3 }, /* (222) groupby_opt ::= GROUP BY grouplist */
  {  276,   -3 }, /* (223) grouplist ::= grouplist COMMA item */
  {  276,   -1 }, /* (224) grouplist ::= item */
  {  259,    0 }, /* (225) having_opt ::= */
  {  259,   -2 }, /* (226) having_opt ::= HAVING expr */
  {  262,    0 }, /* (227) limit_opt ::= */
  {  262,   -2 }, /* (228) limit_opt ::= LIMIT signed */
  {  262,   -4 }, /* (229) limit_opt ::= LIMIT signed OFFSET signed */
  {  262,   -4 }, /* (230) limit_opt ::= LIMIT signed COMMA signed */
  {  261,    0 }, /* (231) slimit_opt ::= */
  {  261,   -2 }, /* (232) slimit_opt ::= SLIMIT signed */
  {  261,   -4 }, /* (233) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  261,   -4 }, /* (234) slimit_opt ::= SLIMIT signed COMMA signed */
  {  252,    0 }, /* (235) where_opt ::= */
  {  252,   -2 }, /* (236) where_opt ::= WHERE expr */
  {  266,   -3 }, /* (237) expr ::= LP expr RP */
  {  266,   -1 }, /* (238) expr ::= ID */
  {  266,   -3 }, /* (239) expr ::= ID DOT ID */
  {  266,   -3 }, /* (240) expr ::= ID DOT STAR */
  {  266,   -1 }, /* (241) expr ::= INTEGER */
  {  266,   -2 }, /* (242) expr ::= MINUS INTEGER */
  {  266,   -2 }, /* (243) expr ::= PLUS INTEGER */
  {  266,   -1 }, /* (244) expr ::= FLOAT */
  {  266,   -2 }, /* (245) expr ::= MINUS FLOAT */
  {  266,   -2 }, /* (246) expr ::= PLUS FLOAT */
  {  266,   -1 }, /* (247) expr ::= STRING */
  {  266,   -1 }, /* (248) expr ::= NOW */
  {  266,   -1 }, /* (249) expr ::= VARIABLE */
  {  266,   -2 }, /* (250) expr ::= PLUS VARIABLE */
  {  266,   -2 }, /* (251) expr ::= MINUS VARIABLE */
  {  266,   -1 }, /* (252) expr ::= BOOL */
  {  266,   -1 }, /* (253) expr ::= NULL */
  {  266,   -4 }, /* (254) expr ::= ID LP exprlist RP */
  {  266,   -4 }, /* (255) expr ::= ID LP STAR RP */
  {  266,   -3 }, /* (256) expr ::= expr IS NULL */
  {  266,   -4 }, /* (257) expr ::= expr IS NOT NULL */
  {  266,   -3 }, /* (258) expr ::= expr LT expr */
  {  266,   -3 }, /* (259) expr ::= expr GT expr */
  {  266,   -3 }, /* (260) expr ::= expr LE expr */
  {  266,   -3 }, /* (261) expr ::= expr GE expr */
  {  266,   -3 }, /* (262) expr ::= expr NE expr */
  {  266,   -3 }, /* (263) expr ::= expr EQ expr */
  {  266,   -5 }, /* (264) expr ::= expr BETWEEN expr AND expr */
  {  266,   -3 }, /* (265) expr ::= expr AND expr */
  {  266,   -3 }, /* (266) expr ::= expr OR expr */
  {  266,   -3 }, /* (267) expr ::= expr PLUS expr */
  {  266,   -3 }, /* (268) expr ::= expr MINUS expr */
  {  266,   -3 }, /* (269) expr ::= expr STAR expr */
  {  266,   -3 }, /* (270) expr ::= expr SLASH expr */
  {  266,   -3 }, /* (271) expr ::= expr REM expr */
  {  266,   -3 }, /* (272) expr ::= expr LIKE expr */
  {  266,   -3 }, /* (273) expr ::= expr MATCH expr */
  {  266,   -3 }, /* (274) expr ::= expr NMATCH expr */
  {  266,   -5 }, /* (275) expr ::= expr IN LP exprlist RP */
  {  206,   -3 }, /* (276) exprlist ::= exprlist COMMA expritem */
  {  206,   -1 }, /* (277) exprlist ::= expritem */
  {  277,   -1 }, /* (278) expritem ::= expr */
  {  277,    0 }, /* (279) expritem ::= */
  {  199,   -3 }, /* (280) cmd ::= RESET QUERY CACHE */
  {  199,   -3 }, /* (281) cmd ::= SYNCDB ids REPLICA */
  {  199,   -7 }, /* (282) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (286) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (287) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (288) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (289) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -7 }, /* (290) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (294) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (295) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (296) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (297) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -3 }, /* (298) cmd ::= KILL CONNECTION INTEGER */
  {  199,   -5 }, /* (299) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  199,   -5 }, /* (300) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 133: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==135);
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
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy171);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy421);}
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
      case 188: /* distinct ::= */ yytestcase(yyruleno==188);
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy100, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy100, &yymsp[0].minor.yy0, 2);}
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
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 161: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy421 = tListItemAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy69, -1);    }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 86: /* intitemlist ::= intitem */
      case 162: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==162);
{ yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[0].minor.yy69, -1); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 87: /* intitem ::= INTEGER */
      case 163: /* tagitem ::= INTEGER */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= STRING */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= BOOL */ yytestcase(yyruleno==166);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 103: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy90);}
        break;
      case 104: /* db_optr ::= db_optr cache */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 105: /* db_optr ::= db_optr replica */
      case 120: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==120);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 106: /* db_optr ::= db_optr quorum */
      case 121: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==121);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 107: /* db_optr ::= db_optr days */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 108: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 109: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 110: /* db_optr ::= db_optr blocks */
      case 123: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==123);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 111: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 112: /* db_optr ::= db_optr wal */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 113: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 114: /* db_optr ::= db_optr comp */
      case 124: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==124);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 115: /* db_optr ::= db_optr prec */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 116: /* db_optr ::= db_optr keep */
      case 122: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==122);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.keep = yymsp[0].minor.yy421; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 117: /* db_optr ::= db_optr update */
      case 125: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==125);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 118: /* db_optr ::= db_optr cachelast */
      case 126: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==126);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 119: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy90);}
        break;
      case 127: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy100, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy100 = yylhsminor.yy100;
        break;
      case 128: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy325 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy100, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy325;  // negative value of name length
    tSetColumnType(&yylhsminor.yy100, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy100 = yylhsminor.yy100;
        break;
      case 129: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy100, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 130: /* signed ::= INTEGER */
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 131: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 132: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 136: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
        break;
      case 137: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_CTABLE;
  yylhsminor.yy438 = pCreateTable;
}
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 138: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 139: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 140: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy152 = yylhsminor.yy152;
        break;
      case 143: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 144: /* tagNamelist ::= ids */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy56, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 146: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy100); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 147: /* columnlist ::= column */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy100);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 148: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy100, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy100);
}
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 149: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 150: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy421 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 151: /* tagitem1 ::= MINUS INTEGER */
      case 152: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==152);
      case 153: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==153);
      case 154: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==154);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 155: /* tagitem1 ::= INTEGER */
      case 156: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==156);
      case 157: /* tagitem1 ::= STRING */ yytestcase(yyruleno==157);
      case 158: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==158);
      case 159: /* tagitem1 ::= NULL */ yytestcase(yyruleno==159);
      case 160: /* tagitem1 ::= NOW */ yytestcase(yyruleno==160);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 167: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 168: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 169: /* tagitem ::= MINUS INTEGER */
      case 170: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==170);
      case 171: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==171);
      case 172: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==172);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy69, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy69 = yylhsminor.yy69;
        break;
      case 173: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy421, yymsp[-11].minor.yy8, yymsp[-10].minor.yy439, yymsp[-4].minor.yy421, yymsp[-2].minor.yy421, &yymsp[-9].minor.yy400, &yymsp[-7].minor.yy147, &yymsp[-6].minor.yy40, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, &yymsp[0].minor.yy231, &yymsp[-1].minor.yy231, yymsp[-3].minor.yy439);
}
  yymsp[-13].minor.yy56 = yylhsminor.yy56;
        break;
      case 174: /* select ::= LP select RP */
{yymsp[-2].minor.yy56 = yymsp[-1].minor.yy56;}
        break;
      case 175: /* union ::= select */
{ yylhsminor.yy149 = setSubclause(NULL, yymsp[0].minor.yy56); }
  yymsp[0].minor.yy149 = yylhsminor.yy149;
        break;
      case 176: /* union ::= union UNION ALL select */
{ yylhsminor.yy149 = appendSelectClause(yymsp[-3].minor.yy149, SQL_TYPE_UNIONALL, yymsp[0].minor.yy56);  }
  yymsp[-3].minor.yy149 = yylhsminor.yy149;
        break;
      case 177: /* union ::= union UNION select */
{ yylhsminor.yy149 = appendSelectClause(yymsp[-2].minor.yy149, SQL_TYPE_UNION, yymsp[0].minor.yy56);  }
  yymsp[-2].minor.yy149 = yylhsminor.yy149;
        break;
      case 178: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy149, NULL, TSDB_SQL_SELECT); }
        break;
      case 179: /* select ::= SELECT selcollist */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy421, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 180: /* sclp ::= selcollist COMMA */
{yylhsminor.yy421 = yymsp[-1].minor.yy421;}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 181: /* sclp ::= */
      case 213: /* orderby_opt ::= */ yytestcase(yyruleno==213);
{yymsp[1].minor.yy421 = 0;}
        break;
      case 182: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-3].minor.yy421, yymsp[-1].minor.yy439,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 183: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-1].minor.yy421, pNode, 0, 0);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 184: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 185: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 186: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 187: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 189: /* from ::= FROM tablelist */
      case 190: /* from ::= FROM sub */ yytestcase(yyruleno==190);
{yymsp[-1].minor.yy8 = yymsp[0].minor.yy8;}
        break;
      case 191: /* sub ::= LP union RP */
{yymsp[-2].minor.yy8 = addSubquery(NULL, yymsp[-1].minor.yy149, NULL);}
        break;
      case 192: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy8 = addSubquery(NULL, yymsp[-2].minor.yy149, &yymsp[0].minor.yy0);}
        break;
      case 193: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy8 = addSubquery(yymsp[-5].minor.yy8, yymsp[-2].minor.yy149, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy8 = yylhsminor.yy8;
        break;
      case 194: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy8 = yylhsminor.yy8;
        break;
      case 195: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 196: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-3].minor.yy8, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy8 = yylhsminor.yy8;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-4].minor.yy8, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy8 = yylhsminor.yy8;
        break;
      case 198: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 199: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy400.interval = yymsp[-1].minor.yy0; yylhsminor.yy400.offset.n = 0; yylhsminor.yy400.token = yymsp[-3].minor.yy104;}
  yymsp[-3].minor.yy400 = yylhsminor.yy400;
        break;
      case 200: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy400.interval = yymsp[-3].minor.yy0; yylhsminor.yy400.offset = yymsp[-1].minor.yy0;   yylhsminor.yy400.token = yymsp[-5].minor.yy104;}
  yymsp[-5].minor.yy400 = yylhsminor.yy400;
        break;
      case 201: /* interval_option ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 202: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy104 = TK_INTERVAL;}
        break;
      case 203: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy104 = TK_EVERY;   }
        break;
      case 204: /* session_option ::= */
{yymsp[1].minor.yy147.col.n = 0; yymsp[1].minor.yy147.gap.n = 0;}
        break;
      case 205: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy147.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy147.gap = yymsp[-1].minor.yy0;
}
        break;
      case 206: /* windowstate_option ::= */
{ yymsp[1].minor.yy40.col.n = 0; yymsp[1].minor.yy40.col.z = NULL;}
        break;
      case 207: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy40.col = yymsp[-1].minor.yy0; }
        break;
      case 208: /* fill_opt ::= */
{ yymsp[1].minor.yy421 = 0;     }
        break;
      case 209: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
        break;
      case 210: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 211: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 212: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 214: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 215: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy421 = tListItemAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy69, yymsp[0].minor.yy96);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 216: /* sortlist ::= item sortorder */
{
  yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[-1].minor.yy69, yymsp[0].minor.yy96);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 217: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy69, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy69 = yylhsminor.yy69;
        break;
      case 218: /* sortorder ::= ASC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 219: /* sortorder ::= DESC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
        break;
      case 220: /* sortorder ::= */
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 221: /* groupby_opt ::= */
{ yymsp[1].minor.yy421 = 0;}
        break;
      case 222: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 223: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy421 = tListItemAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy69, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 224: /* grouplist ::= item */
{
  yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[0].minor.yy69, -1);
}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 225: /* having_opt ::= */
      case 235: /* where_opt ::= */ yytestcase(yyruleno==235);
      case 279: /* expritem ::= */ yytestcase(yyruleno==279);
{yymsp[1].minor.yy439 = 0;}
        break;
      case 226: /* having_opt ::= HAVING expr */
      case 236: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==236);
{yymsp[-1].minor.yy439 = yymsp[0].minor.yy439;}
        break;
      case 227: /* limit_opt ::= */
      case 231: /* slimit_opt ::= */ yytestcase(yyruleno==231);
{yymsp[1].minor.yy231.limit = -1; yymsp[1].minor.yy231.offset = 0;}
        break;
      case 228: /* limit_opt ::= LIMIT signed */
      case 232: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==232);
{yymsp[-1].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy231.offset = 0;}
        break;
      case 229: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy231.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[0].minor.yy325;}
        break;
      case 230: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[-2].minor.yy325;}
        break;
      case 233: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy231.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[0].minor.yy325;}
        break;
      case 234: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[-2].minor.yy325;}
        break;
      case 237: /* expr ::= LP expr RP */
{yylhsminor.yy439 = yymsp[-1].minor.yy439; yylhsminor.yy439->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy439->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 238: /* expr ::= ID */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 239: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 240: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 241: /* expr ::= INTEGER */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 242: /* expr ::= MINUS INTEGER */
      case 243: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==243);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 244: /* expr ::= FLOAT */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 245: /* expr ::= MINUS FLOAT */
      case 246: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==246);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 247: /* expr ::= STRING */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 248: /* expr ::= NOW */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 249: /* expr ::= VARIABLE */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 250: /* expr ::= PLUS VARIABLE */
      case 251: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==251);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 252: /* expr ::= BOOL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 253: /* expr ::= NULL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 254: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(yymsp[-1].minor.yy421, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 255: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 256: /* expr ::= expr IS NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 257: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-3].minor.yy439, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 258: /* expr ::= expr LT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 259: /* expr ::= expr GT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 260: /* expr ::= expr LE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 261: /* expr ::= expr GE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 262: /* expr ::= expr NE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 263: /* expr ::= expr EQ expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_EQ);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 264: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy439); yylhsminor.yy439 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy439, yymsp[-2].minor.yy439, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy439, TK_LE), TK_AND);}
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 265: /* expr ::= expr AND expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_AND);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 266: /* expr ::= expr OR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_OR); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 267: /* expr ::= expr PLUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_PLUS);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 268: /* expr ::= expr MINUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MINUS); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 269: /* expr ::= expr STAR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_STAR);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 270: /* expr ::= expr SLASH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_DIVIDE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 271: /* expr ::= expr REM expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_REM);   }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 272: /* expr ::= expr LIKE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LIKE);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 273: /* expr ::= expr MATCH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MATCH);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 274: /* expr ::= expr NMATCH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NMATCH);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 275: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-4].minor.yy439, (tSqlExpr*)yymsp[-1].minor.yy421, TK_IN); }
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 276: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy421 = tSqlExprListAppend(yymsp[-2].minor.yy421,yymsp[0].minor.yy439,0, 0);}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 277: /* exprlist ::= expritem */
{yylhsminor.yy421 = tSqlExprListAppend(0,yymsp[0].minor.yy439,0, 0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 278: /* expritem ::= expr */
{yylhsminor.yy439 = yymsp[0].minor.yy439;}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 280: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 281: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 288: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy69, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 296: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy69, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 299: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 300: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
