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
#define YYNSTATE             363
#define YYNRULE              300
#define YYNTOKEN             197
#define YY_MAX_SHIFT         362
#define YY_MIN_SHIFTREDUCE   581
#define YY_MAX_SHIFTREDUCE   880
#define YY_ERROR_ACTION      881
#define YY_ACCEPT_ACTION     882
#define YY_NO_ACTION         883
#define YY_MIN_REDUCE        884
#define YY_MAX_REDUCE        1183
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
#define YY_ACTTAB_COUNT (778)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    95,  632,   36, 1027,  632,   21,  248,  710,  205,  633,
 /*    10 */   361,  229,  633,   55,   56, 1019,   59,   60,  161, 1159,
 /*    20 */   251,   49,   48,   47, 1068,   58,  320,   63,   61,   64,
 /*    30 */    62, 1016, 1017,   33, 1020,   54,   53,  340,  339,   52,
 /*    40 */    51,   50,   55,   56,  231,   59,   60,  242, 1030,  251,
 /*    50 */    49,   48,   47,  667,   58,  320,   63,   61,   64,   62,
 /*    60 */   202,  247,  882,  362,   54,   53,  205,  260,   52,   51,
 /*    70 */    50,   55,   56,  203,   59,   60,  175, 1160,  251,   49,
 /*    80 */    48,   47,  632,   58,  320,   63,   61,   64,   62,   80,
 /*    90 */   633, 1065, 1106,   54,   53,  235, 1045,   52,   51,   50,
 /*   100 */   632,  317,  317,   55,   57,  161,   59,   60,  633, 1058,
 /*   110 */   251,   49,   48,   47,  817,   58,  320,   63,   61,   64,
 /*   120 */    62,  205,  208,  154,  241,   54,   53,  273, 1033,   52,
 /*   130 */    51,   50, 1160,  196,  194,  192,  161,   52,   51,   50,
 /*   140 */   191,  140,  139,  138,  137,  350,  582,  583,  584,  585,
 /*   150 */   586,  587,  588,  589,  590,  591,  592,  593,  594,  595,
 /*   160 */   152,   56,  230,   59,   60,   27,   93,  251,   49,   48,
 /*   170 */    47,   98,   58,  320,   63,   61,   64,   62,   32, 1107,
 /*   180 */    81,  291,   54,   53,  161,   36,   52,   51,   50,   59,
 /*   190 */    60,  279,  278,  251,   49,   48,   47,  265,   58,  320,
 /*   200 */    63,   61,   64,   62,  252, 1021,  269,  268,   54,   53,
 /*   210 */    92,  299,   52,   51,   50,   42,  315,  356,  355,  314,
 /*   220 */   313,  312,  354,  311,  310,  309,  353,  308,  352,  351,
 /*   230 */    22, 1029,  999,  987,  988,  989,  990,  991,  992,  993,
 /*   240 */   994,  995,  996,  997,  998, 1000, 1001,  214,  245,  250,
 /*   250 */   832, 1060, 1033,  821,  215,  824,  293,  827,   91,  254,
 /*   260 */   136,  135,  134,  216,  205,  250,  832,  325,   86,  821,
 /*   270 */   209,  824,   36,  827,  170, 1160,   12,   63,   61,   64,
 /*   280 */    62,   94, 1044,  227,  228,   54,   53,  321,   36,   52,
 /*   290 */    51,   50,  281,    3,   39,  177,  782,  783,  210,  227,
 /*   300 */   228,  104,  109,  100,  107,   43,   86,  823,  746,  826,
 /*   310 */    97,  743,  123,  744,  239,  745,  738, 1154, 1030,  735,
 /*   320 */   304,  736,   86,  737,  259,  350,  822,  272,  825,   78,
 /*   330 */   240,  733,   65,  734, 1030,  255,  223,  253, 1032,  328,
 /*   340 */   327,  256,  257,   43,   42,   85,  356,  355,   65,  243,
 /*   350 */   244,  354,  121,  115,  125,  353,  763,  352,  351,   43,
 /*   360 */    74,  130,  133,  124,   36,   36,   36,  833,  828, 1058,
 /*   370 */   127,   36,  357,  969,  829, 1005,   36, 1003, 1004,  360,
 /*   380 */   359,  145, 1006,  833,  828,   36, 1007,  232, 1008, 1009,
 /*   390 */   829,   54,   53,   36,   36,   52,   51,   50,  322,   75,
 /*   400 */   261, 1058,  258,  932,  335,  334,  329,  330,  331,  187,
 /*   410 */  1030, 1030, 1030,  332,  151,  149,  148, 1030,  336,  233,
 /*   420 */   260,  260, 1030,   79,  799,  747,  748,  337,  830,  176,
 /*   430 */  1031, 1030,  942,  739,  740,  338,  342,  760,  187, 1030,
 /*   440 */  1030,  767,  933,  274,  779,   83,  720,  831,  187,   84,
 /*   450 */   789,  790,   71,  296,  722, 1018,  298,  819,   37,  156,
 /*   460 */   721,   37,    7,  855,  834,   66,   24,  249,   37,   67,
 /*   470 */   631,   96,  731,   77,  732,   67,  132,  131,   23,   23,
 /*   480 */  1153,  798,   70, 1152,  225,   23,   70, 1099,   14,    4,
 /*   490 */    13,  226,  114,   72,  113,  820,   16,  206,   15,  751,
 /*   500 */   207,  752,  836, 1117,  749,  709,  750,  211,  204,   18,
 /*   510 */   120,   17,  119,  212,   20,  213,   19,  218, 1179,  219,
 /*   520 */  1171,  220,  217,  201, 1116,  270,  237, 1113, 1112,  238,
 /*   530 */   341,  153, 1067,   44, 1078, 1075, 1076, 1098, 1080, 1059,
 /*   540 */   276,  150,  155,  160,  287, 1028,  280,  171,  172,  275,
 /*   550 */   234, 1026,  282,  173,  174,  946,  284,  301,  162,  778,
 /*   560 */  1056,  163,  164,  165,  166,  167,  168,  169,  286,  294,
 /*   570 */   302,  290,  303,  306,  307,   76,  199,   40,   73,   46,
 /*   580 */   318,  941,  319,  292,  326,  288, 1178,  111, 1177,  283,
 /*   590 */  1174,  178,  333, 1170,  117, 1169,   45, 1166,  179,  966,
 /*   600 */    41,   38,  200,  930,  126,  305,  928,  128,  129,  926,
 /*   610 */   925,  262,  189,  190,  922,  921,  920,  919,  918,  917,
 /*   620 */   916,  193,  195,  913,  911,  909,  907,  197,  904,  198,
 /*   630 */   900,  122,  343,   82,   87,  344,  285, 1100,  345,  346,
 /*   640 */   347,  348,  349,  358,  880,  224,  246,  300,  263,  264,
 /*   650 */   879,  221,  266,  222,  267,  878,  945,  105,  944,  861,
 /*   660 */   860,  271,   70,  295,    8,   28,  924,  923,  277,  141,
 /*   670 */   181,  967,  182,  142,  184,  915,  180,  183,  185,  143,
 /*   680 */   186,  144,  914,  968,  906,  905,  754,   88,    2,    1,
 /*   690 */   780,  157,  158,   31,  791,  785,  159,   89,  236,  787,
 /*   700 */    90,  289,   29,    9,   30,   10,   11,   25,  297,   26,
 /*   710 */    97,   99,  102,  645,   34,  101,  680,   35,  103,  678,
 /*   720 */   677,  676,  674,  673,  672,  669,  316,  106,  636,  323,
 /*   730 */   108,  835,    5,  324,  837,    6,   37,   68,  110,  112,
 /*   740 */    69,  712,  116,  118,  711,  708,  661,  659,  651,  657,
 /*   750 */   653,  655,  649,  647,  682,  681,  679,  675,  671,  670,
 /*   760 */   634,  188,  599,  884,  883,  883,  883,  883,  883,  883,
 /*   770 */   883,  883,  883,  883,  883,  883,  146,  147,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   207,    1,  200,  200,    1,  266,  206,    5,  266,    9,
 /*    10 */   200,  201,    9,   13,   14,    0,   16,   17,  200,  277,
 /*    20 */    20,   21,   22,   23,  200,   25,   26,   27,   28,   29,
 /*    30 */    30,  238,  239,  240,  241,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  242,   16,   17,  244,  246,   20,
 /*    50 */    21,   22,   23,    5,   25,   26,   27,   28,   29,   30,
 /*    60 */   266,  206,  198,  199,   35,   36,  266,  200,   39,   40,
 /*    70 */    41,   13,   14,  266,   16,   17,  209,  277,   20,   21,
 /*    80 */    22,   23,    1,   25,   26,   27,   28,   29,   30,   89,
 /*    90 */     9,  267,  274,   35,   36,  248,  249,   39,   40,   41,
 /*   100 */     1,   86,   86,   13,   14,  200,   16,   17,    9,  245,
 /*   110 */    20,   21,   22,   23,   85,   25,   26,   27,   28,   29,
 /*   120 */    30,  266,  266,  200,  243,   35,   36,  263,  247,   39,
 /*   130 */    40,   41,  277,   64,   65,   66,  200,   39,   40,   41,
 /*   140 */    71,   72,   73,   74,   75,   93,   47,   48,   49,   50,
 /*   150 */    51,   52,   53,   54,   55,   56,   57,   58,   59,   60,
 /*   160 */    61,   14,   63,   16,   17,   84,  250,   20,   21,   22,
 /*   170 */    23,  207,   25,   26,   27,   28,   29,   30,   84,  274,
 /*   180 */   264,  276,   35,   36,  200,  200,   39,   40,   41,   16,
 /*   190 */    17,  268,  269,   20,   21,   22,   23,  144,   25,   26,
 /*   200 */    27,   28,   29,   30,  206,  241,  153,  154,   35,   36,
 /*   210 */   274,  117,   39,   40,   41,  101,  102,  103,  104,  105,
 /*   220 */   106,  107,  108,  109,  110,  111,  112,  113,  114,  115,
 /*   230 */    46,  246,  222,  223,  224,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,   63,  243,    1,
 /*   250 */     2,  245,  247,    5,   70,    7,  272,    9,  274,   70,
 /*   260 */    76,   77,   78,   79,  266,    1,    2,   83,   84,    5,
 /*   270 */   266,    7,  200,    9,  253,  277,   84,   27,   28,   29,
 /*   280 */    30,   89,  249,   35,   36,   35,   36,   39,  200,   39,
 /*   290 */    40,   41,  271,   64,   65,   66,  127,  128,  266,   35,
 /*   300 */    36,   72,   73,   74,   75,  121,   84,    5,    2,    7,
 /*   310 */   118,    5,   80,    7,  242,    9,    2,  266,  246,    5,
 /*   320 */    91,    7,   84,    9,   70,   93,    5,  143,    7,  145,
 /*   330 */   242,    5,   84,    7,  246,  146,  152,  148,  247,  150,
 /*   340 */   151,   35,   36,  121,  101,  123,  103,  104,   84,   35,
 /*   350 */    36,  108,   64,   65,   66,  112,   39,  114,  115,  121,
 /*   360 */   100,   73,   74,   75,  200,  200,  200,  119,  120,  245,
 /*   370 */    82,  200,  220,  221,  126,  222,  200,  224,  225,   67,
 /*   380 */    68,   69,  229,  119,  120,  200,  233,  263,  235,  236,
 /*   390 */   126,   35,   36,  200,  200,   39,   40,   41,   15,  139,
 /*   400 */   146,  245,  148,  205,  150,  151,  242,  242,  242,  211,
 /*   410 */   246,  246,  246,  242,   64,   65,   66,  246,  242,  263,
 /*   420 */   200,  200,  246,  207,   78,  119,  120,  242,  126,  209,
 /*   430 */   209,  246,  205,  119,  120,  242,  242,  100,  211,  246,
 /*   440 */   246,  124,  205,   85,   85,   85,   85,  126,  211,   85,
 /*   450 */    85,   85,  100,   85,   85,  239,   85,    1,  100,  100,
 /*   460 */    85,  100,  125,   85,   85,  100,  100,   62,  100,  100,
 /*   470 */    85,  100,    5,   84,    7,  100,   80,   81,  100,  100,
 /*   480 */   266,  135,  122,  266,  266,  100,  122,  275,  147,   84,
 /*   490 */   149,  266,  147,  141,  149,   39,  147,  266,  149,    5,
 /*   500 */   266,    7,  119,  237,    5,  116,    7,  266,  266,  147,
 /*   510 */   147,  149,  149,  266,  147,  266,  149,  266,  249,  266,
 /*   520 */   249,  266,  266,  266,  237,  200,  237,  237,  237,  237,
 /*   530 */   237,  200,  200,  265,  200,  200,  200,  275,  200,  245,
 /*   540 */   245,   62,  200,  200,  200,  245,  270,  251,  200,  202,
 /*   550 */   270,  200,  270,  200,  200,  200,  270,  200,  261,  126,
 /*   560 */   262,  260,  259,  258,  257,  256,  255,  254,  129,  133,
 /*   570 */   200,  131,  200,  200,  200,  138,  200,  200,  140,  137,
 /*   580 */   200,  200,  200,  136,  200,  130,  200,  200,  200,  132,
 /*   590 */   200,  200,  200,  200,  200,  200,  142,  200,  200,  200,
 /*   600 */   200,  200,  200,  200,  200,   92,  200,  200,  200,  200,
 /*   610 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   620 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   630 */   200,   99,   98,  202,  202,   53,  202,  202,   95,   97,
 /*   640 */    57,   96,   94,   86,    5,  202,  202,  202,  155,    5,
 /*   650 */     5,  202,  155,  202,    5,    5,  210,  207,  210,  103,
 /*   660 */   102,  144,  122,  117,   84,   84,  202,  202,  100,  203,
 /*   670 */   217,  219,  213,  203,  214,  202,  218,  216,  215,  203,
 /*   680 */   212,  203,  202,  221,  202,  202,   85,  100,  204,  208,
 /*   690 */    85,   84,   84,  252,   85,   85,  100,   84,    1,   85,
 /*   700 */    84,   84,  100,  134,  100,  134,   84,   84,  117,   84,
 /*   710 */   118,   80,   72,    5,   90,   89,    9,   90,   89,    5,
 /*   720 */     5,    5,    5,    5,    5,    5,   15,   80,   87,   26,
 /*   730 */    88,   85,   84,   61,  119,   84,  100,   16,  149,  149,
 /*   740 */    16,    5,  149,  149,    5,   85,    5,    5,    5,    5,
 /*   750 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   760 */    87,  100,   62,    0,  278,  278,  278,  278,  278,  278,
 /*   770 */   278,  278,  278,  278,  278,  278,   21,   21,  278,  278,
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
 /*   970 */   278,  278,  278,  278,  278,
};
#define YY_SHIFT_COUNT    (362)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (763)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   184,  114,  243,   16,  248,  264,  264,   81,    3,    3,
 /*    10 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    20 */     3,    0,   99,  264,  306,  314,  314,  238,  238,    3,
 /*    30 */     3,  169,    3,   15,    3,    3,    3,    3,  232,   16,
 /*    40 */    52,   52,   48,  778,  264,  264,  264,  264,  264,  264,
 /*    50 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*    60 */   264,  264,  264,  264,  264,  264,  306,  314,  306,  306,
 /*    70 */   222,    2,    2,    2,    2,    2,    2,    2,    3,    3,
 /*    80 */     3,  317,    3,    3,    3,  238,  238,    3,    3,    3,
 /*    90 */     3,  346,  346,  337,  238,    3,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   110 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   120 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   130 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   140 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   150 */     3,    3,    3,  479,  479,  479,  433,  433,  433,  433,
 /*   160 */   479,  479,  437,  438,  436,  442,  447,  440,  455,  439,
 /*   170 */   457,  454,  479,  479,  479,  513,  513,   16,  479,  479,
 /*   180 */   532,  534,  582,  543,  542,  583,  545,  548,   48,  479,
 /*   190 */   479,  557,  557,  479,  557,  479,  557,  479,  479,  778,
 /*   200 */   778,   29,   58,   58,   90,   58,  147,  173,  250,  250,
 /*   210 */   250,  250,  250,  250,  229,   69,  288,  356,  356,  356,
 /*   220 */   356,  189,  254,   53,  192,   98,   98,  302,  321,  312,
 /*   230 */   350,  358,  360,  364,  359,  365,  366,  352,  260,  361,
 /*   240 */   368,  369,  371,  326,  467,  375,   94,  378,  379,  456,
 /*   250 */   405,  383,  385,  341,  345,  349,  494,  499,  362,  363,
 /*   260 */   389,  367,  396,  639,  493,  644,  645,  497,  649,  650,
 /*   270 */   556,  558,  517,  540,  546,  580,  601,  581,  568,  587,
 /*   280 */   605,  607,  609,  608,  610,  596,  613,  614,  616,  697,
 /*   290 */   617,  602,  569,  604,  571,  622,  546,  623,  591,  625,
 /*   300 */   592,  631,  624,  626,  640,  708,  627,  629,  707,  714,
 /*   310 */   715,  716,  717,  718,  719,  720,  641,  711,  647,  642,
 /*   320 */   648,  646,  615,  651,  703,  672,  721,  589,  590,  636,
 /*   330 */   636,  636,  636,  724,  593,  594,  636,  636,  636,  736,
 /*   340 */   739,  660,  636,  741,  742,  743,  744,  745,  746,  747,
 /*   350 */   748,  749,  750,  751,  752,  753,  754,  661,  673,  755,
 /*   360 */   756,  700,  763,
};
#define YY_REDUCE_COUNT (200)
#define YY_REDUCE_MIN   (-261)
#define YY_REDUCE_MAX   (484)
static const short yy_reduce_ofst[] = {
 /*     0 */  -136,   10,  153, -207, -200, -145,   -2,  -77, -198,  -95,
 /*    10 */   -16,   72,   88,  164,  165,  166,  171,  176,  185,  193,
 /*    20 */   194, -176, -190, -258, -153, -119,    5,  124,  156, -182,
 /*    30 */   -64,   21, -197,  -36, -133,  220,  221,  -15,  198,  216,
 /*    40 */   227,  237,  152,  -84, -261, -206, -193, -144,    4,   32,
 /*    50 */    51,  214,  217,  218,  225,  231,  234,  241,  242,  247,
 /*    60 */   249,  251,  253,  255,  256,  257,   33,   91,  269,  271,
 /*    70 */     6,  266,  287,  289,  290,  291,  292,  293,  325,  331,
 /*    80 */   332,  268,  334,  335,  336,  294,  295,  338,  342,  343,
 /*    90 */   344,  212,  262,  296,  300,  348,  351,  353,  354,  355,
 /*   100 */   357,  370,  372,  373,  374,  376,  377,  380,  381,  382,
 /*   110 */   384,  386,  387,  388,  390,  391,  392,  393,  394,  395,
 /*   120 */   397,  398,  399,  400,  401,  402,  403,  404,  406,  407,
 /*   130 */   408,  409,  410,  411,  412,  413,  414,  415,  416,  417,
 /*   140 */   418,  419,  420,  421,  422,  423,  424,  425,  426,  427,
 /*   150 */   428,  429,  430,  347,  431,  432,  276,  280,  282,  286,
 /*   160 */   434,  435,  298,  297,  301,  303,  305,  307,  309,  311,
 /*   170 */   313,  441,  443,  444,  445,  446,  448,  450,  449,  451,
 /*   180 */   452,  458,  453,  459,  461,  460,  463,  468,  462,  464,
 /*   190 */   465,  466,  470,  473,  476,  480,  478,  482,  483,  481,
 /*   200 */   484,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   881,  943,  931,  940, 1162, 1162, 1162,  881,  881,  881,
 /*    10 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*    20 */   881, 1069,  901, 1162,  881,  881,  881,  881,  881,  881,
 /*    30 */   881, 1084,  881,  940,  881,  881,  881,  881,  949,  940,
 /*    40 */   949,  949,  881, 1064,  881,  881,  881,  881,  881,  881,
 /*    50 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*    60 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*    70 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*    80 */   881, 1071, 1077, 1074,  881,  881,  881, 1079,  881,  881,
 /*    90 */   881, 1103, 1103, 1062,  881,  881,  881,  881,  881,  881,
 /*   100 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   110 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   120 */   881,  881,  881,  881,  881,  881,  929,  881,  927,  881,
 /*   130 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   140 */   881,  881,  881,  881,  881,  912,  881,  881,  881,  881,
 /*   150 */   881,  881,  899,  903,  903,  903,  881,  881,  881,  881,
 /*   160 */   903,  903, 1110, 1114, 1096, 1108, 1104, 1091, 1089, 1087,
 /*   170 */  1095, 1118,  903,  903,  903,  947,  947,  940,  903,  903,
 /*   180 */   965,  963,  961,  953,  959,  955,  957,  951,  881,  903,
 /*   190 */   903,  938,  938,  903,  938,  903,  938,  903,  903,  986,
 /*   200 */  1002,  881, 1119, 1109,  881, 1161, 1149, 1148, 1157, 1156,
 /*   210 */  1155, 1147, 1146, 1145,  881,  881,  881, 1141, 1144, 1143,
 /*   220 */  1142,  881,  881,  881,  881, 1151, 1150,  881,  881,  881,
 /*   230 */   881,  881,  881,  881,  881,  881,  881, 1115, 1111,  881,
 /*   240 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   250 */  1121,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   260 */  1010,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   270 */   881,  881,  881, 1061,  881,  881,  881,  881, 1073, 1072,
 /*   280 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   290 */   881, 1105,  881, 1097,  881,  881, 1022,  881,  881,  881,
 /*   300 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   310 */   881,  881,  881,  881,  881,  881,  881,  881,  881,  881,
 /*   320 */   881,  881,  881,  881,  881,  881,  881,  881,  881, 1180,
 /*   330 */  1175, 1176, 1173,  881,  881,  881, 1172, 1167, 1168,  881,
 /*   340 */   881,  881, 1165,  881,  881,  881,  881,  881,  881,  881,
 /*   350 */   881,  881,  881,  881,  881,  881,  881,  971,  881,  910,
 /*   360 */   908,  881,  881,
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
  /*   88 */ "PORT",
  /*   89 */ "AS",
  /*   90 */ "OUTPUTTYPE",
  /*   91 */ "AGGREGATE",
  /*   92 */ "BUFSIZE",
  /*   93 */ "PPS",
  /*   94 */ "TSERIES",
  /*   95 */ "DBS",
  /*   96 */ "STORAGE",
  /*   97 */ "QTIME",
  /*   98 */ "CONNS",
  /*   99 */ "STATE",
  /*  100 */ "COMMA",
  /*  101 */ "KEEP",
  /*  102 */ "CACHE",
  /*  103 */ "REPLICA",
  /*  104 */ "QUORUM",
  /*  105 */ "DAYS",
  /*  106 */ "MINROWS",
  /*  107 */ "MAXROWS",
  /*  108 */ "BLOCKS",
  /*  109 */ "CTIME",
  /*  110 */ "WAL",
  /*  111 */ "FSYNC",
  /*  112 */ "COMP",
  /*  113 */ "PRECISION",
  /*  114 */ "UPDATE",
  /*  115 */ "CACHELAST",
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
 /* 102 */ "db_optr ::=",
 /* 103 */ "db_optr ::= db_optr cache",
 /* 104 */ "db_optr ::= db_optr replica",
 /* 105 */ "db_optr ::= db_optr quorum",
 /* 106 */ "db_optr ::= db_optr days",
 /* 107 */ "db_optr ::= db_optr minrows",
 /* 108 */ "db_optr ::= db_optr maxrows",
 /* 109 */ "db_optr ::= db_optr blocks",
 /* 110 */ "db_optr ::= db_optr ctime",
 /* 111 */ "db_optr ::= db_optr wal",
 /* 112 */ "db_optr ::= db_optr fsync",
 /* 113 */ "db_optr ::= db_optr comp",
 /* 114 */ "db_optr ::= db_optr prec",
 /* 115 */ "db_optr ::= db_optr keep",
 /* 116 */ "db_optr ::= db_optr update",
 /* 117 */ "db_optr ::= db_optr cachelast",
 /* 118 */ "alter_db_optr ::=",
 /* 119 */ "alter_db_optr ::= alter_db_optr replica",
 /* 120 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 121 */ "alter_db_optr ::= alter_db_optr keep",
 /* 122 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 123 */ "alter_db_optr ::= alter_db_optr comp",
 /* 124 */ "alter_db_optr ::= alter_db_optr update",
 /* 125 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 126 */ "typename ::= ids",
 /* 127 */ "typename ::= ids LP signed RP",
 /* 128 */ "typename ::= ids UNSIGNED",
 /* 129 */ "signed ::= INTEGER",
 /* 130 */ "signed ::= PLUS INTEGER",
 /* 131 */ "signed ::= MINUS INTEGER",
 /* 132 */ "cmd ::= CREATE TABLE create_table_args",
 /* 133 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE TABLE create_table_list",
 /* 136 */ "create_table_list ::= create_from_stable",
 /* 137 */ "create_table_list ::= create_table_list create_from_stable",
 /* 138 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 139 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 142 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 143 */ "tagNamelist ::= ids",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 145 */ "columnlist ::= columnlist COMMA column",
 /* 146 */ "columnlist ::= column",
 /* 147 */ "column ::= ids typename",
 /* 148 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 149 */ "tagitemlist1 ::= tagitem1",
 /* 150 */ "tagitem1 ::= MINUS INTEGER",
 /* 151 */ "tagitem1 ::= MINUS FLOAT",
 /* 152 */ "tagitem1 ::= PLUS INTEGER",
 /* 153 */ "tagitem1 ::= PLUS FLOAT",
 /* 154 */ "tagitem1 ::= INTEGER",
 /* 155 */ "tagitem1 ::= FLOAT",
 /* 156 */ "tagitem1 ::= STRING",
 /* 157 */ "tagitem1 ::= BOOL",
 /* 158 */ "tagitem1 ::= NULL",
 /* 159 */ "tagitem1 ::= NOW",
 /* 160 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 161 */ "tagitemlist ::= tagitem",
 /* 162 */ "tagitem ::= INTEGER",
 /* 163 */ "tagitem ::= FLOAT",
 /* 164 */ "tagitem ::= STRING",
 /* 165 */ "tagitem ::= BOOL",
 /* 166 */ "tagitem ::= NULL",
 /* 167 */ "tagitem ::= NOW",
 /* 168 */ "tagitem ::= MINUS INTEGER",
 /* 169 */ "tagitem ::= MINUS FLOAT",
 /* 170 */ "tagitem ::= PLUS INTEGER",
 /* 171 */ "tagitem ::= PLUS FLOAT",
 /* 172 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 173 */ "select ::= LP select RP",
 /* 174 */ "union ::= select",
 /* 175 */ "union ::= union UNION ALL select",
 /* 176 */ "union ::= union UNION select",
 /* 177 */ "cmd ::= union",
 /* 178 */ "select ::= SELECT selcollist",
 /* 179 */ "sclp ::= selcollist COMMA",
 /* 180 */ "sclp ::=",
 /* 181 */ "selcollist ::= sclp distinct expr as",
 /* 182 */ "selcollist ::= sclp STAR",
 /* 183 */ "as ::= AS ids",
 /* 184 */ "as ::= ids",
 /* 185 */ "as ::=",
 /* 186 */ "distinct ::= DISTINCT",
 /* 187 */ "distinct ::=",
 /* 188 */ "from ::= FROM tablelist",
 /* 189 */ "from ::= FROM sub",
 /* 190 */ "sub ::= LP union RP",
 /* 191 */ "sub ::= LP union RP ids",
 /* 192 */ "sub ::= sub COMMA LP union RP ids",
 /* 193 */ "tablelist ::= ids cpxName",
 /* 194 */ "tablelist ::= ids cpxName ids",
 /* 195 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 196 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 197 */ "tmvar ::= VARIABLE",
 /* 198 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 199 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 200 */ "interval_option ::=",
 /* 201 */ "intervalKey ::= INTERVAL",
 /* 202 */ "intervalKey ::= EVERY",
 /* 203 */ "session_option ::=",
 /* 204 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 205 */ "windowstate_option ::=",
 /* 206 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 207 */ "fill_opt ::=",
 /* 208 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 209 */ "fill_opt ::= FILL LP ID RP",
 /* 210 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 211 */ "sliding_opt ::=",
 /* 212 */ "orderby_opt ::=",
 /* 213 */ "orderby_opt ::= ORDER BY sortlist",
 /* 214 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 215 */ "sortlist ::= item sortorder",
 /* 216 */ "item ::= ids cpxName",
 /* 217 */ "sortorder ::= ASC",
 /* 218 */ "sortorder ::= DESC",
 /* 219 */ "sortorder ::=",
 /* 220 */ "groupby_opt ::=",
 /* 221 */ "groupby_opt ::= GROUP BY grouplist",
 /* 222 */ "grouplist ::= grouplist COMMA item",
 /* 223 */ "grouplist ::= item",
 /* 224 */ "having_opt ::=",
 /* 225 */ "having_opt ::= HAVING expr",
 /* 226 */ "limit_opt ::=",
 /* 227 */ "limit_opt ::= LIMIT signed",
 /* 228 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 229 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 230 */ "slimit_opt ::=",
 /* 231 */ "slimit_opt ::= SLIMIT signed",
 /* 232 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 233 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 234 */ "where_opt ::=",
 /* 235 */ "where_opt ::= WHERE expr",
 /* 236 */ "expr ::= LP expr RP",
 /* 237 */ "expr ::= ID",
 /* 238 */ "expr ::= ID DOT ID",
 /* 239 */ "expr ::= ID DOT STAR",
 /* 240 */ "expr ::= INTEGER",
 /* 241 */ "expr ::= MINUS INTEGER",
 /* 242 */ "expr ::= PLUS INTEGER",
 /* 243 */ "expr ::= FLOAT",
 /* 244 */ "expr ::= MINUS FLOAT",
 /* 245 */ "expr ::= PLUS FLOAT",
 /* 246 */ "expr ::= STRING",
 /* 247 */ "expr ::= NOW",
 /* 248 */ "expr ::= VARIABLE",
 /* 249 */ "expr ::= PLUS VARIABLE",
 /* 250 */ "expr ::= MINUS VARIABLE",
 /* 251 */ "expr ::= BOOL",
 /* 252 */ "expr ::= NULL",
 /* 253 */ "expr ::= ID LP exprlist RP",
 /* 254 */ "expr ::= ID LP STAR RP",
 /* 255 */ "expr ::= expr IS NULL",
 /* 256 */ "expr ::= expr IS NOT NULL",
 /* 257 */ "expr ::= expr LT expr",
 /* 258 */ "expr ::= expr GT expr",
 /* 259 */ "expr ::= expr LE expr",
 /* 260 */ "expr ::= expr GE expr",
 /* 261 */ "expr ::= expr NE expr",
 /* 262 */ "expr ::= expr EQ expr",
 /* 263 */ "expr ::= expr BETWEEN expr AND expr",
 /* 264 */ "expr ::= expr AND expr",
 /* 265 */ "expr ::= expr OR expr",
 /* 266 */ "expr ::= expr PLUS expr",
 /* 267 */ "expr ::= expr MINUS expr",
 /* 268 */ "expr ::= expr STAR expr",
 /* 269 */ "expr ::= expr SLASH expr",
 /* 270 */ "expr ::= expr REM expr",
 /* 271 */ "expr ::= expr LIKE expr",
 /* 272 */ "expr ::= expr MATCH expr",
 /* 273 */ "expr ::= expr NMATCH expr",
 /* 274 */ "expr ::= expr IN LP exprlist RP",
 /* 275 */ "exprlist ::= exprlist COMMA expritem",
 /* 276 */ "exprlist ::= expritem",
 /* 277 */ "expritem ::= expr",
 /* 278 */ "expritem ::=",
 /* 279 */ "cmd ::= RESET QUERY CACHE",
 /* 280 */ "cmd ::= SYNCDB ids REPLICA",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 297 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 298 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 299 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
  {  199,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  199,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  199,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  199,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  210,    0 }, /* (63) bufsize ::= */
  {  210,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  211,    0 }, /* (65) pps ::= */
  {  211,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  212,    0 }, /* (67) tseries ::= */
  {  212,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  213,    0 }, /* (69) dbs ::= */
  {  213,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  214,    0 }, /* (71) streams ::= */
  {  214,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  215,    0 }, /* (73) storage ::= */
  {  215,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  216,    0 }, /* (75) qtime ::= */
  {  216,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  217,    0 }, /* (77) users ::= */
  {  217,   -2 }, /* (78) users ::= USERS INTEGER */
  {  218,    0 }, /* (79) conns ::= */
  {  218,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  219,    0 }, /* (81) state ::= */
  {  219,   -2 }, /* (82) state ::= STATE ids */
  {  205,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  220,   -3 }, /* (84) intitemlist ::= intitemlist COMMA intitem */
  {  220,   -1 }, /* (85) intitemlist ::= intitem */
  {  221,   -1 }, /* (86) intitem ::= INTEGER */
  {  222,   -2 }, /* (87) keep ::= KEEP intitemlist */
  {  223,   -2 }, /* (88) cache ::= CACHE INTEGER */
  {  224,   -2 }, /* (89) replica ::= REPLICA INTEGER */
  {  225,   -2 }, /* (90) quorum ::= QUORUM INTEGER */
  {  226,   -2 }, /* (91) days ::= DAYS INTEGER */
  {  227,   -2 }, /* (92) minrows ::= MINROWS INTEGER */
  {  228,   -2 }, /* (93) maxrows ::= MAXROWS INTEGER */
  {  229,   -2 }, /* (94) blocks ::= BLOCKS INTEGER */
  {  230,   -2 }, /* (95) ctime ::= CTIME INTEGER */
  {  231,   -2 }, /* (96) wal ::= WAL INTEGER */
  {  232,   -2 }, /* (97) fsync ::= FSYNC INTEGER */
  {  233,   -2 }, /* (98) comp ::= COMP INTEGER */
  {  234,   -2 }, /* (99) prec ::= PRECISION STRING */
  {  235,   -2 }, /* (100) update ::= UPDATE INTEGER */
  {  236,   -2 }, /* (101) cachelast ::= CACHELAST INTEGER */
  {  208,    0 }, /* (102) db_optr ::= */
  {  208,   -2 }, /* (103) db_optr ::= db_optr cache */
  {  208,   -2 }, /* (104) db_optr ::= db_optr replica */
  {  208,   -2 }, /* (105) db_optr ::= db_optr quorum */
  {  208,   -2 }, /* (106) db_optr ::= db_optr days */
  {  208,   -2 }, /* (107) db_optr ::= db_optr minrows */
  {  208,   -2 }, /* (108) db_optr ::= db_optr maxrows */
  {  208,   -2 }, /* (109) db_optr ::= db_optr blocks */
  {  208,   -2 }, /* (110) db_optr ::= db_optr ctime */
  {  208,   -2 }, /* (111) db_optr ::= db_optr wal */
  {  208,   -2 }, /* (112) db_optr ::= db_optr fsync */
  {  208,   -2 }, /* (113) db_optr ::= db_optr comp */
  {  208,   -2 }, /* (114) db_optr ::= db_optr prec */
  {  208,   -2 }, /* (115) db_optr ::= db_optr keep */
  {  208,   -2 }, /* (116) db_optr ::= db_optr update */
  {  208,   -2 }, /* (117) db_optr ::= db_optr cachelast */
  {  204,    0 }, /* (118) alter_db_optr ::= */
  {  204,   -2 }, /* (119) alter_db_optr ::= alter_db_optr replica */
  {  204,   -2 }, /* (120) alter_db_optr ::= alter_db_optr quorum */
  {  204,   -2 }, /* (121) alter_db_optr ::= alter_db_optr keep */
  {  204,   -2 }, /* (122) alter_db_optr ::= alter_db_optr blocks */
  {  204,   -2 }, /* (123) alter_db_optr ::= alter_db_optr comp */
  {  204,   -2 }, /* (124) alter_db_optr ::= alter_db_optr update */
  {  204,   -2 }, /* (125) alter_db_optr ::= alter_db_optr cachelast */
  {  209,   -1 }, /* (126) typename ::= ids */
  {  209,   -4 }, /* (127) typename ::= ids LP signed RP */
  {  209,   -2 }, /* (128) typename ::= ids UNSIGNED */
  {  237,   -1 }, /* (129) signed ::= INTEGER */
  {  237,   -2 }, /* (130) signed ::= PLUS INTEGER */
  {  237,   -2 }, /* (131) signed ::= MINUS INTEGER */
  {  199,   -3 }, /* (132) cmd ::= CREATE TABLE create_table_args */
  {  199,   -3 }, /* (133) cmd ::= CREATE TABLE create_stable_args */
  {  199,   -3 }, /* (134) cmd ::= CREATE STABLE create_stable_args */
  {  199,   -3 }, /* (135) cmd ::= CREATE TABLE create_table_list */
  {  240,   -1 }, /* (136) create_table_list ::= create_from_stable */
  {  240,   -2 }, /* (137) create_table_list ::= create_table_list create_from_stable */
  {  238,   -6 }, /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  239,  -10 }, /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  241,  -10 }, /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  241,  -13 }, /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  244,   -3 }, /* (142) tagNamelist ::= tagNamelist COMMA ids */
  {  244,   -1 }, /* (143) tagNamelist ::= ids */
  {  238,   -5 }, /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
  {  242,   -3 }, /* (145) columnlist ::= columnlist COMMA column */
  {  242,   -1 }, /* (146) columnlist ::= column */
  {  246,   -2 }, /* (147) column ::= ids typename */
  {  243,   -3 }, /* (148) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  243,   -1 }, /* (149) tagitemlist1 ::= tagitem1 */
  {  247,   -2 }, /* (150) tagitem1 ::= MINUS INTEGER */
  {  247,   -2 }, /* (151) tagitem1 ::= MINUS FLOAT */
  {  247,   -2 }, /* (152) tagitem1 ::= PLUS INTEGER */
  {  247,   -2 }, /* (153) tagitem1 ::= PLUS FLOAT */
  {  247,   -1 }, /* (154) tagitem1 ::= INTEGER */
  {  247,   -1 }, /* (155) tagitem1 ::= FLOAT */
  {  247,   -1 }, /* (156) tagitem1 ::= STRING */
  {  247,   -1 }, /* (157) tagitem1 ::= BOOL */
  {  247,   -1 }, /* (158) tagitem1 ::= NULL */
  {  247,   -1 }, /* (159) tagitem1 ::= NOW */
  {  248,   -3 }, /* (160) tagitemlist ::= tagitemlist COMMA tagitem */
  {  248,   -1 }, /* (161) tagitemlist ::= tagitem */
  {  249,   -1 }, /* (162) tagitem ::= INTEGER */
  {  249,   -1 }, /* (163) tagitem ::= FLOAT */
  {  249,   -1 }, /* (164) tagitem ::= STRING */
  {  249,   -1 }, /* (165) tagitem ::= BOOL */
  {  249,   -1 }, /* (166) tagitem ::= NULL */
  {  249,   -1 }, /* (167) tagitem ::= NOW */
  {  249,   -2 }, /* (168) tagitem ::= MINUS INTEGER */
  {  249,   -2 }, /* (169) tagitem ::= MINUS FLOAT */
  {  249,   -2 }, /* (170) tagitem ::= PLUS INTEGER */
  {  249,   -2 }, /* (171) tagitem ::= PLUS FLOAT */
  {  245,  -14 }, /* (172) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  245,   -3 }, /* (173) select ::= LP select RP */
  {  263,   -1 }, /* (174) union ::= select */
  {  263,   -4 }, /* (175) union ::= union UNION ALL select */
  {  263,   -3 }, /* (176) union ::= union UNION select */
  {  199,   -1 }, /* (177) cmd ::= union */
  {  245,   -2 }, /* (178) select ::= SELECT selcollist */
  {  264,   -2 }, /* (179) sclp ::= selcollist COMMA */
  {  264,    0 }, /* (180) sclp ::= */
  {  250,   -4 }, /* (181) selcollist ::= sclp distinct expr as */
  {  250,   -2 }, /* (182) selcollist ::= sclp STAR */
  {  267,   -2 }, /* (183) as ::= AS ids */
  {  267,   -1 }, /* (184) as ::= ids */
  {  267,    0 }, /* (185) as ::= */
  {  265,   -1 }, /* (186) distinct ::= DISTINCT */
  {  265,    0 }, /* (187) distinct ::= */
  {  251,   -2 }, /* (188) from ::= FROM tablelist */
  {  251,   -2 }, /* (189) from ::= FROM sub */
  {  269,   -3 }, /* (190) sub ::= LP union RP */
  {  269,   -4 }, /* (191) sub ::= LP union RP ids */
  {  269,   -6 }, /* (192) sub ::= sub COMMA LP union RP ids */
  {  268,   -2 }, /* (193) tablelist ::= ids cpxName */
  {  268,   -3 }, /* (194) tablelist ::= ids cpxName ids */
  {  268,   -4 }, /* (195) tablelist ::= tablelist COMMA ids cpxName */
  {  268,   -5 }, /* (196) tablelist ::= tablelist COMMA ids cpxName ids */
  {  270,   -1 }, /* (197) tmvar ::= VARIABLE */
  {  253,   -4 }, /* (198) interval_option ::= intervalKey LP tmvar RP */
  {  253,   -6 }, /* (199) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  253,    0 }, /* (200) interval_option ::= */
  {  271,   -1 }, /* (201) intervalKey ::= INTERVAL */
  {  271,   -1 }, /* (202) intervalKey ::= EVERY */
  {  255,    0 }, /* (203) session_option ::= */
  {  255,   -7 }, /* (204) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  256,    0 }, /* (205) windowstate_option ::= */
  {  256,   -4 }, /* (206) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  257,    0 }, /* (207) fill_opt ::= */
  {  257,   -6 }, /* (208) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  257,   -4 }, /* (209) fill_opt ::= FILL LP ID RP */
  {  254,   -4 }, /* (210) sliding_opt ::= SLIDING LP tmvar RP */
  {  254,    0 }, /* (211) sliding_opt ::= */
  {  260,    0 }, /* (212) orderby_opt ::= */
  {  260,   -3 }, /* (213) orderby_opt ::= ORDER BY sortlist */
  {  272,   -4 }, /* (214) sortlist ::= sortlist COMMA item sortorder */
  {  272,   -2 }, /* (215) sortlist ::= item sortorder */
  {  274,   -2 }, /* (216) item ::= ids cpxName */
  {  275,   -1 }, /* (217) sortorder ::= ASC */
  {  275,   -1 }, /* (218) sortorder ::= DESC */
  {  275,    0 }, /* (219) sortorder ::= */
  {  258,    0 }, /* (220) groupby_opt ::= */
  {  258,   -3 }, /* (221) groupby_opt ::= GROUP BY grouplist */
  {  276,   -3 }, /* (222) grouplist ::= grouplist COMMA item */
  {  276,   -1 }, /* (223) grouplist ::= item */
  {  259,    0 }, /* (224) having_opt ::= */
  {  259,   -2 }, /* (225) having_opt ::= HAVING expr */
  {  262,    0 }, /* (226) limit_opt ::= */
  {  262,   -2 }, /* (227) limit_opt ::= LIMIT signed */
  {  262,   -4 }, /* (228) limit_opt ::= LIMIT signed OFFSET signed */
  {  262,   -4 }, /* (229) limit_opt ::= LIMIT signed COMMA signed */
  {  261,    0 }, /* (230) slimit_opt ::= */
  {  261,   -2 }, /* (231) slimit_opt ::= SLIMIT signed */
  {  261,   -4 }, /* (232) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  261,   -4 }, /* (233) slimit_opt ::= SLIMIT signed COMMA signed */
  {  252,    0 }, /* (234) where_opt ::= */
  {  252,   -2 }, /* (235) where_opt ::= WHERE expr */
  {  266,   -3 }, /* (236) expr ::= LP expr RP */
  {  266,   -1 }, /* (237) expr ::= ID */
  {  266,   -3 }, /* (238) expr ::= ID DOT ID */
  {  266,   -3 }, /* (239) expr ::= ID DOT STAR */
  {  266,   -1 }, /* (240) expr ::= INTEGER */
  {  266,   -2 }, /* (241) expr ::= MINUS INTEGER */
  {  266,   -2 }, /* (242) expr ::= PLUS INTEGER */
  {  266,   -1 }, /* (243) expr ::= FLOAT */
  {  266,   -2 }, /* (244) expr ::= MINUS FLOAT */
  {  266,   -2 }, /* (245) expr ::= PLUS FLOAT */
  {  266,   -1 }, /* (246) expr ::= STRING */
  {  266,   -1 }, /* (247) expr ::= NOW */
  {  266,   -1 }, /* (248) expr ::= VARIABLE */
  {  266,   -2 }, /* (249) expr ::= PLUS VARIABLE */
  {  266,   -2 }, /* (250) expr ::= MINUS VARIABLE */
  {  266,   -1 }, /* (251) expr ::= BOOL */
  {  266,   -1 }, /* (252) expr ::= NULL */
  {  266,   -4 }, /* (253) expr ::= ID LP exprlist RP */
  {  266,   -4 }, /* (254) expr ::= ID LP STAR RP */
  {  266,   -3 }, /* (255) expr ::= expr IS NULL */
  {  266,   -4 }, /* (256) expr ::= expr IS NOT NULL */
  {  266,   -3 }, /* (257) expr ::= expr LT expr */
  {  266,   -3 }, /* (258) expr ::= expr GT expr */
  {  266,   -3 }, /* (259) expr ::= expr LE expr */
  {  266,   -3 }, /* (260) expr ::= expr GE expr */
  {  266,   -3 }, /* (261) expr ::= expr NE expr */
  {  266,   -3 }, /* (262) expr ::= expr EQ expr */
  {  266,   -5 }, /* (263) expr ::= expr BETWEEN expr AND expr */
  {  266,   -3 }, /* (264) expr ::= expr AND expr */
  {  266,   -3 }, /* (265) expr ::= expr OR expr */
  {  266,   -3 }, /* (266) expr ::= expr PLUS expr */
  {  266,   -3 }, /* (267) expr ::= expr MINUS expr */
  {  266,   -3 }, /* (268) expr ::= expr STAR expr */
  {  266,   -3 }, /* (269) expr ::= expr SLASH expr */
  {  266,   -3 }, /* (270) expr ::= expr REM expr */
  {  266,   -3 }, /* (271) expr ::= expr LIKE expr */
  {  266,   -3 }, /* (272) expr ::= expr MATCH expr */
  {  266,   -3 }, /* (273) expr ::= expr NMATCH expr */
  {  266,   -5 }, /* (274) expr ::= expr IN LP exprlist RP */
  {  206,   -3 }, /* (275) exprlist ::= exprlist COMMA expritem */
  {  206,   -1 }, /* (276) exprlist ::= expritem */
  {  277,   -1 }, /* (277) expritem ::= expr */
  {  277,    0 }, /* (278) expritem ::= */
  {  199,   -3 }, /* (279) cmd ::= RESET QUERY CACHE */
  {  199,   -3 }, /* (280) cmd ::= SYNCDB ids REPLICA */
  {  199,   -7 }, /* (281) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (282) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (286) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (287) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (288) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -7 }, /* (289) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (290) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  199,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (294) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (295) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -7 }, /* (296) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  199,   -3 }, /* (297) cmd ::= KILL CONNECTION INTEGER */
  {  199,   -5 }, /* (298) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  199,   -5 }, /* (299) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 132: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==134);
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
      case 187: /* distinct ::= */ yytestcase(yyruleno==187);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids PORT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy90, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy100, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy100, &yymsp[0].minor.yy0, 2);}
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
      case 84: /* intitemlist ::= intitemlist COMMA intitem */
      case 160: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==160);
{ yylhsminor.yy421 = tListItemAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy69, -1);    }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 85: /* intitemlist ::= intitem */
      case 161: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[0].minor.yy69, -1); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 86: /* intitem ::= INTEGER */
      case 162: /* tagitem ::= INTEGER */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= STRING */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= BOOL */ yytestcase(yyruleno==165);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 87: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 102: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy90);}
        break;
      case 103: /* db_optr ::= db_optr cache */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 104: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 105: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 106: /* db_optr ::= db_optr days */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 107: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 108: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 109: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 110: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 111: /* db_optr ::= db_optr wal */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 112: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 113: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 114: /* db_optr ::= db_optr prec */
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 115: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.keep = yymsp[0].minor.yy421; }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 116: /* db_optr ::= db_optr update */
      case 124: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==124);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 117: /* db_optr ::= db_optr cachelast */
      case 125: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==125);
{ yylhsminor.yy90 = yymsp[-1].minor.yy90; yylhsminor.yy90.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy90);}
        break;
      case 126: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy100, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy100 = yylhsminor.yy100;
        break;
      case 127: /* typename ::= ids LP signed RP */
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
      case 128: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy100, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 129: /* signed ::= INTEGER */
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 130: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 131: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 135: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
        break;
      case 136: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_CTABLE;
  yylhsminor.yy438 = pCreateTable;
}
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 137: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 138: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 139: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy152 = yylhsminor.yy152;
        break;
      case 142: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 143: /* tagNamelist ::= ids */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy56, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 145: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy100); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 146: /* columnlist ::= column */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy100);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 147: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy100, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy100);
}
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 148: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy0); yylhsminor.yy421 = yymsp[-2].minor.yy421;}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 149: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy421 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 150: /* tagitem1 ::= MINUS INTEGER */
      case 151: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==151);
      case 152: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==152);
      case 153: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==153);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 154: /* tagitem1 ::= INTEGER */
      case 155: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem1 ::= STRING */ yytestcase(yyruleno==156);
      case 157: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==157);
      case 158: /* tagitem1 ::= NULL */ yytestcase(yyruleno==158);
      case 159: /* tagitem1 ::= NOW */ yytestcase(yyruleno==159);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 166: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 167: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy69, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 168: /* tagitem ::= MINUS INTEGER */
      case 169: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==169);
      case 170: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==170);
      case 171: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==171);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy69, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy69 = yylhsminor.yy69;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy421, yymsp[-11].minor.yy8, yymsp[-10].minor.yy439, yymsp[-4].minor.yy421, yymsp[-2].minor.yy421, &yymsp[-9].minor.yy400, &yymsp[-7].minor.yy147, &yymsp[-6].minor.yy40, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy421, &yymsp[0].minor.yy231, &yymsp[-1].minor.yy231, yymsp[-3].minor.yy439);
}
  yymsp[-13].minor.yy56 = yylhsminor.yy56;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy56 = yymsp[-1].minor.yy56;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy149 = setSubclause(NULL, yymsp[0].minor.yy56); }
  yymsp[0].minor.yy149 = yylhsminor.yy149;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy149 = appendSelectClause(yymsp[-3].minor.yy149, SQL_TYPE_UNIONALL, yymsp[0].minor.yy56);  }
  yymsp[-3].minor.yy149 = yylhsminor.yy149;
        break;
      case 176: /* union ::= union UNION select */
{ yylhsminor.yy149 = appendSelectClause(yymsp[-2].minor.yy149, SQL_TYPE_UNION, yymsp[0].minor.yy56);  }
  yymsp[-2].minor.yy149 = yylhsminor.yy149;
        break;
      case 177: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy149, NULL, TSDB_SQL_SELECT); }
        break;
      case 178: /* select ::= SELECT selcollist */
{
  yylhsminor.yy56 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy421, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 179: /* sclp ::= selcollist COMMA */
{yylhsminor.yy421 = yymsp[-1].minor.yy421;}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 180: /* sclp ::= */
      case 212: /* orderby_opt ::= */ yytestcase(yyruleno==212);
{yymsp[1].minor.yy421 = 0;}
        break;
      case 181: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-3].minor.yy421, yymsp[-1].minor.yy439,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 182: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy421 = tSqlExprListAppend(yymsp[-1].minor.yy421, pNode, 0, 0);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 183: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 184: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 185: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 186: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 188: /* from ::= FROM tablelist */
      case 189: /* from ::= FROM sub */ yytestcase(yyruleno==189);
{yymsp[-1].minor.yy8 = yymsp[0].minor.yy8;}
        break;
      case 190: /* sub ::= LP union RP */
{yymsp[-2].minor.yy8 = addSubquery(NULL, yymsp[-1].minor.yy149, NULL);}
        break;
      case 191: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy8 = addSubquery(NULL, yymsp[-2].minor.yy149, &yymsp[0].minor.yy0);}
        break;
      case 192: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy8 = addSubquery(yymsp[-5].minor.yy8, yymsp[-2].minor.yy149, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy8 = yylhsminor.yy8;
        break;
      case 193: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy8 = yylhsminor.yy8;
        break;
      case 194: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-3].minor.yy8, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy8 = yylhsminor.yy8;
        break;
      case 196: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy8 = setTableNameList(yymsp[-4].minor.yy8, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy8 = yylhsminor.yy8;
        break;
      case 197: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 198: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy400.interval = yymsp[-1].minor.yy0; yylhsminor.yy400.offset.n = 0; yylhsminor.yy400.token = yymsp[-3].minor.yy104;}
  yymsp[-3].minor.yy400 = yylhsminor.yy400;
        break;
      case 199: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy400.interval = yymsp[-3].minor.yy0; yylhsminor.yy400.offset = yymsp[-1].minor.yy0;   yylhsminor.yy400.token = yymsp[-5].minor.yy104;}
  yymsp[-5].minor.yy400 = yylhsminor.yy400;
        break;
      case 200: /* interval_option ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 201: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy104 = TK_INTERVAL;}
        break;
      case 202: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy104 = TK_EVERY;   }
        break;
      case 203: /* session_option ::= */
{yymsp[1].minor.yy147.col.n = 0; yymsp[1].minor.yy147.gap.n = 0;}
        break;
      case 204: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy147.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy147.gap = yymsp[-1].minor.yy0;
}
        break;
      case 205: /* windowstate_option ::= */
{ yymsp[1].minor.yy40.col.n = 0; yymsp[1].minor.yy40.col.z = NULL;}
        break;
      case 206: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy40.col = yymsp[-1].minor.yy0; }
        break;
      case 207: /* fill_opt ::= */
{ yymsp[1].minor.yy421 = 0;     }
        break;
      case 208: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
        break;
      case 209: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 210: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 211: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 213: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 214: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy421 = tListItemAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy69, yymsp[0].minor.yy96);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 215: /* sortlist ::= item sortorder */
{
  yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[-1].minor.yy69, yymsp[0].minor.yy96);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 216: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy69, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy69 = yylhsminor.yy69;
        break;
      case 217: /* sortorder ::= ASC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 218: /* sortorder ::= DESC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
        break;
      case 219: /* sortorder ::= */
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 220: /* groupby_opt ::= */
{ yymsp[1].minor.yy421 = 0;}
        break;
      case 221: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 222: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy421 = tListItemAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy69, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 223: /* grouplist ::= item */
{
  yylhsminor.yy421 = tListItemAppend(NULL, &yymsp[0].minor.yy69, -1);
}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 224: /* having_opt ::= */
      case 234: /* where_opt ::= */ yytestcase(yyruleno==234);
      case 278: /* expritem ::= */ yytestcase(yyruleno==278);
{yymsp[1].minor.yy439 = 0;}
        break;
      case 225: /* having_opt ::= HAVING expr */
      case 235: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==235);
{yymsp[-1].minor.yy439 = yymsp[0].minor.yy439;}
        break;
      case 226: /* limit_opt ::= */
      case 230: /* slimit_opt ::= */ yytestcase(yyruleno==230);
{yymsp[1].minor.yy231.limit = -1; yymsp[1].minor.yy231.offset = 0;}
        break;
      case 227: /* limit_opt ::= LIMIT signed */
      case 231: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==231);
{yymsp[-1].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy231.offset = 0;}
        break;
      case 228: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy231.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[0].minor.yy325;}
        break;
      case 229: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[-2].minor.yy325;}
        break;
      case 232: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy231.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[0].minor.yy325;}
        break;
      case 233: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy231.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy231.offset = yymsp[-2].minor.yy325;}
        break;
      case 236: /* expr ::= LP expr RP */
{yylhsminor.yy439 = yymsp[-1].minor.yy439; yylhsminor.yy439->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy439->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 237: /* expr ::= ID */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 238: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 239: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 240: /* expr ::= INTEGER */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 241: /* expr ::= MINUS INTEGER */
      case 242: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==242);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 243: /* expr ::= FLOAT */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 244: /* expr ::= MINUS FLOAT */
      case 245: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==245);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 246: /* expr ::= STRING */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 247: /* expr ::= NOW */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 248: /* expr ::= VARIABLE */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 249: /* expr ::= PLUS VARIABLE */
      case 250: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==250);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy439 = yylhsminor.yy439;
        break;
      case 251: /* expr ::= BOOL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 252: /* expr ::= NULL */
{ yylhsminor.yy439 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 253: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(yymsp[-1].minor.yy421, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 254: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy439 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 255: /* expr ::= expr IS NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 256: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-3].minor.yy439, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy439 = yylhsminor.yy439;
        break;
      case 257: /* expr ::= expr LT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 258: /* expr ::= expr GT expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GT);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 259: /* expr ::= expr LE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 260: /* expr ::= expr GE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_GE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 261: /* expr ::= expr NE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 262: /* expr ::= expr EQ expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_EQ);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 263: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy439); yylhsminor.yy439 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy439, yymsp[-2].minor.yy439, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy439, TK_LE), TK_AND);}
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 264: /* expr ::= expr AND expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_AND);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 265: /* expr ::= expr OR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_OR); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 266: /* expr ::= expr PLUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_PLUS);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 267: /* expr ::= expr MINUS expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MINUS); }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 268: /* expr ::= expr STAR expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_STAR);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 269: /* expr ::= expr SLASH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_DIVIDE);}
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 270: /* expr ::= expr REM expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_REM);   }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 271: /* expr ::= expr LIKE expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_LIKE);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 272: /* expr ::= expr MATCH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_MATCH);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 273: /* expr ::= expr NMATCH expr */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-2].minor.yy439, yymsp[0].minor.yy439, TK_NMATCH);  }
  yymsp[-2].minor.yy439 = yylhsminor.yy439;
        break;
      case 274: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy439 = tSqlExprCreate(yymsp[-4].minor.yy439, (tSqlExpr*)yymsp[-1].minor.yy421, TK_IN); }
  yymsp[-4].minor.yy439 = yylhsminor.yy439;
        break;
      case 275: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy421 = tSqlExprListAppend(yymsp[-2].minor.yy421,yymsp[0].minor.yy439,0, 0);}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 276: /* exprlist ::= expritem */
{yylhsminor.yy421 = tSqlExprListAppend(0,yymsp[0].minor.yy439,0, 0);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 277: /* expritem ::= expr */
{yylhsminor.yy439 = yymsp[0].minor.yy439;}
  yymsp[0].minor.yy439 = yylhsminor.yy439;
        break;
      case 279: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 280: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 287: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy69, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 295: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy69, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 298: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 299: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
