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
#define YYNOCODE 277
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  TAOS_FIELD yy31;
  int yy52;
  SLimitVal yy126;
  SWindowStateVal yy144;
  SCreateTableSql* yy158;
  SCreateDbInfo yy214;
  SSessionWindowVal yy259;
  tSqlExpr* yy370;
  SRelationInfo* yy412;
  SCreatedTableInfo yy432;
  SSqlNode* yy464;
  int64_t yy501;
  tVariant yy506;
  SIntervalVal yy520;
  SArray* yy525;
  SCreateAcctInfo yy547;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             362
#define YYNRULE              289
#define YYNTOKEN             195
#define YY_MAX_SHIFT         361
#define YY_MIN_SHIFTREDUCE   567
#define YY_MAX_SHIFTREDUCE   855
#define YY_ERROR_ACTION      856
#define YY_ACCEPT_ACTION     857
#define YY_NO_ACTION         858
#define YY_MIN_REDUCE        859
#define YY_MAX_REDUCE        1147
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
#define YY_ACTTAB_COUNT (753)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   206,  618,  245,  618,  618,   97,  244,  228,  359,  619,
 /*    10 */  1123,  619,  619,   56,   57,  152,   60,   61,  654, 1027,
 /*    20 */   248,   50, 1036,   59,  317,   64,   62,   65,   63,  984,
 /*    30 */   249,  982,  983,   55,   54,  231,  985,   53,   52,   51,
 /*    40 */   986, 1002,  987,  988,   53,   52,   51,  568,  569,  570,
 /*    50 */   571,  572,  573,  574,  575,  576,  577,  578,  579,  580,
 /*    60 */   581,  360,  206,  257,  229,  159,  206,   56,   57,   37,
 /*    70 */    60,   61, 1124,  174,  248,   50, 1124,   59,  317,   64,
 /*    80 */    62,   65,   63,  277,  276,   29,   79,   55,   54, 1033,
 /*    90 */   206,   53,   52,   51,   56,   57,  315,   60,   61,  234,
 /*   100 */  1124,  248,   50, 1014,   59,  317,   64,   62,   65,   63,
 /*   110 */   358,  357,  144,  230,   55,   54,   85, 1011,   53,   52,
 /*   120 */    51,   56,   58,  240,   60,   61,  347, 1014,  248,   50,
 /*   130 */    94,   59,  317,   64,   62,   65,   63,  794, 1073,  242,
 /*   140 */   289,   55,   54, 1014,  618,   53,   52,   51,   57,   23,
 /*   150 */    60,   61,  619,   44,  248,   50, 1000,   59,  317,   64,
 /*   160 */    62,   65,   63,  997,  998,   34, 1001,   55,   54,  857,
 /*   170 */   361,   53,   52,   51,   43,  313,  354,  353,  312,  311,
 /*   180 */   310,  352,  309,  308,  307,  351,  306,  350,  349,  976,
 /*   190 */   964,  965,  966,  967,  968,  969,  970,  971,  972,  973,
 /*   200 */   974,  975,  977,  978,   60,   61,   24, 1008,  248,   50,
 /*   210 */   257,   59,  317,   64,   62,   65,   63, 1027,  122, 1027,
 /*   220 */   175,   55,   54,   37,  209,   53,   52,   51,  247,  809,
 /*   230 */   347,  215,  798,  232,  801,  270,  804,  135,  134,  214,
 /*   240 */   315,  247,  809,  322,   85,  798,   14,  801,   37,  804,
 /*   250 */    93,  159,  726,  241,  203,  723,  800,  724,  803,  725,
 /*   260 */   226,  227,  257,   16,  318,   15,   37,  238,    5,   40,
 /*   270 */   178, 1011, 1012,  226,  227,  177,  103,  108,   99,  107,
 /*   280 */    96,   44,  204,  253,  254,  210,   64,   62,   65,   63,
 /*   290 */   355,  945,  159,  302,   55,   54, 1010,  251,   53,   52,
 /*   300 */    51, 1013,   78,  269,  256,   77,  120,  114,  125,   66,
 /*   310 */   239,  702,  222,  124, 1011,  130,  133,  123,   37,  197,
 /*   320 */   195,  193,   66,  127, 1072,   37,  192,  139,  138,  137,
 /*   330 */   136,  799,  159,  802,   37,   43,  999,  354,  353,  337,
 /*   340 */   336,   37,  352,  262,  810,  805,  351,   37,  350,  349,
 /*   350 */    37,  806,  266,  265,  742,   55,   54,  810,  805,   53,
 /*   360 */    52,   51,  326,  291,  806,   90, 1011,  727,  728,  327,
 /*   370 */    37,   37,  252, 1011,  250,  807,  325,  324,  328,  258,
 /*   380 */    82,  255, 1011,  332,  331,  329,  150,  148,  147, 1011,
 /*   390 */   907,  333,   83,  917,  334, 1011,  908,  188, 1011,  271,
 /*   400 */   188,  739,   92,  188,   70,   91,    1,  176,    3,  189,
 /*   410 */   775,  776,  758,   38,  335,  339,   80,  273, 1011, 1011,
 /*   420 */   766,  767,   73,  712,  294,   33,  154,    9,  714,  273,
 /*   430 */   296,  713,  796,  830,   67,   26,  246,   38,   38,  746,
 /*   440 */   811,  319,   67,   76,   95,   67,   71,   25, 1120,  617,
 /*   450 */   808,  132,  131,  113,   25,  112, 1119,    6,  297,   18,
 /*   460 */  1118,   17,   74,   25,  731,  729,  732,  730,  797,   20,
 /*   470 */  1083,   19,  119,  224,  118,  701,   22,  225,   21,  207,
 /*   480 */   208,  211,  205,  212,  213,  217,  218,  219,  216,  202,
 /*   490 */  1143, 1082, 1135,  236,  267, 1079, 1078,  237,  338,  151,
 /*   500 */  1035, 1046,   47, 1065, 1043,  149, 1064, 1025, 1028, 1044,
 /*   510 */   274, 1048,  153,  170,  157, 1009,  278,  283,  171, 1007,
 /*   520 */   172,  233,  166,  280,  161,  757,  160,  173,  162,  922,
 /*   530 */   163,  299,  300,  301,  304,  305,  287,  292,   45,  290,
 /*   540 */    75,  200,  288,  813,  272,   41,   72,   49,  316,  164,
 /*   550 */   916,  323, 1142,  110, 1141, 1138,  286,  179,  330, 1134,
 /*   560 */   284,  116, 1133, 1130,  180,  282,  942,   42,   39,   46,
 /*   570 */   201,  904,  279,  126,   48,  902,  128,  129,  900,  899,
 /*   580 */   259,  191,  897,  896,  895,  894,  893,  892,  891,  194,
 /*   590 */   196,  888,  886,  884,  882,  198,  879,  199,  303,   81,
 /*   600 */    86,  348,  281, 1066,  121,  340,  341,  342,  343,  344,
 /*   610 */   223,  345,  346,  356,  855,  243,  298,  260,  261,  854,
 /*   620 */   263,  220,  221,  264,  853,  836,  104,  921,  920,  105,
 /*   630 */   835,  268,  273,   10,  293,  734,  275,   84,   30,   87,
 /*   640 */   898,  890,  182,  943,  186,  181,  184,  140,  183,  187,
 /*   650 */   185,  141,  142,  889,    4,  143,  980,  881,  880,  944,
 /*   660 */   759,  165,  167,  168,  155,  169,  762,  156,    2,  990,
 /*   670 */    88,  235,  764,   89,  285,   31,  768,  158,   11,   12,
 /*   680 */    13,   32,   27,  295,   28,   96,   98,  101,   35,  100,
 /*   690 */   632,   36,  102,  667,  665,  664,  663,  661,  660,  659,
 /*   700 */   656,  314,  622,  106,    7,  320,  812,  814,    8,  321,
 /*   710 */   109,  111,   68,   69,  115,  704,  703,   38,  117,  700,
 /*   720 */   648,  646,  638,  644,  640,  642,  636,  634,  670,  669,
 /*   730 */   668,  666,  662,  658,  657,  190,  620,  585,  583,  859,
 /*   740 */   858,  858,  858,  858,  858,  858,  858,  858,  858,  858,
 /*   750 */   858,  145,  146,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   265,    1,  205,    1,    1,  206,  205,  198,  199,    9,
 /*    10 */   275,    9,    9,   13,   14,  199,   16,   17,    5,  246,
 /*    20 */    20,   21,  199,   23,   24,   25,   26,   27,   28,  222,
 /*    30 */   205,  224,  225,   33,   34,  262,  229,   37,   38,   39,
 /*    40 */   233,  242,  235,  236,   37,   38,   39,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  265,  199,   62,  199,  265,   13,   14,  199,
 /*    70 */    16,   17,  275,  209,   20,   21,  275,   23,   24,   25,
 /*    80 */    26,   27,   28,  267,  268,   82,   86,   33,   34,  266,
 /*    90 */   265,   37,   38,   39,   13,   14,   84,   16,   17,  244,
 /*   100 */   275,   20,   21,  248,   23,   24,   25,   26,   27,   28,
 /*   110 */    66,   67,   68,  243,   33,   34,   82,  247,   37,   38,
 /*   120 */    39,   13,   14,  244,   16,   17,   90,  248,   20,   21,
 /*   130 */   206,   23,   24,   25,   26,   27,   28,   83,  272,  244,
 /*   140 */   274,   33,   34,  248,    1,   37,   38,   39,   14,  265,
 /*   150 */    16,   17,    9,  119,   20,   21,    0,   23,   24,   25,
 /*   160 */    26,   27,   28,  239,  240,  241,  242,   33,   34,  196,
 /*   170 */   197,   37,   38,   39,   98,   99,  100,  101,  102,  103,
 /*   180 */   104,  105,  106,  107,  108,  109,  110,  111,  112,  222,
 /*   190 */   223,  224,  225,  226,  227,  228,  229,  230,  231,  232,
 /*   200 */   233,  234,  235,  236,   16,   17,   44,  199,   20,   21,
 /*   210 */   199,   23,   24,   25,   26,   27,   28,  246,   78,  246,
 /*   220 */   209,   33,   34,  199,   62,   37,   38,   39,    1,    2,
 /*   230 */    90,   69,    5,  262,    7,  262,    9,   75,   76,   77,
 /*   240 */    84,    1,    2,   81,   82,    5,   82,    7,  199,    9,
 /*   250 */    86,  199,    2,  245,  265,    5,    5,    7,    7,    9,
 /*   260 */    33,   34,  199,  145,   37,  147,  199,  243,   63,   64,
 /*   270 */    65,  247,  209,   33,   34,   70,   71,   72,   73,   74,
 /*   280 */   116,  119,  265,   33,   34,  265,   25,   26,   27,   28,
 /*   290 */   220,  221,  199,   88,   33,   34,  247,   69,   37,   38,
 /*   300 */    39,  248,  206,  141,   69,  143,   63,   64,   65,   82,
 /*   310 */   243,    5,  150,   70,  247,   72,   73,   74,  199,   63,
 /*   320 */    64,   65,   82,   80,  272,  199,   70,   71,   72,   73,
 /*   330 */    74,    5,  199,    7,  199,   98,  240,  100,  101,   33,
 /*   340 */    34,  199,  105,  142,  117,  118,  109,  199,  111,  112,
 /*   350 */   199,  124,  151,  152,   37,   33,   34,  117,  118,   37,
 /*   360 */    38,   39,  243,  270,  124,  272,  247,  117,  118,  243,
 /*   370 */   199,  199,  144,  247,  146,  124,  148,  149,  243,  144,
 /*   380 */    83,  146,  247,  148,  149,  243,   63,   64,   65,  247,
 /*   390 */   204,  243,   83,  204,  243,  247,  204,  211,  247,   83,
 /*   400 */   211,   97,  249,  211,   97,  272,  207,  208,  202,  203,
 /*   410 */   132,  133,   83,   97,  243,  243,  263,  120,  247,  247,
 /*   420 */    83,   83,   97,   83,   83,   82,   97,  123,   83,  120,
 /*   430 */    83,   83,    1,   83,   97,   97,   61,   97,   97,  122,
 /*   440 */    83,   15,   97,   82,   97,   97,  139,   97,  265,   83,
 /*   450 */   124,   78,   79,  145,   97,  147,  265,   82,  115,  145,
 /*   460 */   265,  147,  137,   97,    5,    5,    7,    7,   37,  145,
 /*   470 */   238,  147,  145,  265,  147,  114,  145,  265,  147,  265,
 /*   480 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   490 */   248,  238,  248,  238,  199,  238,  238,  238,  238,  199,
 /*   500 */   199,  199,  264,  273,  199,   61,  273,  261,  246,  199,
 /*   510 */   246,  199,  199,  250,  199,  246,  269,  199,  199,  199,
 /*   520 */   199,  269,  254,  269,  259,  124,  260,  199,  258,  199,
 /*   530 */   257,  199,  199,  199,  199,  199,  269,  130,  199,  134,
 /*   540 */   136,  199,  129,  117,  200,  199,  138,  135,  199,  256,
 /*   550 */   199,  199,  199,  199,  199,  199,  128,  199,  199,  199,
 /*   560 */   127,  199,  199,  199,  199,  126,  199,  199,  199,  199,
 /*   570 */   199,  199,  125,  199,  140,  199,  199,  199,  199,  199,
 /*   580 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   590 */   199,  199,  199,  199,  199,  199,  199,  199,   89,  200,
 /*   600 */   200,  113,  200,  200,   96,   95,   51,   92,   94,   55,
 /*   610 */   200,   93,   91,   84,    5,  200,  200,  153,    5,    5,
 /*   620 */   153,  200,  200,    5,    5,  100,  206,  210,  210,  206,
 /*   630 */    99,  142,  120,   82,  115,   83,   97,  121,   82,   97,
 /*   640 */   200,  200,  217,  219,  215,  218,  216,  201,  213,  212,
 /*   650 */   214,  201,  201,  200,  202,  201,  237,  200,  200,  221,
 /*   660 */    83,  255,  253,  252,   82,  251,   83,   97,  207,  237,
 /*   670 */    82,    1,   83,   82,   82,   97,   83,   82,  131,  131,
 /*   680 */    82,   97,   82,  115,   82,  116,   78,   71,   87,   86,
 /*   690 */     5,   87,   86,    9,    5,    5,    5,    5,    5,    5,
 /*   700 */     5,   15,   85,   78,   82,   24,   83,  117,   82,   59,
 /*   710 */   147,  147,   16,   16,  147,    5,    5,   97,  147,   83,
 /*   720 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   730 */     5,    5,    5,    5,    5,   97,   85,   61,   60,    0,
 /*   740 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   750 */   276,   21,   21,  276,  276,  276,  276,  276,  276,  276,
 /*   760 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   770 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   780 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   790 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   800 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   810 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   820 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   830 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   840 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   850 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   860 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   870 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   880 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   890 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   900 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   910 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   920 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   930 */   276,  276,  276,  276,  276,  276,  276,  276,  276,  276,
 /*   940 */   276,  276,  276,  276,  276,  276,  276,  276,
};
#define YY_SHIFT_COUNT    (361)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (739)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   162,   76,   76,  237,  237,   12,  227,  240,  240,    3,
 /*    10 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*    20 */   143,  143,  143,    0,    2,  240,  250,  250,  250,   34,
 /*    30 */    34,  143,  143,  143,  156,  143,  143,  143,  143,  140,
 /*    40 */    12,   36,   36,   13,  753,  753,  753,  240,  240,  240,
 /*    50 */   240,  240,  240,  240,  240,  240,  240,  240,  240,  240,
 /*    60 */   240,  240,  240,  240,  240,  240,  240,  250,  250,  250,
 /*    70 */   306,  306,  306,  306,  306,  306,  306,  143,  143,  143,
 /*    80 */   317,  143,  143,  143,   34,   34,  143,  143,  143,  143,
 /*    90 */   278,  278,  304,   34,  143,  143,  143,  143,  143,  143,
 /*   100 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   110 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   120 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   130 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   140 */   143,  143,  143,  143,  143,  143,  143,  143,  143,  143,
 /*   150 */   143,  444,  444,  444,  401,  401,  401,  444,  401,  444,
 /*   160 */   404,  408,  407,  412,  405,  413,  428,  433,  439,  447,
 /*   170 */   434,  444,  444,  444,  509,  509,  488,   12,   12,  444,
 /*   180 */   444,  508,  510,  555,  515,  514,  554,  518,  521,  488,
 /*   190 */    13,  444,  529,  529,  444,  529,  444,  529,  444,  444,
 /*   200 */   753,  753,   54,   81,   81,  108,   81,  134,  188,  205,
 /*   210 */   261,  261,  261,  261,  243,  256,  322,  322,  322,  322,
 /*   220 */   228,  235,  201,  164,    7,    7,  251,  326,   44,  323,
 /*   230 */   316,  297,  309,  329,  337,  338,  307,  325,  340,  341,
 /*   240 */   345,  347,  348,  343,  350,  357,  431,  375,  426,  366,
 /*   250 */   118,  308,  314,  459,  460,  324,  327,  361,  331,  373,
 /*   260 */   609,  464,  613,  614,  467,  618,  619,  525,  531,  489,
 /*   270 */   512,  519,  551,  516,  552,  556,  539,  542,  577,  582,
 /*   280 */   583,  570,  588,  589,  591,  670,  592,  593,  595,  578,
 /*   290 */   547,  584,  548,  598,  519,  600,  568,  602,  569,  608,
 /*   300 */   601,  603,  616,  685,  604,  606,  684,  689,  690,  691,
 /*   310 */   692,  693,  694,  695,  617,  686,  625,  622,  623,  590,
 /*   320 */   626,  681,  650,  696,  563,  564,  620,  620,  620,  620,
 /*   330 */   697,  567,  571,  620,  620,  620,  710,  711,  636,  620,
 /*   340 */   715,  716,  717,  718,  719,  720,  721,  722,  723,  724,
 /*   350 */   725,  726,  727,  728,  729,  638,  651,  730,  731,  676,
 /*   360 */   678,  739,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-265)
#define YY_REDUCE_MAX   (461)
static const short yy_reduce_ofst[] = {
 /*     0 */   -27,  -33,  -33, -193, -193,  -76, -203, -199, -175, -184,
 /*    10 */  -130, -134,   93,   24,   67,  119,  126,  135,  142,  148,
 /*    20 */   151,  171,  172, -177, -191, -265, -145, -121, -105, -227,
 /*    30 */   -29,   52,  133,    8, -201, -136,   11,   63,   49,  186,
 /*    40 */    96,  189,  192,   70,  153,  199,  206, -116,  -11,   17,
 /*    50 */    20,  183,  191,  195,  208,  212,  214,  215,  216,  217,
 /*    60 */   218,  219,  220,  221,  222,  223,  224,   53,  242,  244,
 /*    70 */   232,  253,  255,  257,  258,  259,  260,  295,  300,  301,
 /*    80 */   238,  302,  305,  310,  262,  264,  312,  313,  315,  318,
 /*    90 */   230,  233,  263,  269,  319,  320,  321,  328,  330,  332,
 /*   100 */   333,  334,  335,  336,  339,  342,  346,  349,  351,  352,
 /*   110 */   353,  354,  355,  356,  358,  359,  360,  362,  363,  364,
 /*   120 */   365,  367,  368,  369,  370,  371,  372,  374,  376,  377,
 /*   130 */   378,  379,  380,  381,  382,  383,  384,  385,  386,  387,
 /*   140 */   388,  389,  390,  391,  392,  393,  394,  395,  396,  397,
 /*   150 */   398,  344,  399,  400,  247,  252,  254,  402,  267,  403,
 /*   160 */   246,  266,  265,  270,  273,  293,  406,  268,  409,  411,
 /*   170 */   414,  410,  415,  416,  417,  418,  419,  420,  423,  421,
 /*   180 */   422,  424,  427,  425,  435,  430,  436,  429,  437,  432,
 /*   190 */   438,  440,  446,  450,  441,  451,  453,  454,  457,  458,
 /*   200 */   461,  452,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   856,  979,  918,  989,  905,  915, 1126, 1126, 1126,  856,
 /*    10 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    20 */   856,  856,  856, 1037,  876, 1126,  856,  856,  856,  856,
 /*    30 */   856,  856,  856,  856,  915,  856,  856,  856,  856,  925,
 /*    40 */   915,  925,  925,  856, 1032,  963,  981,  856,  856,  856,
 /*    50 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    60 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    70 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    80 */  1039, 1045, 1042,  856,  856,  856, 1047,  856,  856,  856,
 /*    90 */  1069, 1069, 1030,  856,  856,  856,  856,  856,  856,  856,
 /*   100 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   110 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   120 */   856,  856,  856,  856,  856,  856,  903,  856,  901,  856,
 /*   130 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   140 */   856,  856,  856,  856,  887,  856,  856,  856,  856,  856,
 /*   150 */   856,  878,  878,  878,  856,  856,  856,  878,  856,  878,
 /*   160 */  1076, 1080, 1062, 1074, 1070, 1061, 1057, 1055, 1053, 1052,
 /*   170 */  1084,  878,  878,  878,  923,  923,  919,  915,  915,  878,
 /*   180 */   878,  941,  939,  937,  929,  935,  931,  933,  927,  906,
 /*   190 */   856,  878,  913,  913,  878,  913,  878,  913,  878,  878,
 /*   200 */   963,  981,  856, 1085, 1075,  856, 1125, 1115, 1114,  856,
 /*   210 */  1121, 1113, 1112, 1111,  856,  856, 1107, 1110, 1109, 1108,
 /*   220 */   856,  856,  856,  856, 1117, 1116,  856,  856,  856,  856,
 /*   230 */   856,  856,  856,  856,  856,  856, 1081, 1077,  856,  856,
 /*   240 */   856,  856,  856,  856,  856,  856,  856, 1087,  856,  856,
 /*   250 */   856,  856,  856,  856,  856,  856,  856,  991,  856,  856,
 /*   260 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   270 */  1029,  856,  856,  856,  856,  856, 1041, 1040,  856,  856,
 /*   280 */   856,  856,  856,  856,  856,  856,  856,  856,  856, 1071,
 /*   290 */   856, 1063,  856,  856, 1003,  856,  856,  856,  856,  856,
 /*   300 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   310 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   320 */   856,  856,  856,  856,  856,  856, 1144, 1139, 1140, 1137,
 /*   330 */   856,  856,  856, 1136, 1131, 1132,  856,  856,  856, 1129,
 /*   340 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   350 */   856,  856,  856,  856,  856,  947,  856,  885,  883,  856,
 /*   360 */   874,  856,
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
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    1,  /*       DESC => ID */
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
  /*   77 */ "ALTER",
  /*   78 */ "PASS",
  /*   79 */ "PRIVILEGE",
  /*   80 */ "LOCAL",
  /*   81 */ "COMPACT",
  /*   82 */ "LP",
  /*   83 */ "RP",
  /*   84 */ "IF",
  /*   85 */ "EXISTS",
  /*   86 */ "AS",
  /*   87 */ "OUTPUTTYPE",
  /*   88 */ "AGGREGATE",
  /*   89 */ "BUFSIZE",
  /*   90 */ "PPS",
  /*   91 */ "TSERIES",
  /*   92 */ "DBS",
  /*   93 */ "STORAGE",
  /*   94 */ "QTIME",
  /*   95 */ "CONNS",
  /*   96 */ "STATE",
  /*   97 */ "COMMA",
  /*   98 */ "KEEP",
  /*   99 */ "CACHE",
  /*  100 */ "REPLICA",
  /*  101 */ "QUORUM",
  /*  102 */ "DAYS",
  /*  103 */ "MINROWS",
  /*  104 */ "MAXROWS",
  /*  105 */ "BLOCKS",
  /*  106 */ "CTIME",
  /*  107 */ "WAL",
  /*  108 */ "FSYNC",
  /*  109 */ "COMP",
  /*  110 */ "PRECISION",
  /*  111 */ "UPDATE",
  /*  112 */ "CACHELAST",
  /*  113 */ "PARTITIONS",
  /*  114 */ "UNSIGNED",
  /*  115 */ "TAGS",
  /*  116 */ "USING",
  /*  117 */ "NULL",
  /*  118 */ "NOW",
  /*  119 */ "SELECT",
  /*  120 */ "UNION",
  /*  121 */ "ALL",
  /*  122 */ "DISTINCT",
  /*  123 */ "FROM",
  /*  124 */ "VARIABLE",
  /*  125 */ "INTERVAL",
  /*  126 */ "SESSION",
  /*  127 */ "STATE_WINDOW",
  /*  128 */ "FILL",
  /*  129 */ "SLIDING",
  /*  130 */ "ORDER",
  /*  131 */ "BY",
  /*  132 */ "ASC",
  /*  133 */ "DESC",
  /*  134 */ "GROUP",
  /*  135 */ "HAVING",
  /*  136 */ "LIMIT",
  /*  137 */ "OFFSET",
  /*  138 */ "SLIMIT",
  /*  139 */ "SOFFSET",
  /*  140 */ "WHERE",
  /*  141 */ "RESET",
  /*  142 */ "QUERY",
  /*  143 */ "SYNCDB",
  /*  144 */ "ADD",
  /*  145 */ "COLUMN",
  /*  146 */ "MODIFY",
  /*  147 */ "TAG",
  /*  148 */ "CHANGE",
  /*  149 */ "SET",
  /*  150 */ "KILL",
  /*  151 */ "CONNECTION",
  /*  152 */ "STREAM",
  /*  153 */ "COLON",
  /*  154 */ "ABORT",
  /*  155 */ "AFTER",
  /*  156 */ "ATTACH",
  /*  157 */ "BEFORE",
  /*  158 */ "BEGIN",
  /*  159 */ "CASCADE",
  /*  160 */ "CLUSTER",
  /*  161 */ "CONFLICT",
  /*  162 */ "COPY",
  /*  163 */ "DEFERRED",
  /*  164 */ "DELIMITERS",
  /*  165 */ "DETACH",
  /*  166 */ "EACH",
  /*  167 */ "END",
  /*  168 */ "EXPLAIN",
  /*  169 */ "FAIL",
  /*  170 */ "FOR",
  /*  171 */ "IGNORE",
  /*  172 */ "IMMEDIATE",
  /*  173 */ "INITIALLY",
  /*  174 */ "INSTEAD",
  /*  175 */ "MATCH",
  /*  176 */ "KEY",
  /*  177 */ "OF",
  /*  178 */ "RAISE",
  /*  179 */ "REPLACE",
  /*  180 */ "RESTRICT",
  /*  181 */ "ROW",
  /*  182 */ "STATEMENT",
  /*  183 */ "TRIGGER",
  /*  184 */ "VIEW",
  /*  185 */ "SEMI",
  /*  186 */ "NONE",
  /*  187 */ "PREV",
  /*  188 */ "LINEAR",
  /*  189 */ "IMPORT",
  /*  190 */ "TBNAME",
  /*  191 */ "JOIN",
  /*  192 */ "INSERT",
  /*  193 */ "INTO",
  /*  194 */ "VALUES",
  /*  195 */ "error",
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
  /*  252 */ "interval_opt",
  /*  253 */ "session_option",
  /*  254 */ "windowstate_option",
  /*  255 */ "fill_opt",
  /*  256 */ "sliding_opt",
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
  /*  270 */ "sortlist",
  /*  271 */ "sortitem",
  /*  272 */ "item",
  /*  273 */ "sortorder",
  /*  274 */ "grouplist",
  /*  275 */ "expritem",
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
 /* 166 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
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
 /* 191 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 192 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 193 */ "interval_opt ::=",
 /* 194 */ "session_option ::=",
 /* 195 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 196 */ "windowstate_option ::=",
 /* 197 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 198 */ "fill_opt ::=",
 /* 199 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 200 */ "fill_opt ::= FILL LP ID RP",
 /* 201 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 202 */ "sliding_opt ::=",
 /* 203 */ "orderby_opt ::=",
 /* 204 */ "orderby_opt ::= ORDER BY sortlist",
 /* 205 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 206 */ "sortlist ::= item sortorder",
 /* 207 */ "item ::= ids cpxName",
 /* 208 */ "sortorder ::= ASC",
 /* 209 */ "sortorder ::= DESC",
 /* 210 */ "sortorder ::=",
 /* 211 */ "groupby_opt ::=",
 /* 212 */ "groupby_opt ::= GROUP BY grouplist",
 /* 213 */ "grouplist ::= grouplist COMMA item",
 /* 214 */ "grouplist ::= item",
 /* 215 */ "having_opt ::=",
 /* 216 */ "having_opt ::= HAVING expr",
 /* 217 */ "limit_opt ::=",
 /* 218 */ "limit_opt ::= LIMIT signed",
 /* 219 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 220 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 221 */ "slimit_opt ::=",
 /* 222 */ "slimit_opt ::= SLIMIT signed",
 /* 223 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 224 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 225 */ "where_opt ::=",
 /* 226 */ "where_opt ::= WHERE expr",
 /* 227 */ "expr ::= LP expr RP",
 /* 228 */ "expr ::= ID",
 /* 229 */ "expr ::= ID DOT ID",
 /* 230 */ "expr ::= ID DOT STAR",
 /* 231 */ "expr ::= INTEGER",
 /* 232 */ "expr ::= MINUS INTEGER",
 /* 233 */ "expr ::= PLUS INTEGER",
 /* 234 */ "expr ::= FLOAT",
 /* 235 */ "expr ::= MINUS FLOAT",
 /* 236 */ "expr ::= PLUS FLOAT",
 /* 237 */ "expr ::= STRING",
 /* 238 */ "expr ::= NOW",
 /* 239 */ "expr ::= VARIABLE",
 /* 240 */ "expr ::= PLUS VARIABLE",
 /* 241 */ "expr ::= MINUS VARIABLE",
 /* 242 */ "expr ::= BOOL",
 /* 243 */ "expr ::= NULL",
 /* 244 */ "expr ::= ID LP exprlist RP",
 /* 245 */ "expr ::= ID LP STAR RP",
 /* 246 */ "expr ::= expr IS NULL",
 /* 247 */ "expr ::= expr IS NOT NULL",
 /* 248 */ "expr ::= expr LT expr",
 /* 249 */ "expr ::= expr GT expr",
 /* 250 */ "expr ::= expr LE expr",
 /* 251 */ "expr ::= expr GE expr",
 /* 252 */ "expr ::= expr NE expr",
 /* 253 */ "expr ::= expr EQ expr",
 /* 254 */ "expr ::= expr BETWEEN expr AND expr",
 /* 255 */ "expr ::= expr AND expr",
 /* 256 */ "expr ::= expr OR expr",
 /* 257 */ "expr ::= expr PLUS expr",
 /* 258 */ "expr ::= expr MINUS expr",
 /* 259 */ "expr ::= expr STAR expr",
 /* 260 */ "expr ::= expr SLASH expr",
 /* 261 */ "expr ::= expr REM expr",
 /* 262 */ "expr ::= expr LIKE expr",
 /* 263 */ "expr ::= expr IN LP exprlist RP",
 /* 264 */ "exprlist ::= exprlist COMMA expritem",
 /* 265 */ "exprlist ::= expritem",
 /* 266 */ "expritem ::= expr",
 /* 267 */ "expritem ::=",
 /* 268 */ "cmd ::= RESET QUERY CACHE",
 /* 269 */ "cmd ::= SYNCDB ids REPLICA",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 286 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 287 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 288 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 205: /* exprlist */
    case 249: /* selcollist */
    case 263: /* sclp */
{
tSqlExprListDestroy((yypminor->yy525));
}
      break;
    case 220: /* intitemlist */
    case 222: /* keep */
    case 243: /* columnlist */
    case 244: /* tagitemlist */
    case 245: /* tagNamelist */
    case 255: /* fill_opt */
    case 257: /* groupby_opt */
    case 259: /* orderby_opt */
    case 270: /* sortlist */
    case 274: /* grouplist */
{
taosArrayDestroy((yypminor->yy525));
}
      break;
    case 241: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy158));
}
      break;
    case 246: /* select */
{
destroySqlNode((yypminor->yy464));
}
      break;
    case 250: /* from */
    case 267: /* tablelist */
    case 268: /* sub */
{
destroyRelationInfo((yypminor->yy412));
}
      break;
    case 251: /* where_opt */
    case 258: /* having_opt */
    case 265: /* expr */
    case 275: /* expritem */
{
tSqlExprDestroy((yypminor->yy370));
}
      break;
    case 262: /* union */
{
destroyAllSqlNode((yypminor->yy525));
}
      break;
    case 271: /* sortitem */
{
tVariantDestroy(&(yypminor->yy506));
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
  {  197,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  197,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  197,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  197,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  197,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  197,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  197,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  197,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  197,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  197,   -2 }, /* (38) cmd ::= USE ids */
  {  197,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
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
  {  246,  -14 }, /* (166) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
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
  {  252,   -4 }, /* (191) interval_opt ::= INTERVAL LP tmvar RP */
  {  252,   -6 }, /* (192) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  252,    0 }, /* (193) interval_opt ::= */
  {  253,    0 }, /* (194) session_option ::= */
  {  253,   -7 }, /* (195) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  254,    0 }, /* (196) windowstate_option ::= */
  {  254,   -4 }, /* (197) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  255,    0 }, /* (198) fill_opt ::= */
  {  255,   -6 }, /* (199) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  255,   -4 }, /* (200) fill_opt ::= FILL LP ID RP */
  {  256,   -4 }, /* (201) sliding_opt ::= SLIDING LP tmvar RP */
  {  256,    0 }, /* (202) sliding_opt ::= */
  {  259,    0 }, /* (203) orderby_opt ::= */
  {  259,   -3 }, /* (204) orderby_opt ::= ORDER BY sortlist */
  {  270,   -4 }, /* (205) sortlist ::= sortlist COMMA item sortorder */
  {  270,   -2 }, /* (206) sortlist ::= item sortorder */
  {  272,   -2 }, /* (207) item ::= ids cpxName */
  {  273,   -1 }, /* (208) sortorder ::= ASC */
  {  273,   -1 }, /* (209) sortorder ::= DESC */
  {  273,    0 }, /* (210) sortorder ::= */
  {  257,    0 }, /* (211) groupby_opt ::= */
  {  257,   -3 }, /* (212) groupby_opt ::= GROUP BY grouplist */
  {  274,   -3 }, /* (213) grouplist ::= grouplist COMMA item */
  {  274,   -1 }, /* (214) grouplist ::= item */
  {  258,    0 }, /* (215) having_opt ::= */
  {  258,   -2 }, /* (216) having_opt ::= HAVING expr */
  {  261,    0 }, /* (217) limit_opt ::= */
  {  261,   -2 }, /* (218) limit_opt ::= LIMIT signed */
  {  261,   -4 }, /* (219) limit_opt ::= LIMIT signed OFFSET signed */
  {  261,   -4 }, /* (220) limit_opt ::= LIMIT signed COMMA signed */
  {  260,    0 }, /* (221) slimit_opt ::= */
  {  260,   -2 }, /* (222) slimit_opt ::= SLIMIT signed */
  {  260,   -4 }, /* (223) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  260,   -4 }, /* (224) slimit_opt ::= SLIMIT signed COMMA signed */
  {  251,    0 }, /* (225) where_opt ::= */
  {  251,   -2 }, /* (226) where_opt ::= WHERE expr */
  {  265,   -3 }, /* (227) expr ::= LP expr RP */
  {  265,   -1 }, /* (228) expr ::= ID */
  {  265,   -3 }, /* (229) expr ::= ID DOT ID */
  {  265,   -3 }, /* (230) expr ::= ID DOT STAR */
  {  265,   -1 }, /* (231) expr ::= INTEGER */
  {  265,   -2 }, /* (232) expr ::= MINUS INTEGER */
  {  265,   -2 }, /* (233) expr ::= PLUS INTEGER */
  {  265,   -1 }, /* (234) expr ::= FLOAT */
  {  265,   -2 }, /* (235) expr ::= MINUS FLOAT */
  {  265,   -2 }, /* (236) expr ::= PLUS FLOAT */
  {  265,   -1 }, /* (237) expr ::= STRING */
  {  265,   -1 }, /* (238) expr ::= NOW */
  {  265,   -1 }, /* (239) expr ::= VARIABLE */
  {  265,   -2 }, /* (240) expr ::= PLUS VARIABLE */
  {  265,   -2 }, /* (241) expr ::= MINUS VARIABLE */
  {  265,   -1 }, /* (242) expr ::= BOOL */
  {  265,   -1 }, /* (243) expr ::= NULL */
  {  265,   -4 }, /* (244) expr ::= ID LP exprlist RP */
  {  265,   -4 }, /* (245) expr ::= ID LP STAR RP */
  {  265,   -3 }, /* (246) expr ::= expr IS NULL */
  {  265,   -4 }, /* (247) expr ::= expr IS NOT NULL */
  {  265,   -3 }, /* (248) expr ::= expr LT expr */
  {  265,   -3 }, /* (249) expr ::= expr GT expr */
  {  265,   -3 }, /* (250) expr ::= expr LE expr */
  {  265,   -3 }, /* (251) expr ::= expr GE expr */
  {  265,   -3 }, /* (252) expr ::= expr NE expr */
  {  265,   -3 }, /* (253) expr ::= expr EQ expr */
  {  265,   -5 }, /* (254) expr ::= expr BETWEEN expr AND expr */
  {  265,   -3 }, /* (255) expr ::= expr AND expr */
  {  265,   -3 }, /* (256) expr ::= expr OR expr */
  {  265,   -3 }, /* (257) expr ::= expr PLUS expr */
  {  265,   -3 }, /* (258) expr ::= expr MINUS expr */
  {  265,   -3 }, /* (259) expr ::= expr STAR expr */
  {  265,   -3 }, /* (260) expr ::= expr SLASH expr */
  {  265,   -3 }, /* (261) expr ::= expr REM expr */
  {  265,   -3 }, /* (262) expr ::= expr LIKE expr */
  {  265,   -5 }, /* (263) expr ::= expr IN LP exprlist RP */
  {  205,   -3 }, /* (264) exprlist ::= exprlist COMMA expritem */
  {  205,   -1 }, /* (265) exprlist ::= expritem */
  {  275,   -1 }, /* (266) expritem ::= expr */
  {  275,    0 }, /* (267) expritem ::= */
  {  197,   -3 }, /* (268) cmd ::= RESET QUERY CACHE */
  {  197,   -3 }, /* (269) cmd ::= SYNCDB ids REPLICA */
  {  197,   -7 }, /* (270) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  197,   -7 }, /* (271) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  197,   -7 }, /* (272) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  197,   -7 }, /* (273) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  197,   -7 }, /* (274) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  197,   -8 }, /* (275) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  197,   -9 }, /* (276) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  197,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  197,   -7 }, /* (278) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  197,   -7 }, /* (279) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  197,   -7 }, /* (280) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  197,   -7 }, /* (281) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  197,   -7 }, /* (282) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  197,   -8 }, /* (283) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  197,   -9 }, /* (284) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  197,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  197,   -3 }, /* (286) cmd ::= KILL CONNECTION INTEGER */
  {  197,   -5 }, /* (287) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  197,   -5 }, /* (288) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy214, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy547);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy525);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy214, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy31, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy31, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy547.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy547.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy547.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy547.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy547.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy547.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy547.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy547.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy547.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy547 = yylhsminor.yy547;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy525 = tVariantListAppend(yymsp[-2].minor.yy525, &yymsp[0].minor.yy506, -1);    }
  yymsp[-2].minor.yy525 = yylhsminor.yy525;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy525 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy525 = yymsp[0].minor.yy525; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy214); yymsp[1].minor.yy214.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.keep = yymsp[0].minor.yy525; }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy214 = yymsp[0].minor.yy214; yylhsminor.yy214.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy214 = yylhsminor.yy214;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy214 = yymsp[-1].minor.yy214; yylhsminor.yy214.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy214 = yylhsminor.yy214;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy214); yymsp[1].minor.yy214.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy31, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy501 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy31, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy501;  // negative value of name length
    tSetColumnType(&yylhsminor.yy31, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy31 = yylhsminor.yy31;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy31, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy501 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy501 = yylhsminor.yy501;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy501 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy501 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy158;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy432);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy158 = pCreateTable;
}
  yymsp[0].minor.yy158 = yylhsminor.yy158;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy158->childTableInfo, &yymsp[0].minor.yy432);
  yylhsminor.yy158 = yymsp[-1].minor.yy158;
}
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy158 = tSetCreateTableInfo(yymsp[-1].minor.yy525, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy158, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy158 = yylhsminor.yy158;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy158 = tSetCreateTableInfo(yymsp[-5].minor.yy525, yymsp[-1].minor.yy525, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy158, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy158 = yylhsminor.yy158;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy432 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy525, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy432 = yylhsminor.yy432;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy432 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy525, yymsp[-1].minor.yy525, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy432 = yylhsminor.yy432;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy525, &yymsp[0].minor.yy0); yylhsminor.yy525 = yymsp[-2].minor.yy525;  }
  yymsp[-2].minor.yy525 = yylhsminor.yy525;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy525 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy525, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy158 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy464, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy158, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy158 = yylhsminor.yy158;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy525, &yymsp[0].minor.yy31); yylhsminor.yy525 = yymsp[-2].minor.yy525;  }
  yymsp[-2].minor.yy525 = yylhsminor.yy525;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy525 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy525, &yymsp[0].minor.yy31);}
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy31, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy31);
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 162: /* tagitem ::= MINUS INTEGER */
      case 163: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==165);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 166: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy464 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy525, yymsp[-11].minor.yy412, yymsp[-10].minor.yy370, yymsp[-4].minor.yy525, yymsp[-2].minor.yy525, &yymsp[-9].minor.yy520, &yymsp[-8].minor.yy259, &yymsp[-7].minor.yy144, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy525, &yymsp[0].minor.yy126, &yymsp[-1].minor.yy126, yymsp[-3].minor.yy370);
}
  yymsp[-13].minor.yy464 = yylhsminor.yy464;
        break;
      case 167: /* select ::= LP select RP */
{yymsp[-2].minor.yy464 = yymsp[-1].minor.yy464;}
        break;
      case 168: /* union ::= select */
{ yylhsminor.yy525 = setSubclause(NULL, yymsp[0].minor.yy464); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 169: /* union ::= union UNION ALL select */
{ yylhsminor.yy525 = appendSelectClause(yymsp[-3].minor.yy525, yymsp[0].minor.yy464); }
  yymsp[-3].minor.yy525 = yylhsminor.yy525;
        break;
      case 170: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy525, NULL, TSDB_SQL_SELECT); }
        break;
      case 171: /* select ::= SELECT selcollist */
{
  yylhsminor.yy464 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy525, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy464 = yylhsminor.yy464;
        break;
      case 172: /* sclp ::= selcollist COMMA */
{yylhsminor.yy525 = yymsp[-1].minor.yy525;}
  yymsp[-1].minor.yy525 = yylhsminor.yy525;
        break;
      case 173: /* sclp ::= */
      case 203: /* orderby_opt ::= */ yytestcase(yyruleno==203);
{yymsp[1].minor.yy525 = 0;}
        break;
      case 174: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy525 = tSqlExprListAppend(yymsp[-3].minor.yy525, yymsp[-1].minor.yy370,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy525 = yylhsminor.yy525;
        break;
      case 175: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy525 = tSqlExprListAppend(yymsp[-1].minor.yy525, pNode, 0, 0);
}
  yymsp[-1].minor.yy525 = yylhsminor.yy525;
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
{yymsp[-1].minor.yy412 = yymsp[0].minor.yy412;}
        break;
      case 183: /* sub ::= LP union RP */
{yymsp[-2].minor.yy412 = addSubqueryElem(NULL, yymsp[-1].minor.yy525, NULL);}
        break;
      case 184: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy412 = addSubqueryElem(NULL, yymsp[-2].minor.yy525, &yymsp[0].minor.yy0);}
        break;
      case 185: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy412 = addSubqueryElem(yymsp[-5].minor.yy412, yymsp[-2].minor.yy525, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy412 = yylhsminor.yy412;
        break;
      case 186: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy412 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy412 = yylhsminor.yy412;
        break;
      case 187: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy412 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy412 = yylhsminor.yy412;
        break;
      case 188: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy412 = setTableNameList(yymsp[-3].minor.yy412, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy412 = yylhsminor.yy412;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy412 = setTableNameList(yymsp[-4].minor.yy412, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy412 = yylhsminor.yy412;
        break;
      case 190: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy520.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy520.offset.n = 0;}
        break;
      case 192: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy520.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy520.offset = yymsp[-1].minor.yy0;}
        break;
      case 193: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy520, 0, sizeof(yymsp[1].minor.yy520));}
        break;
      case 194: /* session_option ::= */
{yymsp[1].minor.yy259.col.n = 0; yymsp[1].minor.yy259.gap.n = 0;}
        break;
      case 195: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy259.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy259.gap = yymsp[-1].minor.yy0;
}
        break;
      case 196: /* windowstate_option ::= */
{ yymsp[1].minor.yy144.col.n = 0; yymsp[1].minor.yy144.col.z = NULL;}
        break;
      case 197: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy144.col = yymsp[-1].minor.yy0; }
        break;
      case 198: /* fill_opt ::= */
{ yymsp[1].minor.yy525 = 0;     }
        break;
      case 199: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy525, &A, -1, 0);
    yymsp[-5].minor.yy525 = yymsp[-1].minor.yy525;
}
        break;
      case 200: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy525 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 201: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 202: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 204: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy525 = yymsp[0].minor.yy525;}
        break;
      case 205: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy525 = tVariantListAppend(yymsp[-3].minor.yy525, &yymsp[-1].minor.yy506, yymsp[0].minor.yy52);
}
  yymsp[-3].minor.yy525 = yylhsminor.yy525;
        break;
      case 206: /* sortlist ::= item sortorder */
{
  yylhsminor.yy525 = tVariantListAppend(NULL, &yymsp[-1].minor.yy506, yymsp[0].minor.yy52);
}
  yymsp[-1].minor.yy525 = yylhsminor.yy525;
        break;
      case 207: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 208: /* sortorder ::= ASC */
{ yymsp[0].minor.yy52 = TSDB_ORDER_ASC; }
        break;
      case 209: /* sortorder ::= DESC */
{ yymsp[0].minor.yy52 = TSDB_ORDER_DESC;}
        break;
      case 210: /* sortorder ::= */
{ yymsp[1].minor.yy52 = TSDB_ORDER_ASC; }
        break;
      case 211: /* groupby_opt ::= */
{ yymsp[1].minor.yy525 = 0;}
        break;
      case 212: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy525 = yymsp[0].minor.yy525;}
        break;
      case 213: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy525 = tVariantListAppend(yymsp[-2].minor.yy525, &yymsp[0].minor.yy506, -1);
}
  yymsp[-2].minor.yy525 = yylhsminor.yy525;
        break;
      case 214: /* grouplist ::= item */
{
  yylhsminor.yy525 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1);
}
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 215: /* having_opt ::= */
      case 225: /* where_opt ::= */ yytestcase(yyruleno==225);
      case 267: /* expritem ::= */ yytestcase(yyruleno==267);
{yymsp[1].minor.yy370 = 0;}
        break;
      case 216: /* having_opt ::= HAVING expr */
      case 226: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==226);
{yymsp[-1].minor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 217: /* limit_opt ::= */
      case 221: /* slimit_opt ::= */ yytestcase(yyruleno==221);
{yymsp[1].minor.yy126.limit = -1; yymsp[1].minor.yy126.offset = 0;}
        break;
      case 218: /* limit_opt ::= LIMIT signed */
      case 222: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==222);
{yymsp[-1].minor.yy126.limit = yymsp[0].minor.yy501;  yymsp[-1].minor.yy126.offset = 0;}
        break;
      case 219: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy501;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy501;}
        break;
      case 220: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy501;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy501;}
        break;
      case 223: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy501;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy501;}
        break;
      case 224: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy501;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy501;}
        break;
      case 227: /* expr ::= LP expr RP */
{yylhsminor.yy370 = yymsp[-1].minor.yy370; yylhsminor.yy370->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy370->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 228: /* expr ::= ID */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 229: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 230: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 231: /* expr ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 232: /* expr ::= MINUS INTEGER */
      case 233: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 234: /* expr ::= FLOAT */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 235: /* expr ::= MINUS FLOAT */
      case 236: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 237: /* expr ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 238: /* expr ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 239: /* expr ::= VARIABLE */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 240: /* expr ::= PLUS VARIABLE */
      case 241: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==241);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 242: /* expr ::= BOOL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 243: /* expr ::= NULL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 244: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(yymsp[-1].minor.yy525, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 245: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 246: /* expr ::= expr IS NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 247: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-3].minor.yy370, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 248: /* expr ::= expr LT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 249: /* expr ::= expr GT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 250: /* expr ::= expr LE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 251: /* expr ::= expr GE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 252: /* expr ::= expr NE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 253: /* expr ::= expr EQ expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 254: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy370); yylhsminor.yy370 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy370, yymsp[-2].minor.yy370, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy370, TK_LE), TK_AND);}
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 255: /* expr ::= expr AND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 256: /* expr ::= expr OR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 257: /* expr ::= expr PLUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 258: /* expr ::= expr MINUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 259: /* expr ::= expr STAR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 260: /* expr ::= expr SLASH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 261: /* expr ::= expr REM expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 262: /* expr ::= expr LIKE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 263: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-4].minor.yy370, (tSqlExpr*)yymsp[-1].minor.yy525, TK_IN); }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 264: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy525 = tSqlExprListAppend(yymsp[-2].minor.yy525,yymsp[0].minor.yy370,0, 0);}
  yymsp[-2].minor.yy525 = yylhsminor.yy525;
        break;
      case 265: /* exprlist ::= expritem */
{yylhsminor.yy525 = tSqlExprListAppend(0,yymsp[0].minor.yy370,0, 0);}
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 266: /* expritem ::= expr */
{yylhsminor.yy370 = yymsp[0].minor.yy370;}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 268: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 269: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 276: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 284: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy525, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 287: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 288: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
