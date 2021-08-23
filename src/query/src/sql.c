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
#define YYNOCODE 275
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSessionWindowVal yy39;
  SCreateDbInfo yy42;
  int yy43;
  tSqlExpr* yy46;
  SCreatedTableInfo yy96;
  SArray* yy131;
  TAOS_FIELD yy163;
  SSqlNode* yy256;
  SCreateTableSql* yy272;
  SLimitVal yy284;
  SCreateAcctInfo yy341;
  int64_t yy459;
  tVariant yy516;
  SIntervalVal yy530;
  SWindowStateVal yy538;
  SRelationInfo* yy544;
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
#define YYNSTATE             364
#define YYNRULE              290
#define YYNRULE_WITH_ACTION  290
#define YYNTOKEN             195
#define YY_MAX_SHIFT         363
#define YY_MIN_SHIFTREDUCE   569
#define YY_MAX_SHIFTREDUCE   858
#define YY_ERROR_ACTION      859
#define YY_ACCEPT_ACTION     860
#define YY_NO_ACTION         861
#define YY_MIN_REDUCE        862
#define YY_MAX_REDUCE        1151
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
#define YY_ACTTAB_COUNT (761)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   208,  620,  237,  620,  362,  231, 1017, 1030,  243,  621,
 /*    10 */  1127,  621, 1017,   57,   58,   37,   61,   62,  656, 1039,
 /*    20 */   251,   51,   50,  234,   60,  320,   65,   63,   66,   64,
 /*    30 */  1030,  802,  245,  805,   56,   55, 1017,   23,   54,   53,
 /*    40 */    52,   57,   58,  620,   61,   62,  235,  260,  251,   51,
 /*    50 */    50,  621,   60,  320,   65,   63,   66,   64,  176,  154,
 /*    60 */   233,  205,   56,   55, 1014,  248,   54,   53,   52,  979,
 /*    70 */   967,  968,  969,  970,  971,  972,  973,  974,  975,  976,
 /*    80 */   977,  978,  980,  981,   29,  318,   80, 1036,  570,  571,
 /*    90 */   572,  573,  574,  575,  576,  577,  578,  579,  580,  581,
 /*   100 */   582,  583,  152,   98,  232,   57,   58,   37,   61,   62,
 /*   110 */   247,  796,  251,   51,   50,  350,   60,  320,   65,   63,
 /*   120 */    66,   64,   54,   53,   52,  208,   56,   55,  280,  279,
 /*   130 */    54,   53,   52,   57,   59, 1128,   61,   62,   74, 1005,
 /*   140 */   251,   51,   50,  620,   60,  320,   65,   63,   66,   64,
 /*   150 */   809,  621,  241,  206,   56,   55, 1014, 1003,   54,   53,
 /*   160 */    52,   58,  161,   61,   62,  212,  260,  251,   51,   50,
 /*   170 */   208,   60,  320,   65,   63,   66,   64,  177,   75,  252,
 /*   180 */  1128,   56,   55,  161,  213,   54,   53,   52,   61,   62,
 /*   190 */   860,  363,  251,   51,   50,  265,   60,  320,   65,   63,
 /*   200 */    66,   64,  704, 1011,  269,  268,   56,   55,  358,  948,
 /*   210 */    54,   53,   52,   43,  316,  357,  356,  315,  314,  313,
 /*   220 */   355,  312,  311,  310,  354,  309,  353,  352,   24,  250,
 /*   230 */   811,  340,  339,  800,   95,  803, 1076,  806,  292,  208,
 /*   240 */  1030,  318,   86,  260, 1123,  211,   37,  250,  811, 1128,
 /*   250 */   244,  800,  218,  803, 1015,  806,  273, 1075,  136,  135,
 /*   260 */   217, 1122,  229,  230,  325,   86,  321, 1000, 1001,   34,
 /*   270 */  1004, 1121,   37,   37,  274,    5,   40,  180,   37,   44,
 /*   280 */   229,  230,  179,  104,  109,  100,  108,  728,   38,  123,
 /*   290 */   725,  242,  726,   37,  727, 1014,  801,  161,  804,   37,
 /*   300 */   305,  350,   44,   65,   63,   66,   64,  910,  161,   14,
 /*   310 */    67,   56,   55,   94,  190,   54,   53,   52,  329,  256,
 /*   320 */   257, 1013, 1014,  330,  272,  227,   78, 1014,   67,  920,
 /*   330 */   121,  115,  126,  225,  254,   37,  190,  125,  331,  131,
 /*   340 */   134,  124, 1014,   97,  332,  812,  807,  128, 1014,  199,
 /*   350 */   197,  195,  808,  259,    1,  178,  194,  140,  139,  138,
 /*   360 */   137,   56,   55,  812,  807,   54,   53,   52,  911,  294,
 /*   370 */   808,   91,   37,   37,   43,  190,  357,  356,   37,   93,
 /*   380 */   336,  355,   92,  744, 1014,  354,  322,  353,  352,  361,
 /*   390 */   360,  145,   79,   81,  987,   71,  985,  986,  151,  149,
 /*   400 */   148,  988,  729,  730,   83,  989, 1016,  990,  991,  255,
 /*   410 */   741,  253,   84,  328,  327,  810,  228,  337,  338,    3,
 /*   420 */   191, 1014, 1014,  342,  777,  778, 1002, 1014,  261,  760,
 /*   430 */   258,  768,  335,  334,  769,   33,    9,   72,  714,  297,
 /*   440 */   716,  276,  299,  156,  715,   68,  833,  813,   26,  276,
 /*   450 */   798,  249,   38,   38,   68,  619,   96,   16,   68,   15,
 /*   460 */    25,   25,  114,   18,  113,   17,   77,  748,  300,   25,
 /*   470 */   209,  733,    6,  734,  731,   20,  732,   19,  120,   22,
 /*   480 */   119,   21,  133,  132,  210,  214,  207,  799,  815,  215,
 /*   490 */   216, 1147, 1139,  220,  221,  222,  219,  204,  703, 1086,
 /*   500 */  1085,  239,  270, 1082,  153, 1081,  240,  341, 1038, 1068,
 /*   510 */    47, 1049, 1046, 1047, 1067,  150, 1031,  277, 1012, 1051,
 /*   520 */   281,  155,  160,  288,  172,  173,  236,  283, 1010,  174,
 /*   530 */   170,  168,  163,  175,  925,  302,  303,  304,  307,  308,
 /*   540 */    45,  202,  759, 1028,   41,  164,  319,  919,  285,  326,
 /*   550 */    76, 1146,  162,   73,  295,  111,   49, 1145,  165, 1142,
 /*   560 */   293,  291,  181,  333, 1138,  117, 1137, 1134,  289,  287,
 /*   570 */   182,  945,   42,   39,   46,  203,  907,  127,  905,  129,
 /*   580 */   130,  284,  903,  902,  262,  193,  900,  282,  899,  898,
 /*   590 */   897,  896,  895,  894,  196,  198,  891,  889,  887,  885,
 /*   600 */   200,   48,  882,  201,  878,  306,  351,  275,   82,   87,
 /*   610 */   286, 1069,  344,  122,  343,  345,  346,  347,  226,  246,
 /*   620 */   348,  349,  301,  359,  858,  263,  264,  857,  223,  266,
 /*   630 */   224,  267,  856,  839,  924,  105,  923,  106,  271,  838,
 /*   640 */   276,  296,   85,  278,   10,  901,  736,  141,  142,  185,
 /*   650 */   893,  184,  946,  183,  186,  187,  189,  143,  188,  892,
 /*   660 */   947,    2,   30,  144,  983,    4,  884,  883,  166,  169,
 /*   670 */   167,   88,  171,  761,  993,  157,  159,  770,  158,  238,
 /*   680 */   764,   89,   31,  766,   90,  290,   11,   12,   32,   13,
 /*   690 */    27,  298,   28,   97,   99,  102,   35,  101,  634,   36,
 /*   700 */   103,  669,  667,  666,  665,  663,  662,  661,  658,  317,
 /*   710 */   624,  107,    7,  323,  814,  816,    8,  324,  110,  112,
 /*   720 */    69,   70,  116,  706,  705,   38,  702,  650,  118,  648,
 /*   730 */   640,  646,  642,  644,  638,  636,  672,  671,  670,  668,
 /*   740 */   664,  660,  659,  192,  622,  587,  862,  861,  861,  861,
 /*   750 */   861,  861,  861,  861,  861,  861,  861,  861,  861,  146,
 /*   760 */   147,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   264,    1,  243,    1,  197,  198,  247,  245,  243,    9,
 /*    10 */   274,    9,  247,   13,   14,  197,   16,   17,    5,  197,
 /*    20 */    20,   21,   22,  261,   24,   25,   26,   27,   28,   29,
 /*    30 */   245,    5,  243,    7,   34,   35,  247,  264,   38,   39,
 /*    40 */    40,   13,   14,    1,   16,   17,  261,  197,   20,   21,
 /*    50 */    22,    9,   24,   25,   26,   27,   28,   29,  208,  197,
 /*    60 */   242,  264,   34,   35,  246,  204,   38,   39,   40,  221,
 /*    70 */   222,  223,  224,  225,  226,  227,  228,  229,  230,  231,
 /*    80 */   232,  233,  234,  235,   82,   84,   86,  265,   46,   47,
 /*    90 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   100 */    58,   59,   60,  205,   62,   13,   14,  197,   16,   17,
 /*   110 */   204,   83,   20,   21,   22,   90,   24,   25,   26,   27,
 /*   120 */    28,   29,   38,   39,   40,  264,   34,   35,  266,  267,
 /*   130 */    38,   39,   40,   13,   14,  274,   16,   17,   97,  241,
 /*   140 */    20,   21,   22,    1,   24,   25,   26,   27,   28,   29,
 /*   150 */   124,    9,  242,  264,   34,   35,  246,    0,   38,   39,
 /*   160 */    40,   14,  197,   16,   17,  264,  197,   20,   21,   22,
 /*   170 */   264,   24,   25,   26,   27,   28,   29,  208,  137,  204,
 /*   180 */   274,   34,   35,  197,  264,   38,   39,   40,   16,   17,
 /*   190 */   195,  196,   20,   21,   22,  142,   24,   25,   26,   27,
 /*   200 */    28,   29,    5,  197,  151,  152,   34,   35,  219,  220,
 /*   210 */    38,   39,   40,   98,   99,  100,  101,  102,  103,  104,
 /*   220 */   105,  106,  107,  108,  109,  110,  111,  112,   45,    1,
 /*   230 */     2,   34,   35,    5,  205,    7,  271,    9,  273,  264,
 /*   240 */   245,   84,   82,  197,  264,   62,  197,    1,    2,  274,
 /*   250 */   244,    5,   69,    7,  208,    9,  261,  271,   75,   76,
 /*   260 */    77,  264,   34,   35,   81,   82,   38,  238,  239,  240,
 /*   270 */   241,  264,  197,  197,   83,   63,   64,   65,  197,  119,
 /*   280 */    34,   35,   70,   71,   72,   73,   74,    2,   97,   78,
 /*   290 */     5,  242,    7,  197,    9,  246,    5,  197,    7,  197,
 /*   300 */    88,   90,  119,   26,   27,   28,   29,  203,  197,   82,
 /*   310 */    82,   34,   35,   86,  210,   38,   39,   40,  242,   34,
 /*   320 */    35,  246,  246,  242,  141,  264,  143,  246,   82,  203,
 /*   330 */    63,   64,   65,  150,   69,  197,  210,   70,  242,   72,
 /*   340 */    73,   74,  246,  116,  242,  117,  118,   80,  246,   63,
 /*   350 */    64,   65,  124,   69,  206,  207,   70,   71,   72,   73,
 /*   360 */    74,   34,   35,  117,  118,   38,   39,   40,  203,  269,
 /*   370 */   124,  271,  197,  197,   98,  210,  100,  101,  197,  248,
 /*   380 */   242,  105,  271,   38,  246,  109,   15,  111,  112,   66,
 /*   390 */    67,   68,  205,  262,  221,   97,  223,  224,   63,   64,
 /*   400 */    65,  228,  117,  118,   83,  232,  247,  234,  235,  144,
 /*   410 */    97,  146,   83,  148,  149,  124,  264,  242,  242,  201,
 /*   420 */   202,  246,  246,  242,  132,  133,  239,  246,  144,   83,
 /*   430 */   146,   83,  148,  149,   83,   82,  123,  139,   83,   83,
 /*   440 */    83,  120,   83,   97,   83,   97,   83,   83,   97,  120,
 /*   450 */     1,   61,   97,   97,   97,   83,   97,  145,   97,  147,
 /*   460 */    97,   97,  145,  145,  147,  147,   82,  122,  115,   97,
 /*   470 */   264,    5,   82,    7,    5,  145,    7,  147,  145,  145,
 /*   480 */   147,  147,   78,   79,  264,  264,  264,   38,  117,  264,
 /*   490 */   264,  247,  247,  264,  264,  264,  264,  264,  114,  237,
 /*   500 */   237,  237,  197,  237,  197,  237,  237,  237,  197,  272,
 /*   510 */   263,  197,  197,  197,  272,   61,  245,  245,  245,  197,
 /*   520 */   268,  197,  197,  197,  249,  197,  268,  268,  197,  197,
 /*   530 */   251,  253,  258,  197,  197,  197,  197,  197,  197,  197,
 /*   540 */   197,  197,  124,  260,  197,  257,  197,  197,  268,  197,
 /*   550 */   136,  197,  259,  138,  130,  197,  135,  197,  256,  197,
 /*   560 */   134,  128,  197,  197,  197,  197,  197,  197,  127,  126,
 /*   570 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   580 */   197,  129,  197,  197,  197,  197,  197,  125,  197,  197,
 /*   590 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   600 */   197,  140,  197,  197,  197,   89,  113,  199,  199,  199,
 /*   610 */   199,  199,   52,   96,   95,   92,   94,   56,  199,  199,
 /*   620 */    93,   91,  199,   84,    5,  153,    5,    5,  199,  153,
 /*   630 */   199,    5,    5,  100,  209,  205,  209,  205,  142,   99,
 /*   640 */   120,  115,  121,   97,   82,  199,   83,  200,  200,  212,
 /*   650 */   199,  216,  218,  217,  215,  213,  211,  200,  214,  199,
 /*   660 */   220,  206,   82,  200,  236,  201,  199,  199,  255,  252,
 /*   670 */   254,   97,  250,   83,  236,   82,   97,   83,   82,    1,
 /*   680 */    83,   82,   97,   83,   82,   82,  131,  131,   97,   82,
 /*   690 */    82,  115,   82,  116,   78,   71,   87,   86,    5,   87,
 /*   700 */    86,    9,    5,    5,    5,    5,    5,    5,    5,   15,
 /*   710 */    85,   78,   82,   25,   83,  117,   82,   60,  147,  147,
 /*   720 */    16,   16,  147,    5,    5,   97,   83,    5,  147,    5,
 /*   730 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */     5,    5,    5,   97,   85,   61,    0,  275,  275,  275,
 /*   750 */   275,  275,  275,  275,  275,  275,  275,  275,  275,   21,
 /*   760 */    21,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   770 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   780 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   790 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   800 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   810 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   820 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   830 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   840 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   850 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   860 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   870 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   880 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   890 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   900 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   910 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   920 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   930 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   940 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   950 */   275,  275,  275,  275,  275,  275,
};
#define YY_SHIFT_COUNT    (363)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (746)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   183,  115,  115,  276,  276,    1,  228,  246,  246,    2,
 /*    10 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*    20 */   142,  142,  142,    0,   42,  246,  285,  285,  285,  160,
 /*    30 */   160,  142,  142,  142,  157,  142,  142,  142,  142,  211,
 /*    40 */     1,   25,   25,   13,  761,  761,  761,  246,  246,  246,
 /*    50 */   246,  246,  246,  246,  246,  246,  246,  246,  246,  246,
 /*    60 */   246,  246,  246,  246,  246,  246,  246,  246,  285,  285,
 /*    70 */   285,  197,  197,  197,  197,  197,  197,  197,  142,  142,
 /*    80 */   142,  345,  142,  142,  142,  160,  160,  142,  142,  142,
 /*    90 */   142,  292,  292,  313,  160,  142,  142,  142,  142,  142,
 /*   100 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*   110 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*   120 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*   130 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*   140 */   142,  142,  142,  142,  142,  142,  142,  142,  142,  142,
 /*   150 */   142,  142,  142,  454,  454,  454,  418,  418,  418,  418,
 /*   160 */   454,  454,  414,  415,  424,  421,  426,  433,  441,  443,
 /*   170 */   452,  462,  461,  454,  454,  454,  516,  516,  493,    1,
 /*   180 */     1,  454,  454,  517,  519,  560,  523,  522,  561,  527,
 /*   190 */   530,  493,   13,  454,  539,  539,  454,  539,  454,  539,
 /*   200 */   454,  454,  761,  761,   28,   92,   92,  120,   92,  147,
 /*   210 */   172,  212,  277,  277,  277,  277,  277,  267,  286,  327,
 /*   220 */   327,  327,  327,  265,  284,   53,  227,   84,   84,   26,
 /*   230 */   291,  323,  335,  191,  321,  329,  346,  348,  351,  298,
 /*   240 */    41,  355,  356,  357,  359,  361,  353,  363,  364,  449,
 /*   250 */   390,  371,  372,  312,  317,  318,  466,  469,  330,  333,
 /*   260 */   384,  334,  404,  619,  472,  621,  622,  476,  626,  627,
 /*   270 */   533,  540,  496,  520,  526,  562,  521,  563,  580,  546,
 /*   280 */   574,  590,  593,  594,  596,  597,  579,  599,  600,  602,
 /*   290 */   678,  603,  585,  555,  591,  556,  607,  526,  608,  576,
 /*   300 */   610,  577,  616,  609,  611,  624,  693,  612,  614,  692,
 /*   310 */   697,  698,  699,  700,  701,  702,  703,  625,  694,  633,
 /*   320 */   630,  631,  598,  634,  688,  657,  704,  571,  572,  628,
 /*   330 */   628,  628,  628,  705,  575,  581,  628,  628,  628,  718,
 /*   340 */   719,  643,  628,  722,  724,  725,  726,  727,  728,  729,
 /*   350 */   730,  731,  732,  733,  734,  735,  736,  737,  646,  659,
 /*   360 */   738,  739,  684,  746,
};
#define YY_REDUCE_COUNT (203)
#define YY_REDUCE_MIN   (-264)
#define YY_REDUCE_MAX   (468)
static const short yy_reduce_ofst[] = {
 /*     0 */    -5, -152, -152,  173,  173,   29, -139,  -94,  -25, -138,
 /*    10 */  -182,  -35,  100,  -90,   49,   76,   81,   96,  102,  138,
 /*    20 */   175,  176,  181, -178, -193, -264, -241, -235, -211, -238,
 /*    30 */  -215,  -14,  111,    6, -102, -150,  -31,   46,   75,  104,
 /*    40 */   187,  126,  165,  -11,  131,  148,  218, -227, -203, -111,
 /*    50 */   -99,  -80,  -20,   -3,    7,   61,  152,  206,  220,  221,
 /*    60 */   222,  225,  226,  229,  230,  231,  232,  233,  159,  244,
 /*    70 */   245,  262,  263,  264,  266,  268,  269,  270,  305,  307,
 /*    80 */   311,  247,  314,  315,  316,  271,  272,  322,  324,  325,
 /*    90 */   326,  237,  242,  275,  273,  328,  331,  332,  336,  337,
 /*   100 */   338,  339,  340,  341,  342,  343,  344,  347,  349,  350,
 /*   110 */   352,  354,  358,  360,  362,  365,  366,  367,  368,  369,
 /*   120 */   370,  373,  374,  375,  376,  377,  378,  379,  380,  381,
 /*   130 */   382,  383,  385,  386,  387,  388,  389,  391,  392,  393,
 /*   140 */   394,  395,  396,  397,  398,  399,  400,  401,  402,  403,
 /*   150 */   405,  406,  407,  408,  409,  410,  252,  258,  259,  280,
 /*   160 */   411,  412,  283,  293,  274,  288,  302,  413,  416,  278,
 /*   170 */   417,  279,  422,  419,  420,  423,  425,  427,  428,  430,
 /*   180 */   432,  429,  431,  434,  436,  435,  437,  439,  442,  444,
 /*   190 */   445,  438,  440,  446,  447,  448,  451,  457,  460,  463,
 /*   200 */   467,  468,  455,  464,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   859,  982,  921,  992,  908,  918, 1130, 1130, 1130,  859,
 /*    10 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*    20 */   859,  859,  859, 1040,  879, 1130,  859,  859,  859,  859,
 /*    30 */   859,  859,  859,  859,  918,  859,  859,  859,  859,  928,
 /*    40 */   918,  928,  928,  859, 1035,  966,  984,  859,  859,  859,
 /*    50 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*    60 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*    70 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*    80 */   859, 1042, 1048, 1045,  859,  859,  859, 1050,  859,  859,
 /*    90 */   859, 1072, 1072, 1033,  859,  859,  859,  859,  859,  859,
 /*   100 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   110 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   120 */   859,  859,  859,  859,  859,  859,  859,  906,  859,  904,
 /*   130 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   140 */   859,  859,  859,  859,  859,  890,  859,  859,  859,  859,
 /*   150 */   859,  859,  877,  881,  881,  881,  859,  859,  859,  859,
 /*   160 */   881,  881, 1079, 1083, 1065, 1077, 1073, 1060, 1058, 1056,
 /*   170 */  1064, 1055, 1087,  881,  881,  881,  926,  926,  922,  918,
 /*   180 */   918,  881,  881,  944,  942,  940,  932,  938,  934,  936,
 /*   190 */   930,  909,  859,  881,  916,  916,  881,  916,  881,  916,
 /*   200 */   881,  881,  966,  984,  859, 1088, 1078,  859, 1129, 1118,
 /*   210 */  1117,  859, 1125, 1124, 1116, 1115, 1114,  859,  859, 1110,
 /*   220 */  1113, 1112, 1111,  859,  859,  859,  859, 1120, 1119,  859,
 /*   230 */   859,  859,  859,  859,  859,  859,  859,  859,  859, 1084,
 /*   240 */  1080,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   250 */  1090,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   260 */   994,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   270 */   859,  859,  859, 1032,  859,  859,  859,  859,  859, 1044,
 /*   280 */  1043,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   290 */   859,  859, 1074,  859, 1066,  859,  859, 1006,  859,  859,
 /*   300 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   310 */   859,  859,  859,  859,  859,  859,  859,  859,  859,  859,
 /*   320 */   859,  859,  859,  859,  859,  859,  859,  859,  859, 1148,
 /*   330 */  1143, 1144, 1141,  859,  859,  859, 1140, 1135, 1136,  859,
 /*   340 */   859,  859, 1133,  859,  859,  859,  859,  859,  859,  859,
 /*   350 */   859,  859,  859,  859,  859,  859,  859,  859,  950,  859,
 /*   360 */   888,  886,  859,  859,
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
  /*  175 */ "KEY",
  /*  176 */ "OF",
  /*  177 */ "RAISE",
  /*  178 */ "REPLACE",
  /*  179 */ "RESTRICT",
  /*  180 */ "ROW",
  /*  181 */ "STATEMENT",
  /*  182 */ "TRIGGER",
  /*  183 */ "VIEW",
  /*  184 */ "IPTOKEN",
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
  /*  195 */ "program",
  /*  196 */ "cmd",
  /*  197 */ "ids",
  /*  198 */ "dbPrefix",
  /*  199 */ "cpxName",
  /*  200 */ "ifexists",
  /*  201 */ "alter_db_optr",
  /*  202 */ "alter_topic_optr",
  /*  203 */ "acct_optr",
  /*  204 */ "exprlist",
  /*  205 */ "ifnotexists",
  /*  206 */ "db_optr",
  /*  207 */ "topic_optr",
  /*  208 */ "typename",
  /*  209 */ "bufsize",
  /*  210 */ "pps",
  /*  211 */ "tseries",
  /*  212 */ "dbs",
  /*  213 */ "streams",
  /*  214 */ "storage",
  /*  215 */ "qtime",
  /*  216 */ "users",
  /*  217 */ "conns",
  /*  218 */ "state",
  /*  219 */ "intitemlist",
  /*  220 */ "intitem",
  /*  221 */ "keep",
  /*  222 */ "cache",
  /*  223 */ "replica",
  /*  224 */ "quorum",
  /*  225 */ "days",
  /*  226 */ "minrows",
  /*  227 */ "maxrows",
  /*  228 */ "blocks",
  /*  229 */ "ctime",
  /*  230 */ "wal",
  /*  231 */ "fsync",
  /*  232 */ "comp",
  /*  233 */ "prec",
  /*  234 */ "update",
  /*  235 */ "cachelast",
  /*  236 */ "partitions",
  /*  237 */ "signed",
  /*  238 */ "create_table_args",
  /*  239 */ "create_stable_args",
  /*  240 */ "create_table_list",
  /*  241 */ "create_from_stable",
  /*  242 */ "columnlist",
  /*  243 */ "tagitemlist",
  /*  244 */ "tagNamelist",
  /*  245 */ "select",
  /*  246 */ "column",
  /*  247 */ "tagitem",
  /*  248 */ "selcollist",
  /*  249 */ "from",
  /*  250 */ "where_opt",
  /*  251 */ "interval_opt",
  /*  252 */ "sliding_opt",
  /*  253 */ "session_option",
  /*  254 */ "windowstate_option",
  /*  255 */ "fill_opt",
  /*  256 */ "groupby_opt",
  /*  257 */ "having_opt",
  /*  258 */ "orderby_opt",
  /*  259 */ "slimit_opt",
  /*  260 */ "limit_opt",
  /*  261 */ "union",
  /*  262 */ "sclp",
  /*  263 */ "distinct",
  /*  264 */ "expr",
  /*  265 */ "as",
  /*  266 */ "tablelist",
  /*  267 */ "sub",
  /*  268 */ "tmvar",
  /*  269 */ "sortlist",
  /*  270 */ "sortitem",
  /*  271 */ "item",
  /*  272 */ "sortorder",
  /*  273 */ "grouplist",
  /*  274 */ "expritem",
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
 /* 166 */ "select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
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
 /* 263 */ "expr ::= expr MATCH expr",
 /* 264 */ "expr ::= expr IN LP exprlist RP",
 /* 265 */ "exprlist ::= exprlist COMMA expritem",
 /* 266 */ "exprlist ::= expritem",
 /* 267 */ "expritem ::= expr",
 /* 268 */ "expritem ::=",
 /* 269 */ "cmd ::= RESET QUERY CACHE",
 /* 270 */ "cmd ::= SYNCDB ids REPLICA",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 287 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 288 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 289 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 204: /* exprlist */
    case 248: /* selcollist */
    case 262: /* sclp */
{
tSqlExprListDestroy((yypminor->yy131));
}
      break;
    case 219: /* intitemlist */
    case 221: /* keep */
    case 242: /* columnlist */
    case 243: /* tagitemlist */
    case 244: /* tagNamelist */
    case 255: /* fill_opt */
    case 256: /* groupby_opt */
    case 258: /* orderby_opt */
    case 269: /* sortlist */
    case 273: /* grouplist */
{
taosArrayDestroy((yypminor->yy131));
}
      break;
    case 240: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy272));
}
      break;
    case 245: /* select */
{
destroySqlNode((yypminor->yy256));
}
      break;
    case 249: /* from */
    case 266: /* tablelist */
    case 267: /* sub */
{
destroyRelationInfo((yypminor->yy544));
}
      break;
    case 250: /* where_opt */
    case 257: /* having_opt */
    case 264: /* expr */
    case 274: /* expritem */
{
tSqlExprDestroy((yypminor->yy46));
}
      break;
    case 261: /* union */
{
destroyAllSqlNode((yypminor->yy131));
}
      break;
    case 270: /* sortitem */
{
tVariantDestroy(&(yypminor->yy516));
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
   195,  /* (0) program ::= cmd */
   196,  /* (1) cmd ::= SHOW DATABASES */
   196,  /* (2) cmd ::= SHOW TOPICS */
   196,  /* (3) cmd ::= SHOW FUNCTIONS */
   196,  /* (4) cmd ::= SHOW MNODES */
   196,  /* (5) cmd ::= SHOW DNODES */
   196,  /* (6) cmd ::= SHOW ACCOUNTS */
   196,  /* (7) cmd ::= SHOW USERS */
   196,  /* (8) cmd ::= SHOW MODULES */
   196,  /* (9) cmd ::= SHOW QUERIES */
   196,  /* (10) cmd ::= SHOW CONNECTIONS */
   196,  /* (11) cmd ::= SHOW STREAMS */
   196,  /* (12) cmd ::= SHOW VARIABLES */
   196,  /* (13) cmd ::= SHOW SCORES */
   196,  /* (14) cmd ::= SHOW GRANTS */
   196,  /* (15) cmd ::= SHOW VNODES */
   196,  /* (16) cmd ::= SHOW VNODES ids */
   198,  /* (17) dbPrefix ::= */
   198,  /* (18) dbPrefix ::= ids DOT */
   199,  /* (19) cpxName ::= */
   199,  /* (20) cpxName ::= DOT ids */
   196,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   196,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   196,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   196,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   196,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   196,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   196,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   196,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   196,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   196,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   196,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   196,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   196,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   196,  /* (34) cmd ::= DROP FUNCTION ids */
   196,  /* (35) cmd ::= DROP DNODE ids */
   196,  /* (36) cmd ::= DROP USER ids */
   196,  /* (37) cmd ::= DROP ACCOUNT ids */
   196,  /* (38) cmd ::= USE ids */
   196,  /* (39) cmd ::= DESCRIBE ids cpxName */
   196,  /* (40) cmd ::= ALTER USER ids PASS ids */
   196,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   196,  /* (42) cmd ::= ALTER DNODE ids ids */
   196,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   196,  /* (44) cmd ::= ALTER LOCAL ids */
   196,  /* (45) cmd ::= ALTER LOCAL ids ids */
   196,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   196,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   196,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   196,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   196,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   197,  /* (51) ids ::= ID */
   197,  /* (52) ids ::= STRING */
   200,  /* (53) ifexists ::= IF EXISTS */
   200,  /* (54) ifexists ::= */
   205,  /* (55) ifnotexists ::= IF NOT EXISTS */
   205,  /* (56) ifnotexists ::= */
   196,  /* (57) cmd ::= CREATE DNODE ids */
   196,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   196,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   196,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   196,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   196,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   196,  /* (63) cmd ::= CREATE USER ids PASS ids */
   209,  /* (64) bufsize ::= */
   209,  /* (65) bufsize ::= BUFSIZE INTEGER */
   210,  /* (66) pps ::= */
   210,  /* (67) pps ::= PPS INTEGER */
   211,  /* (68) tseries ::= */
   211,  /* (69) tseries ::= TSERIES INTEGER */
   212,  /* (70) dbs ::= */
   212,  /* (71) dbs ::= DBS INTEGER */
   213,  /* (72) streams ::= */
   213,  /* (73) streams ::= STREAMS INTEGER */
   214,  /* (74) storage ::= */
   214,  /* (75) storage ::= STORAGE INTEGER */
   215,  /* (76) qtime ::= */
   215,  /* (77) qtime ::= QTIME INTEGER */
   216,  /* (78) users ::= */
   216,  /* (79) users ::= USERS INTEGER */
   217,  /* (80) conns ::= */
   217,  /* (81) conns ::= CONNS INTEGER */
   218,  /* (82) state ::= */
   218,  /* (83) state ::= STATE ids */
   203,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   219,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   219,  /* (86) intitemlist ::= intitem */
   220,  /* (87) intitem ::= INTEGER */
   221,  /* (88) keep ::= KEEP intitemlist */
   222,  /* (89) cache ::= CACHE INTEGER */
   223,  /* (90) replica ::= REPLICA INTEGER */
   224,  /* (91) quorum ::= QUORUM INTEGER */
   225,  /* (92) days ::= DAYS INTEGER */
   226,  /* (93) minrows ::= MINROWS INTEGER */
   227,  /* (94) maxrows ::= MAXROWS INTEGER */
   228,  /* (95) blocks ::= BLOCKS INTEGER */
   229,  /* (96) ctime ::= CTIME INTEGER */
   230,  /* (97) wal ::= WAL INTEGER */
   231,  /* (98) fsync ::= FSYNC INTEGER */
   232,  /* (99) comp ::= COMP INTEGER */
   233,  /* (100) prec ::= PRECISION STRING */
   234,  /* (101) update ::= UPDATE INTEGER */
   235,  /* (102) cachelast ::= CACHELAST INTEGER */
   236,  /* (103) partitions ::= PARTITIONS INTEGER */
   206,  /* (104) db_optr ::= */
   206,  /* (105) db_optr ::= db_optr cache */
   206,  /* (106) db_optr ::= db_optr replica */
   206,  /* (107) db_optr ::= db_optr quorum */
   206,  /* (108) db_optr ::= db_optr days */
   206,  /* (109) db_optr ::= db_optr minrows */
   206,  /* (110) db_optr ::= db_optr maxrows */
   206,  /* (111) db_optr ::= db_optr blocks */
   206,  /* (112) db_optr ::= db_optr ctime */
   206,  /* (113) db_optr ::= db_optr wal */
   206,  /* (114) db_optr ::= db_optr fsync */
   206,  /* (115) db_optr ::= db_optr comp */
   206,  /* (116) db_optr ::= db_optr prec */
   206,  /* (117) db_optr ::= db_optr keep */
   206,  /* (118) db_optr ::= db_optr update */
   206,  /* (119) db_optr ::= db_optr cachelast */
   207,  /* (120) topic_optr ::= db_optr */
   207,  /* (121) topic_optr ::= topic_optr partitions */
   201,  /* (122) alter_db_optr ::= */
   201,  /* (123) alter_db_optr ::= alter_db_optr replica */
   201,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   201,  /* (125) alter_db_optr ::= alter_db_optr keep */
   201,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   201,  /* (127) alter_db_optr ::= alter_db_optr comp */
   201,  /* (128) alter_db_optr ::= alter_db_optr update */
   201,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   202,  /* (130) alter_topic_optr ::= alter_db_optr */
   202,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   208,  /* (132) typename ::= ids */
   208,  /* (133) typename ::= ids LP signed RP */
   208,  /* (134) typename ::= ids UNSIGNED */
   237,  /* (135) signed ::= INTEGER */
   237,  /* (136) signed ::= PLUS INTEGER */
   237,  /* (137) signed ::= MINUS INTEGER */
   196,  /* (138) cmd ::= CREATE TABLE create_table_args */
   196,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   196,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   196,  /* (141) cmd ::= CREATE TABLE create_table_list */
   240,  /* (142) create_table_list ::= create_from_stable */
   240,  /* (143) create_table_list ::= create_table_list create_from_stable */
   238,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   239,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   241,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   241,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   244,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   244,  /* (149) tagNamelist ::= ids */
   238,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   242,  /* (151) columnlist ::= columnlist COMMA column */
   242,  /* (152) columnlist ::= column */
   246,  /* (153) column ::= ids typename */
   243,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   243,  /* (155) tagitemlist ::= tagitem */
   247,  /* (156) tagitem ::= INTEGER */
   247,  /* (157) tagitem ::= FLOAT */
   247,  /* (158) tagitem ::= STRING */
   247,  /* (159) tagitem ::= BOOL */
   247,  /* (160) tagitem ::= NULL */
   247,  /* (161) tagitem ::= NOW */
   247,  /* (162) tagitem ::= MINUS INTEGER */
   247,  /* (163) tagitem ::= MINUS FLOAT */
   247,  /* (164) tagitem ::= PLUS INTEGER */
   247,  /* (165) tagitem ::= PLUS FLOAT */
   245,  /* (166) select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   245,  /* (167) select ::= LP select RP */
   261,  /* (168) union ::= select */
   261,  /* (169) union ::= union UNION ALL select */
   196,  /* (170) cmd ::= union */
   245,  /* (171) select ::= SELECT selcollist */
   262,  /* (172) sclp ::= selcollist COMMA */
   262,  /* (173) sclp ::= */
   248,  /* (174) selcollist ::= sclp distinct expr as */
   248,  /* (175) selcollist ::= sclp STAR */
   265,  /* (176) as ::= AS ids */
   265,  /* (177) as ::= ids */
   265,  /* (178) as ::= */
   263,  /* (179) distinct ::= DISTINCT */
   263,  /* (180) distinct ::= */
   249,  /* (181) from ::= FROM tablelist */
   249,  /* (182) from ::= FROM sub */
   267,  /* (183) sub ::= LP union RP */
   267,  /* (184) sub ::= LP union RP ids */
   267,  /* (185) sub ::= sub COMMA LP union RP ids */
   266,  /* (186) tablelist ::= ids cpxName */
   266,  /* (187) tablelist ::= ids cpxName ids */
   266,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   266,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   268,  /* (190) tmvar ::= VARIABLE */
   251,  /* (191) interval_opt ::= INTERVAL LP tmvar RP */
   251,  /* (192) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   251,  /* (193) interval_opt ::= */
   253,  /* (194) session_option ::= */
   253,  /* (195) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   254,  /* (196) windowstate_option ::= */
   254,  /* (197) windowstate_option ::= STATE_WINDOW LP ids RP */
   255,  /* (198) fill_opt ::= */
   255,  /* (199) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   255,  /* (200) fill_opt ::= FILL LP ID RP */
   252,  /* (201) sliding_opt ::= SLIDING LP tmvar RP */
   252,  /* (202) sliding_opt ::= */
   258,  /* (203) orderby_opt ::= */
   258,  /* (204) orderby_opt ::= ORDER BY sortlist */
   269,  /* (205) sortlist ::= sortlist COMMA item sortorder */
   269,  /* (206) sortlist ::= item sortorder */
   271,  /* (207) item ::= ids cpxName */
   272,  /* (208) sortorder ::= ASC */
   272,  /* (209) sortorder ::= DESC */
   272,  /* (210) sortorder ::= */
   256,  /* (211) groupby_opt ::= */
   256,  /* (212) groupby_opt ::= GROUP BY grouplist */
   273,  /* (213) grouplist ::= grouplist COMMA item */
   273,  /* (214) grouplist ::= item */
   257,  /* (215) having_opt ::= */
   257,  /* (216) having_opt ::= HAVING expr */
   260,  /* (217) limit_opt ::= */
   260,  /* (218) limit_opt ::= LIMIT signed */
   260,  /* (219) limit_opt ::= LIMIT signed OFFSET signed */
   260,  /* (220) limit_opt ::= LIMIT signed COMMA signed */
   259,  /* (221) slimit_opt ::= */
   259,  /* (222) slimit_opt ::= SLIMIT signed */
   259,  /* (223) slimit_opt ::= SLIMIT signed SOFFSET signed */
   259,  /* (224) slimit_opt ::= SLIMIT signed COMMA signed */
   250,  /* (225) where_opt ::= */
   250,  /* (226) where_opt ::= WHERE expr */
   264,  /* (227) expr ::= LP expr RP */
   264,  /* (228) expr ::= ID */
   264,  /* (229) expr ::= ID DOT ID */
   264,  /* (230) expr ::= ID DOT STAR */
   264,  /* (231) expr ::= INTEGER */
   264,  /* (232) expr ::= MINUS INTEGER */
   264,  /* (233) expr ::= PLUS INTEGER */
   264,  /* (234) expr ::= FLOAT */
   264,  /* (235) expr ::= MINUS FLOAT */
   264,  /* (236) expr ::= PLUS FLOAT */
   264,  /* (237) expr ::= STRING */
   264,  /* (238) expr ::= NOW */
   264,  /* (239) expr ::= VARIABLE */
   264,  /* (240) expr ::= PLUS VARIABLE */
   264,  /* (241) expr ::= MINUS VARIABLE */
   264,  /* (242) expr ::= BOOL */
   264,  /* (243) expr ::= NULL */
   264,  /* (244) expr ::= ID LP exprlist RP */
   264,  /* (245) expr ::= ID LP STAR RP */
   264,  /* (246) expr ::= expr IS NULL */
   264,  /* (247) expr ::= expr IS NOT NULL */
   264,  /* (248) expr ::= expr LT expr */
   264,  /* (249) expr ::= expr GT expr */
   264,  /* (250) expr ::= expr LE expr */
   264,  /* (251) expr ::= expr GE expr */
   264,  /* (252) expr ::= expr NE expr */
   264,  /* (253) expr ::= expr EQ expr */
   264,  /* (254) expr ::= expr BETWEEN expr AND expr */
   264,  /* (255) expr ::= expr AND expr */
   264,  /* (256) expr ::= expr OR expr */
   264,  /* (257) expr ::= expr PLUS expr */
   264,  /* (258) expr ::= expr MINUS expr */
   264,  /* (259) expr ::= expr STAR expr */
   264,  /* (260) expr ::= expr SLASH expr */
   264,  /* (261) expr ::= expr REM expr */
   264,  /* (262) expr ::= expr LIKE expr */
   264,  /* (263) expr ::= expr MATCH expr */
   264,  /* (264) expr ::= expr IN LP exprlist RP */
   204,  /* (265) exprlist ::= exprlist COMMA expritem */
   204,  /* (266) exprlist ::= expritem */
   274,  /* (267) expritem ::= expr */
   274,  /* (268) expritem ::= */
   196,  /* (269) cmd ::= RESET QUERY CACHE */
   196,  /* (270) cmd ::= SYNCDB ids REPLICA */
   196,  /* (271) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   196,  /* (272) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   196,  /* (273) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   196,  /* (274) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   196,  /* (275) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   196,  /* (276) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   196,  /* (277) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   196,  /* (278) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   196,  /* (279) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   196,  /* (280) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   196,  /* (281) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   196,  /* (282) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   196,  /* (283) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   196,  /* (284) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   196,  /* (285) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   196,  /* (286) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   196,  /* (287) cmd ::= KILL CONNECTION INTEGER */
   196,  /* (288) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   196,  /* (289) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (40) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (42) cmd ::= ALTER DNODE ids ids */
   -5,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (44) cmd ::= ALTER LOCAL ids */
   -4,  /* (45) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -6,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   -1,  /* (51) ids ::= ID */
   -1,  /* (52) ids ::= STRING */
   -2,  /* (53) ifexists ::= IF EXISTS */
    0,  /* (54) ifexists ::= */
   -3,  /* (55) ifnotexists ::= IF NOT EXISTS */
    0,  /* (56) ifnotexists ::= */
   -3,  /* (57) cmd ::= CREATE DNODE ids */
   -6,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -8,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (63) cmd ::= CREATE USER ids PASS ids */
    0,  /* (64) bufsize ::= */
   -2,  /* (65) bufsize ::= BUFSIZE INTEGER */
    0,  /* (66) pps ::= */
   -2,  /* (67) pps ::= PPS INTEGER */
    0,  /* (68) tseries ::= */
   -2,  /* (69) tseries ::= TSERIES INTEGER */
    0,  /* (70) dbs ::= */
   -2,  /* (71) dbs ::= DBS INTEGER */
    0,  /* (72) streams ::= */
   -2,  /* (73) streams ::= STREAMS INTEGER */
    0,  /* (74) storage ::= */
   -2,  /* (75) storage ::= STORAGE INTEGER */
    0,  /* (76) qtime ::= */
   -2,  /* (77) qtime ::= QTIME INTEGER */
    0,  /* (78) users ::= */
   -2,  /* (79) users ::= USERS INTEGER */
    0,  /* (80) conns ::= */
   -2,  /* (81) conns ::= CONNS INTEGER */
    0,  /* (82) state ::= */
   -2,  /* (83) state ::= STATE ids */
   -9,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (86) intitemlist ::= intitem */
   -1,  /* (87) intitem ::= INTEGER */
   -2,  /* (88) keep ::= KEEP intitemlist */
   -2,  /* (89) cache ::= CACHE INTEGER */
   -2,  /* (90) replica ::= REPLICA INTEGER */
   -2,  /* (91) quorum ::= QUORUM INTEGER */
   -2,  /* (92) days ::= DAYS INTEGER */
   -2,  /* (93) minrows ::= MINROWS INTEGER */
   -2,  /* (94) maxrows ::= MAXROWS INTEGER */
   -2,  /* (95) blocks ::= BLOCKS INTEGER */
   -2,  /* (96) ctime ::= CTIME INTEGER */
   -2,  /* (97) wal ::= WAL INTEGER */
   -2,  /* (98) fsync ::= FSYNC INTEGER */
   -2,  /* (99) comp ::= COMP INTEGER */
   -2,  /* (100) prec ::= PRECISION STRING */
   -2,  /* (101) update ::= UPDATE INTEGER */
   -2,  /* (102) cachelast ::= CACHELAST INTEGER */
   -2,  /* (103) partitions ::= PARTITIONS INTEGER */
    0,  /* (104) db_optr ::= */
   -2,  /* (105) db_optr ::= db_optr cache */
   -2,  /* (106) db_optr ::= db_optr replica */
   -2,  /* (107) db_optr ::= db_optr quorum */
   -2,  /* (108) db_optr ::= db_optr days */
   -2,  /* (109) db_optr ::= db_optr minrows */
   -2,  /* (110) db_optr ::= db_optr maxrows */
   -2,  /* (111) db_optr ::= db_optr blocks */
   -2,  /* (112) db_optr ::= db_optr ctime */
   -2,  /* (113) db_optr ::= db_optr wal */
   -2,  /* (114) db_optr ::= db_optr fsync */
   -2,  /* (115) db_optr ::= db_optr comp */
   -2,  /* (116) db_optr ::= db_optr prec */
   -2,  /* (117) db_optr ::= db_optr keep */
   -2,  /* (118) db_optr ::= db_optr update */
   -2,  /* (119) db_optr ::= db_optr cachelast */
   -1,  /* (120) topic_optr ::= db_optr */
   -2,  /* (121) topic_optr ::= topic_optr partitions */
    0,  /* (122) alter_db_optr ::= */
   -2,  /* (123) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (125) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (127) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (128) alter_db_optr ::= alter_db_optr update */
   -2,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (130) alter_topic_optr ::= alter_db_optr */
   -2,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (132) typename ::= ids */
   -4,  /* (133) typename ::= ids LP signed RP */
   -2,  /* (134) typename ::= ids UNSIGNED */
   -1,  /* (135) signed ::= INTEGER */
   -2,  /* (136) signed ::= PLUS INTEGER */
   -2,  /* (137) signed ::= MINUS INTEGER */
   -3,  /* (138) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (141) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (142) create_table_list ::= create_from_stable */
   -2,  /* (143) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (149) tagNamelist ::= ids */
   -5,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (151) columnlist ::= columnlist COMMA column */
   -1,  /* (152) columnlist ::= column */
   -2,  /* (153) column ::= ids typename */
   -3,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (155) tagitemlist ::= tagitem */
   -1,  /* (156) tagitem ::= INTEGER */
   -1,  /* (157) tagitem ::= FLOAT */
   -1,  /* (158) tagitem ::= STRING */
   -1,  /* (159) tagitem ::= BOOL */
   -1,  /* (160) tagitem ::= NULL */
   -1,  /* (161) tagitem ::= NOW */
   -2,  /* (162) tagitem ::= MINUS INTEGER */
   -2,  /* (163) tagitem ::= MINUS FLOAT */
   -2,  /* (164) tagitem ::= PLUS INTEGER */
   -2,  /* (165) tagitem ::= PLUS FLOAT */
  -14,  /* (166) select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (167) select ::= LP select RP */
   -1,  /* (168) union ::= select */
   -4,  /* (169) union ::= union UNION ALL select */
   -1,  /* (170) cmd ::= union */
   -2,  /* (171) select ::= SELECT selcollist */
   -2,  /* (172) sclp ::= selcollist COMMA */
    0,  /* (173) sclp ::= */
   -4,  /* (174) selcollist ::= sclp distinct expr as */
   -2,  /* (175) selcollist ::= sclp STAR */
   -2,  /* (176) as ::= AS ids */
   -1,  /* (177) as ::= ids */
    0,  /* (178) as ::= */
   -1,  /* (179) distinct ::= DISTINCT */
    0,  /* (180) distinct ::= */
   -2,  /* (181) from ::= FROM tablelist */
   -2,  /* (182) from ::= FROM sub */
   -3,  /* (183) sub ::= LP union RP */
   -4,  /* (184) sub ::= LP union RP ids */
   -6,  /* (185) sub ::= sub COMMA LP union RP ids */
   -2,  /* (186) tablelist ::= ids cpxName */
   -3,  /* (187) tablelist ::= ids cpxName ids */
   -4,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (190) tmvar ::= VARIABLE */
   -4,  /* (191) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (192) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (193) interval_opt ::= */
    0,  /* (194) session_option ::= */
   -7,  /* (195) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (196) windowstate_option ::= */
   -4,  /* (197) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (198) fill_opt ::= */
   -6,  /* (199) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (200) fill_opt ::= FILL LP ID RP */
   -4,  /* (201) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (202) sliding_opt ::= */
    0,  /* (203) orderby_opt ::= */
   -3,  /* (204) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (205) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (206) sortlist ::= item sortorder */
   -2,  /* (207) item ::= ids cpxName */
   -1,  /* (208) sortorder ::= ASC */
   -1,  /* (209) sortorder ::= DESC */
    0,  /* (210) sortorder ::= */
    0,  /* (211) groupby_opt ::= */
   -3,  /* (212) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (213) grouplist ::= grouplist COMMA item */
   -1,  /* (214) grouplist ::= item */
    0,  /* (215) having_opt ::= */
   -2,  /* (216) having_opt ::= HAVING expr */
    0,  /* (217) limit_opt ::= */
   -2,  /* (218) limit_opt ::= LIMIT signed */
   -4,  /* (219) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (220) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (221) slimit_opt ::= */
   -2,  /* (222) slimit_opt ::= SLIMIT signed */
   -4,  /* (223) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (224) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (225) where_opt ::= */
   -2,  /* (226) where_opt ::= WHERE expr */
   -3,  /* (227) expr ::= LP expr RP */
   -1,  /* (228) expr ::= ID */
   -3,  /* (229) expr ::= ID DOT ID */
   -3,  /* (230) expr ::= ID DOT STAR */
   -1,  /* (231) expr ::= INTEGER */
   -2,  /* (232) expr ::= MINUS INTEGER */
   -2,  /* (233) expr ::= PLUS INTEGER */
   -1,  /* (234) expr ::= FLOAT */
   -2,  /* (235) expr ::= MINUS FLOAT */
   -2,  /* (236) expr ::= PLUS FLOAT */
   -1,  /* (237) expr ::= STRING */
   -1,  /* (238) expr ::= NOW */
   -1,  /* (239) expr ::= VARIABLE */
   -2,  /* (240) expr ::= PLUS VARIABLE */
   -2,  /* (241) expr ::= MINUS VARIABLE */
   -1,  /* (242) expr ::= BOOL */
   -1,  /* (243) expr ::= NULL */
   -4,  /* (244) expr ::= ID LP exprlist RP */
   -4,  /* (245) expr ::= ID LP STAR RP */
   -3,  /* (246) expr ::= expr IS NULL */
   -4,  /* (247) expr ::= expr IS NOT NULL */
   -3,  /* (248) expr ::= expr LT expr */
   -3,  /* (249) expr ::= expr GT expr */
   -3,  /* (250) expr ::= expr LE expr */
   -3,  /* (251) expr ::= expr GE expr */
   -3,  /* (252) expr ::= expr NE expr */
   -3,  /* (253) expr ::= expr EQ expr */
   -5,  /* (254) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (255) expr ::= expr AND expr */
   -3,  /* (256) expr ::= expr OR expr */
   -3,  /* (257) expr ::= expr PLUS expr */
   -3,  /* (258) expr ::= expr MINUS expr */
   -3,  /* (259) expr ::= expr STAR expr */
   -3,  /* (260) expr ::= expr SLASH expr */
   -3,  /* (261) expr ::= expr REM expr */
   -3,  /* (262) expr ::= expr LIKE expr */
   -3,  /* (263) expr ::= expr MATCH expr */
   -5,  /* (264) expr ::= expr IN LP exprlist RP */
   -3,  /* (265) exprlist ::= exprlist COMMA expritem */
   -1,  /* (266) exprlist ::= expritem */
   -1,  /* (267) expritem ::= expr */
    0,  /* (268) expritem ::= */
   -3,  /* (269) cmd ::= RESET QUERY CACHE */
   -3,  /* (270) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (271) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (272) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (273) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (274) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (275) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (276) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (277) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (278) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (279) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (280) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (281) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (282) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (283) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (284) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (285) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (286) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (287) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (288) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (289) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy341);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy131);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy163, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy163, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy341.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy341.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy341.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy341.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy341.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy341.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy341.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy341 = yylhsminor.yy341;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);    }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1); }
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy131 = yymsp[0].minor.yy131; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.keep = yymsp[0].minor.yy131; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy42 = yymsp[0].minor.yy42; yylhsminor.yy42.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy163, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy163 = yylhsminor.yy163;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy459 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy459;  // negative value of name length
    tSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy163 = yylhsminor.yy163;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy163, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy163 = yylhsminor.yy163;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy459 = yylhsminor.yy459;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy459 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy272;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy272 = pCreateTable;
}
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy272->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy272 = yymsp[-1].minor.yy272;
}
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-1].minor.yy131, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy272 = yylhsminor.yy272;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy272 = yylhsminor.yy272;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy131, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy0); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy131 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy272 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy256, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy272 = yylhsminor.yy272;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy163); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy131 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy163);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy163, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy163);
}
  yymsp[-1].minor.yy163 = yylhsminor.yy163;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 162: /* tagitem ::= MINUS INTEGER */
      case 163: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==165);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 166: /* select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy131, yymsp[-11].minor.yy544, yymsp[-10].minor.yy46, yymsp[-4].minor.yy131, yymsp[-2].minor.yy131, &yymsp[-9].minor.yy530, &yymsp[-7].minor.yy39, &yymsp[-6].minor.yy538, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284, yymsp[-3].minor.yy46);
}
  yymsp[-13].minor.yy256 = yylhsminor.yy256;
        break;
      case 167: /* select ::= LP select RP */
{yymsp[-2].minor.yy256 = yymsp[-1].minor.yy256;}
        break;
      case 168: /* union ::= select */
{ yylhsminor.yy131 = setSubclause(NULL, yymsp[0].minor.yy256); }
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 169: /* union ::= union UNION ALL select */
{ yylhsminor.yy131 = appendSelectClause(yymsp[-3].minor.yy131, yymsp[0].minor.yy256); }
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 170: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy131, NULL, TSDB_SQL_SELECT); }
        break;
      case 171: /* select ::= SELECT selcollist */
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy131, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 172: /* sclp ::= selcollist COMMA */
{yylhsminor.yy131 = yymsp[-1].minor.yy131;}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 173: /* sclp ::= */
      case 203: /* orderby_opt ::= */ yytestcase(yyruleno==203);
{yymsp[1].minor.yy131 = 0;}
        break;
      case 174: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-3].minor.yy131, yymsp[-1].minor.yy46,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 175: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-1].minor.yy131, pNode, 0, 0);
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
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
{yymsp[-1].minor.yy544 = yymsp[0].minor.yy544;}
        break;
      case 183: /* sub ::= LP union RP */
{yymsp[-2].minor.yy544 = addSubqueryElem(NULL, yymsp[-1].minor.yy131, NULL);}
        break;
      case 184: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy544 = addSubqueryElem(NULL, yymsp[-2].minor.yy131, &yymsp[0].minor.yy0);}
        break;
      case 185: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy544 = addSubqueryElem(yymsp[-5].minor.yy544, yymsp[-2].minor.yy131, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy544 = yylhsminor.yy544;
        break;
      case 186: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy544 = yylhsminor.yy544;
        break;
      case 187: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy544 = yylhsminor.yy544;
        break;
      case 188: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-3].minor.yy544, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy544 = yylhsminor.yy544;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-4].minor.yy544, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy544 = yylhsminor.yy544;
        break;
      case 190: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy530.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy530.offset.n = 0;}
        break;
      case 192: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy530.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy530.offset = yymsp[-1].minor.yy0;}
        break;
      case 193: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy530, 0, sizeof(yymsp[1].minor.yy530));}
        break;
      case 194: /* session_option ::= */
{yymsp[1].minor.yy39.col.n = 0; yymsp[1].minor.yy39.gap.n = 0;}
        break;
      case 195: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy39.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy39.gap = yymsp[-1].minor.yy0;
}
        break;
      case 196: /* windowstate_option ::= */
{ yymsp[1].minor.yy538.col.n = 0; yymsp[1].minor.yy538.col.z = NULL;}
        break;
      case 197: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy538.col = yymsp[-1].minor.yy0; }
        break;
      case 198: /* fill_opt ::= */
{ yymsp[1].minor.yy131 = 0;     }
        break;
      case 199: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy131, &A, -1, 0);
    yymsp[-5].minor.yy131 = yymsp[-1].minor.yy131;
}
        break;
      case 200: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy131 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 201: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 202: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 204: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 205: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy131 = tVariantListAppend(yymsp[-3].minor.yy131, &yymsp[-1].minor.yy516, yymsp[0].minor.yy43);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 206: /* sortlist ::= item sortorder */
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[-1].minor.yy516, yymsp[0].minor.yy43);
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 207: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 208: /* sortorder ::= ASC */
{ yymsp[0].minor.yy43 = TSDB_ORDER_ASC; }
        break;
      case 209: /* sortorder ::= DESC */
{ yymsp[0].minor.yy43 = TSDB_ORDER_DESC;}
        break;
      case 210: /* sortorder ::= */
{ yymsp[1].minor.yy43 = TSDB_ORDER_ASC; }
        break;
      case 211: /* groupby_opt ::= */
{ yymsp[1].minor.yy131 = 0;}
        break;
      case 212: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 213: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);
}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 214: /* grouplist ::= item */
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1);
}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 215: /* having_opt ::= */
      case 225: /* where_opt ::= */ yytestcase(yyruleno==225);
      case 268: /* expritem ::= */ yytestcase(yyruleno==268);
{yymsp[1].minor.yy46 = 0;}
        break;
      case 216: /* having_opt ::= HAVING expr */
      case 226: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==226);
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
        break;
      case 217: /* limit_opt ::= */
      case 221: /* slimit_opt ::= */ yytestcase(yyruleno==221);
{yymsp[1].minor.yy284.limit = -1; yymsp[1].minor.yy284.offset = 0;}
        break;
      case 218: /* limit_opt ::= LIMIT signed */
      case 222: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==222);
{yymsp[-1].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-1].minor.yy284.offset = 0;}
        break;
      case 219: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
        break;
      case 220: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
        break;
      case 223: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
        break;
      case 224: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
        break;
      case 227: /* expr ::= LP expr RP */
{yylhsminor.yy46 = yymsp[-1].minor.yy46; yylhsminor.yy46->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy46->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 228: /* expr ::= ID */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 229: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 230: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 231: /* expr ::= INTEGER */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 232: /* expr ::= MINUS INTEGER */
      case 233: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 234: /* expr ::= FLOAT */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 235: /* expr ::= MINUS FLOAT */
      case 236: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 237: /* expr ::= STRING */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 238: /* expr ::= NOW */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 239: /* expr ::= VARIABLE */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 240: /* expr ::= PLUS VARIABLE */
      case 241: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==241);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 242: /* expr ::= BOOL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 243: /* expr ::= NULL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 244: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(yymsp[-1].minor.yy131, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 245: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 246: /* expr ::= expr IS NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 247: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-3].minor.yy46, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 248: /* expr ::= expr LT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 249: /* expr ::= expr GT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 250: /* expr ::= expr LE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 251: /* expr ::= expr GE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 252: /* expr ::= expr NE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 253: /* expr ::= expr EQ expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_EQ);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 254: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy46); yylhsminor.yy46 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy46, yymsp[-2].minor.yy46, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy46, TK_LE), TK_AND);}
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 255: /* expr ::= expr AND expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_AND);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 256: /* expr ::= expr OR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_OR); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 257: /* expr ::= expr PLUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_PLUS);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 258: /* expr ::= expr MINUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MINUS); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 259: /* expr ::= expr STAR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_STAR);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 260: /* expr ::= expr SLASH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_DIVIDE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 261: /* expr ::= expr REM expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_REM);   }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 262: /* expr ::= expr LIKE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LIKE);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 263: /* expr ::= expr MATCH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MATCH);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 264: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-4].minor.yy46, (tSqlExpr*)yymsp[-1].minor.yy131, TK_IN); }
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 265: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy131 = tSqlExprListAppend(yymsp[-2].minor.yy131,yymsp[0].minor.yy46,0, 0);}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 266: /* exprlist ::= expritem */
{yylhsminor.yy131 = tSqlExprListAppend(0,yymsp[0].minor.yy46,0, 0);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 267: /* expritem ::= expr */
{yylhsminor.yy46 = yymsp[0].minor.yy46;}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 269: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 270: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 277: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy516, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 285: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy516, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 288: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 289: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
  assert( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) );
  return yyFallback[iToken];
#else
  (void)iToken;
  return 0;
#endif
}
