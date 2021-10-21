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
#define YYNOCODE 282
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int32_t yy2;
  SCreatedTableInfo yy42;
  tSqlExpr* yy44;
  SRelationInfo* yy46;
  SCreateAcctInfo yy47;
  TAOS_FIELD yy179;
  SLimitVal yy204;
  int yy222;
  SSqlNode* yy246;
  SArray* yy247;
  SCreateDbInfo yy262;
  SCreateTableSql* yy336;
  tVariant yy378;
  int64_t yy403;
  SIntervalVal yy430;
  SWindowStateVal yy492;
  SSessionWindowVal yy507;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             370
#define YYNRULE              296
#define YYNTOKEN             198
#define YY_MAX_SHIFT         369
#define YY_MIN_SHIFTREDUCE   579
#define YY_MAX_SHIFTREDUCE   874
#define YY_ERROR_ACTION      875
#define YY_ACCEPT_ACTION     876
#define YY_NO_ACTION         877
#define YY_MIN_REDUCE        878
#define YY_MAX_REDUCE        1173
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
 /*     0 */   164,  631,  368,  237, 1058, 1030, 1022,  717,  213,  632,
 /*    10 */   876,  369,  669,   59,   60, 1049,   63,   64, 1049, 1149,
 /*    20 */   257,   53,   52,   51,  631,   62,  326,   67,   65,   68,
 /*    30 */    66,  240,  632,   23,  241,   58,   57,  346,  345,   56,
 /*    40 */    55,   54,   59,   60,  210,   63,   64,  164,  164,  257,
 /*    50 */    53,   52,   51,  250,   62,  326,   67,   65,   68,   66,
 /*    60 */   254, 1049,  243,  249,   58,   57, 1036, 1036,   56,   55,
 /*    70 */    54,   59,   60, 1055,   63,   64, 1096,  279,  257,   53,
 /*    80 */    52,   51,  631,   62,  326,   67,   65,   68,   66,   82,
 /*    90 */   632,  251,  324,   58,   57, 1036,  356,   56,   55,   54,
 /*   100 */   631,  324,   97,   59,   61,  211,   63,   64,  632,  157,
 /*   110 */   257,   53,   52,   51,  811,   62,  326,   67,   65,   68,
 /*   120 */    66,  213,  300,   94,   93,   58,   57,  173,  164,   56,
 /*   130 */    55,   54, 1150,  204,  202,  200, 1019, 1020,   35, 1023,
 /*   140 */   199,  143,  142,  141,  140,  288,  580,  581,  582,  583,
 /*   150 */   584,  585,  586,  587,  588,  589,  590,  591,  592,  593,
 /*   160 */   155,   60,  238,   63,   64,   29,  266,  257,   53,   52,
 /*   170 */    51,  125,   62,  326,   67,   65,   68,   66,  179,  286,
 /*   180 */   285,  217,   58,   57,  356,   38,   56,   55,   54,   63,
 /*   190 */    64,  253,   88,  257,   53,   52,   51,  260,   62,  326,
 /*   200 */    67,   65,   68,   66, 1097,   16,  298,   15,   58,   57,
 /*   210 */   776,  777,   56,   55,   54,   44,  322,  363,  362,  321,
 /*   220 */   320,  319,  361,  318,  317,  316,  360,  315,  359,  358,
 /*   230 */    45,  239,  364,  967,   24, 1033,  998,  986,  987,  988,
 /*   240 */   989,  990,  991,  992,  993,  994,  995,  996,  997,  999,
 /*   250 */  1000,  216,  213,  256,  826,  100,  218,  815,  224,  818,
 /*   260 */   754,  821,   81, 1150,  139,  138,  137,  223,  258,  256,
 /*   270 */   826,  331,   88,  815,  261,  818,  259,  821,  334,  333,
 /*   280 */    38,   67,   65,   68,   66,   38,    9,  235,  236,   58,
 /*   290 */    57,  327, 1024,   56,   55,   54,  219, 1021,   44,   38,
 /*   300 */   363,  362, 1144,  235,  236,  361,    5,   41,  184,  360,
 /*   310 */    45,  359,  358,  183,  106,  111,  102,  110,   38,  265,
 /*   320 */   741,   76,  817,  738,  820,  739,  247,  740,  757,  213,
 /*   330 */  1033,  248,  278,  311,   80, 1033,   69,  123,  117,  128,
 /*   340 */  1150,  231,  927,  939,  127,  335,  133,  136,  126, 1033,
 /*   350 */   194,  194,   69,  262,  263,  130, 1006,   38, 1004, 1005,
 /*   360 */    38,   77,   38, 1007,  336,   38,   38, 1008, 1033, 1009,
 /*   370 */  1010,   38,  827,  822, 1143,  271,   38,   58,   57,  823,
 /*   380 */  1142,   56,   55,   54,  275,  274,  928,  266,  827,  822,
 /*   390 */   266,  816,   14,  819,  194,  823,  267,   96,  264,  181,
 /*   400 */   341,  340, 1034,  337,    1,  182,  338, 1033,  342,  793,
 /*   410 */  1033,  343, 1033,   85,  761, 1033, 1032,  344,   56,   55,
 /*   420 */    54, 1033,  348,  367,  366,  148, 1033,   99,  154,  152,
 /*   430 */   151,   95,    3,  195,  280,   86,   73,  773,  742,  743,
 /*   440 */   783,  813,  784,  727,  824,   83,  303,  729,  305,   39,
 /*   450 */   728,  282,  159,   34,  255,   70,  849,   26,   39,  828,
 /*   460 */   328,   39,   70,   98,  116,   70,  115,  792,  630,   79,
 /*   470 */    18,   25,   17,  282,   25,   20,    6,   19,   74,  814,
 /*   480 */   746,  233,  747,   25,  744,  234,  745,  306,  122,   22,
 /*   490 */   121,   21,  135,  134,  214,  215, 1035,  220,  212, 1169,
 /*   500 */   221,  222,  716,  226,  227,  228,  225,  209, 1161, 1107,
 /*   510 */   276, 1106,  245,  825, 1103,  156, 1102,  246,  347,   48,
 /*   520 */  1057, 1068, 1065, 1089, 1066, 1070,  158, 1050,  283, 1031,
 /*   530 */   163,  294,  153, 1088,  174,  175, 1029,  287,  176,  177,
 /*   540 */   944, 1002,  169,  167,  308,  309,  310,  313,  314,   46,
 /*   550 */   772, 1047,  207,  166,   42,  325,  242,  289,  291,  165,
 /*   560 */   938,  332,  301,   78,   50,  830,   75, 1168,  113, 1167,
 /*   570 */   299, 1164,  168,  185,  339,  297, 1160,  119, 1159, 1156,
 /*   580 */   295,  186,  964,   43,  293,   40,  290,   49,   47,  208,
 /*   590 */   924,  129,  922,  637,  131,  132,  920,  919,  268,  197,
 /*   600 */   198,  916,  915,  914,  913,  912,  911,  910,  201,  203,
 /*   610 */   907,  905,  903,  312,  901,  205,  898,  206,  894,  357,
 /*   620 */   349,  281,   84,   89,  292, 1090,  124,  350,  351,  352,
 /*   630 */   353,  354,  355,  365,  874,  232,  270,  252,  307,  269,
 /*   640 */   873,  273,  872,  272,  855,  229,  943,  854,  942,  277,
 /*   650 */   107,  178,  282,  180,  230,  108,   10,  302,   87,  284,
 /*   660 */   749,   30,   90,  918,  774,  917,  160,  144,  145,  188,
 /*   670 */   965,  192,  187,  189,  909,  190,  191,  193,  146,  908,
 /*   680 */     2,  147,  785,  900,  966,   33,  170,  171,  172,  899,
 /*   690 */     4,  161,  162,  779,   91,  244,  781,   92, 1012,  296,
 /*   700 */    31,   11,   32,   12,   13,   27,  304,   28,   99,   36,
 /*   710 */   101,  103,  104,  647,   37,  105,  682,  680,  679,  678,
 /*   720 */   676,  675,  674,  671,  323,  635,  109,    7,  329,  829,
 /*   730 */   330,    8,  831,  112,  114,   71,   72,  118,  719,   39,
 /*   740 */   120,  718,  715,  663,  661,  653,  659,  655,  657,  651,
 /*   750 */   649,  685,  684,  683,  681,  677,  673,  672,  196,  633,
 /*   760 */   597,  878,  877,  877,  877,  877,  877,  877,  877,  877,
 /*   770 */   877,  877,  877,  877,  149,  150,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   201,    1,  201,  202,  201,  201,    0,    5,  269,    9,
 /*    10 */   199,  200,    5,   13,   14,  250,   16,   17,  250,  280,
 /*    20 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*    30 */    30,  266,    9,  269,  266,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  269,   16,   17,  201,  201,   20,
 /*    50 */    21,   22,   23,  249,   25,   26,   27,   28,   29,   30,
 /*    60 */   208,  250,  248,  248,   35,   36,  252,  252,   39,   40,
 /*    70 */    41,   13,   14,  270,   16,   17,  277,  266,   20,   21,
 /*    80 */    22,   23,    1,   25,   26,   27,   28,   29,   30,   89,
 /*    90 */     9,  248,   86,   35,   36,  252,   93,   39,   40,   41,
 /*   100 */     1,   86,  209,   13,   14,  269,   16,   17,    9,  201,
 /*   110 */    20,   21,   22,   23,   85,   25,   26,   27,   28,   29,
 /*   120 */    30,  269,  275,  277,  277,   35,   36,  256,  201,   39,
 /*   130 */    40,   41,  280,   64,   65,   66,  243,  244,  245,  246,
 /*   140 */    71,   72,   73,   74,   75,  274,   47,   48,   49,   50,
 /*   150 */    51,   52,   53,   54,   55,   56,   57,   58,   59,   60,
 /*   160 */    61,   14,   63,   16,   17,   84,  201,   20,   21,   22,
 /*   170 */    23,   80,   25,   26,   27,   28,   29,   30,  213,  271,
 /*   180 */   272,  269,   35,   36,   93,  201,   39,   40,   41,   16,
 /*   190 */    17,  208,   84,   20,   21,   22,   23,   70,   25,   26,
 /*   200 */    27,   28,   29,   30,  277,  148,  279,  150,   35,   36,
 /*   210 */   128,  129,   39,   40,   41,  101,  102,  103,  104,  105,
 /*   220 */   106,  107,  108,  109,  110,  111,  112,  113,  114,  115,
 /*   230 */   122,  247,  224,  225,   46,  251,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,  237,  238,  239,
 /*   250 */   240,   63,  269,    1,    2,  209,  269,    5,   70,    7,
 /*   260 */   100,    9,  209,  280,   76,   77,   78,   79,  208,    1,
 /*   270 */     2,   83,   84,    5,  147,    7,  149,    9,  151,  152,
 /*   280 */   201,   27,   28,   29,   30,  201,  126,   35,   36,   35,
 /*   290 */    36,   39,  246,   39,   40,   41,  269,  244,  101,  201,
 /*   300 */   103,  104,  269,   35,   36,  108,   64,   65,   66,  112,
 /*   310 */   122,  114,  115,   71,   72,   73,   74,   75,  201,   70,
 /*   320 */     2,  100,    5,    5,    7,    7,  247,    9,   39,  269,
 /*   330 */   251,  247,  144,   91,  146,  251,   84,   64,   65,   66,
 /*   340 */   280,  153,  207,  207,   71,  247,   73,   74,   75,  251,
 /*   350 */   215,  215,   84,   35,   36,   82,  226,  201,  228,  229,
 /*   360 */   201,  140,  201,  233,  247,  201,  201,  237,  251,  239,
 /*   370 */   240,  201,  120,  121,  269,  145,  201,   35,   36,  127,
 /*   380 */   269,   39,   40,   41,  154,  155,  207,  201,  120,  121,
 /*   390 */   201,    5,   84,    7,  215,  127,  147,   89,  149,  213,
 /*   400 */   151,  152,  213,  247,  211,  212,  247,  251,  247,   78,
 /*   410 */   251,  247,  251,   85,  125,  251,  251,  247,   39,   40,
 /*   420 */    41,  251,  247,   67,   68,   69,  251,  119,   64,   65,
 /*   430 */    66,  253,  205,  206,   85,   85,  100,   85,  120,  121,
 /*   440 */    85,    1,   85,   85,  127,  267,   85,   85,   85,  100,
 /*   450 */    85,  123,  100,   84,   62,  100,   85,  100,  100,   85,
 /*   460 */    15,  100,  100,  100,  148,  100,  150,  136,   85,   84,
 /*   470 */   148,  100,  150,  123,  100,  148,   84,  150,  142,   39,
 /*   480 */     5,  269,    7,  100,    5,  269,    7,  118,  148,  148,
 /*   490 */   150,  150,   80,   81,  269,  269,  252,  269,  269,  252,
 /*   500 */   269,  269,  117,  269,  269,  269,  269,  269,  252,  242,
 /*   510 */   201,  242,  242,  127,  242,  201,  242,  242,  242,  268,
 /*   520 */   201,  201,  201,  278,  201,  201,  201,  250,  250,  250,
 /*   530 */   201,  201,   62,  278,  254,  201,  201,  273,  201,  201,
 /*   540 */   201,  241,  260,  262,  201,  201,  201,  201,  201,  201,
 /*   550 */   127,  265,  201,  263,  201,  201,  273,  273,  273,  264,
 /*   560 */   201,  201,  134,  139,  138,  120,  141,  201,  201,  201,
 /*   570 */   137,  201,  261,  201,  201,  132,  201,  201,  201,  201,
 /*   580 */   131,  201,  201,  201,  130,  201,  133,  143,  201,  201,
 /*   590 */   201,  201,  201,   88,  201,  201,  201,  201,  201,  201,
 /*   600 */   201,  201,  201,  201,  201,  201,  201,  201,  201,  201,
 /*   610 */   201,  201,  201,   92,  201,  201,  201,  201,  201,  116,
 /*   620 */    98,  203,  203,  203,  203,  203,   99,   53,   95,   97,
 /*   630 */    57,   96,   94,   86,    5,  203,    5,  203,  203,  156,
 /*   640 */     5,    5,    5,  156,  103,  203,  210,  102,  210,  145,
 /*   650 */   209,  214,  123,  214,  203,  209,   84,  118,  124,  100,
 /*   660 */    85,   84,  100,  203,   85,  203,   84,  204,  204,  221,
 /*   670 */   223,  219,  222,  217,  203,  220,  218,  216,  204,  203,
 /*   680 */   211,  204,   85,  203,  225,  255,  259,  258,  257,  203,
 /*   690 */   205,   84,  100,   85,   84,    1,   85,   84,  241,   84,
 /*   700 */   100,  135,  100,  135,   84,   84,  118,   84,  119,   90,
 /*   710 */    80,   89,   72,    5,   90,   89,    9,    5,    5,    5,
 /*   720 */     5,    5,    5,    5,   15,   87,   80,   84,   26,   85,
 /*   730 */    61,   84,  120,  150,  150,   16,   16,  150,    5,  100,
 /*   740 */   150,    5,   85,    5,    5,    5,    5,    5,    5,    5,
 /*   750 */     5,    5,    5,    5,    5,    5,    5,    5,  100,   87,
 /*   760 */    62,    0,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   770 */   281,  281,  281,  281,   21,   21,  281,  281,  281,  281,
 /*   780 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   790 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   800 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   810 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   820 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   830 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   840 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   850 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   860 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   870 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   880 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   890 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   900 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   910 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   920 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   930 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   940 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   950 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   960 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   970 */   281,  281,  281,  281,
};
#define YY_SHIFT_COUNT    (369)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (761)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   188,  114,  114,  197,  197,   15,  252,  268,  268,   81,
 /*    10 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*    20 */    23,   23,   23,    0,   99,  268,  318,  318,  318,  108,
 /*    30 */   108,   23,   23,   82,   23,    6,   23,   23,   23,   23,
 /*    40 */    91,   15,    3,    3,    7,  776,  776,  776,  268,  268,
 /*    50 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*    60 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*    70 */   318,  318,  318,    2,    2,    2,    2,    2,    2,    2,
 /*    80 */    23,   23,   23,  289,   23,   23,   23,  108,  108,   23,
 /*    90 */    23,   23,   23,  331,  331,  160,  108,   23,   23,   23,
 /*   100 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   110 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   120 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   130 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   140 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   150 */    23,   23,   23,   23,   23,   23,  470,  470,  470,  423,
 /*   160 */   423,  423,  423,  470,  470,  424,  425,  428,  426,  433,
 /*   170 */   443,  449,  454,  453,  444,  470,  470,  470,  505,  521,
 /*   180 */   505,  521,  503,   15,   15,  470,  470,  527,  522,  574,
 /*   190 */   533,  532,  573,  535,  538,  503,    7,  470,  470,  547,
 /*   200 */   547,  470,  547,  470,  547,  470,  470,  776,  776,   29,
 /*   210 */    58,   58,   90,   58,  147,  173,  242,  254,  254,  254,
 /*   220 */   254,  254,  254,  273,   69,  342,  342,  342,  342,  127,
 /*   230 */   249,  230,  308,  379,  379,  317,  386,  356,  364,  349,
 /*   240 */   328,  350,  352,  355,  357,  336,  221,  358,  361,  362,
 /*   250 */   363,  365,  369,  371,  374,  440,  392,  445,  383,   57,
 /*   260 */   316,  322,  475,  479,  327,  340,  385,  341,  412,  629,
 /*   270 */   483,  631,  635,  487,  636,  637,  541,  545,  504,  529,
 /*   280 */   539,  572,  534,  575,  577,  559,  562,  579,  582,  597,
 /*   290 */   607,  608,  592,  610,  611,  613,  694,  615,  600,  566,
 /*   300 */   602,  568,  620,  539,  621,  588,  623,  589,  630,  619,
 /*   310 */   622,  640,  708,  624,  626,  707,  712,  713,  714,  715,
 /*   320 */   716,  717,  718,  638,  709,  646,  643,  644,  612,  647,
 /*   330 */   702,  669,  719,  583,  584,  639,  639,  639,  639,  720,
 /*   340 */   587,  590,  639,  639,  639,  733,  736,  657,  639,  738,
 /*   350 */   739,  740,  741,  742,  743,  744,  745,  746,  747,  748,
 /*   360 */   749,  750,  751,  752,  658,  672,  753,  754,  698,  761,
};
#define YY_REDUCE_COUNT (208)
#define YY_REDUCE_MIN   (-261)
#define YY_REDUCE_MAX   (486)
static const short yy_reduce_ofst[] = {
 /*     0 */  -189,   10,   10,  130,  130, -107, -148,  -17,   60,  -92,
 /*    10 */   -16,  -73, -153,   79,   84,   98,  117,  156,  159,  161,
 /*    20 */   164,  170,  175, -197, -199, -261, -186, -185, -157, -235,
 /*    30 */  -232, -201, -154, -129, -196,   46,  -35,  186,  189,  165,
 /*    40 */   135,   53,  136,  179,    8,  178,  193,  227, -236, -225,
 /*    50 */  -164,  -88,  -13,   27,   33,  105,  111,  212,  216,  225,
 /*    60 */   226,  228,  229,  231,  232,  234,  235,  236,  237,  238,
 /*    70 */   244,  247,  256,  267,  269,  270,  272,  274,  275,  276,
 /*    80 */   309,  314,  319,  251,  320,  321,  323,  277,  278,  324,
 /*    90 */   325,  329,  330,  245,  255,  280,  279,  334,  335,  337,
 /*   100 */   338,  339,  343,  344,  345,  346,  347,  348,  351,  353,
 /*   110 */   354,  359,  360,  366,  367,  368,  370,  372,  373,  375,
 /*   120 */   376,  377,  378,  380,  381,  382,  384,  387,  388,  389,
 /*   130 */   390,  391,  393,  394,  395,  396,  397,  398,  399,  400,
 /*   140 */   401,  402,  403,  404,  405,  406,  407,  408,  409,  410,
 /*   150 */   411,  413,  414,  415,  416,  417,  418,  419,  420,  264,
 /*   160 */   283,  284,  285,  421,  422,  286,  295,  290,  281,  311,
 /*   170 */   282,  427,  429,  431,  430,  432,  434,  435,  436,  437,
 /*   180 */   438,  439,  300,  441,  446,  442,  451,  447,  450,  448,
 /*   190 */   456,  455,  458,  452,  461,  457,  459,  460,  462,  463,
 /*   200 */   464,  471,  474,  476,  477,  480,  486,  469,  485,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   875, 1001,  940, 1011,  925,  935, 1152, 1152, 1152,  875,
 /*    10 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*    20 */   875,  875,  875, 1059,  895, 1152,  875,  875,  875,  875,
 /*    30 */   875,  875,  875, 1074,  875,  935,  875,  875,  875,  875,
 /*    40 */   947,  935,  947,  947,  875, 1054,  985, 1003,  875,  875,
 /*    50 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*    60 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*    70 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*    80 */   875,  875,  875, 1061, 1067, 1064,  875,  875,  875, 1069,
 /*    90 */   875,  875,  875, 1093, 1093, 1052,  875,  875,  875,  875,
 /*   100 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   110 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   120 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  923,
 /*   130 */   875,  921,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   140 */   875,  875,  875,  875,  875,  875,  875,  875,  906,  875,
 /*   150 */   875,  875,  875,  875,  875,  893,  897,  897,  897,  875,
 /*   160 */   875,  875,  875,  897,  897, 1100, 1104, 1086, 1098, 1094,
 /*   170 */  1081, 1079, 1077, 1085, 1108,  897,  897,  897,  937,  945,
 /*   180 */   937,  945,  941,  935,  935,  897,  897,  963,  961,  959,
 /*   190 */   951,  957,  953,  955,  949,  926,  875,  897,  897,  933,
 /*   200 */   933,  897,  933,  897,  933,  897,  897,  985, 1003,  875,
 /*   210 */  1109, 1099,  875, 1151, 1139, 1138,  875, 1147, 1146, 1145,
 /*   220 */  1137, 1136, 1135,  875,  875, 1131, 1134, 1133, 1132,  875,
 /*   230 */   875,  875,  875, 1141, 1140,  875,  875,  875,  875,  875,
 /*   240 */   875,  875,  875,  875,  875, 1105, 1101,  875,  875,  875,
 /*   250 */   875,  875,  875,  875,  875,  875, 1111,  875,  875,  875,
 /*   260 */   875,  875,  875,  875,  875,  875, 1013,  875,  875,  875,
 /*   270 */   875,  875,  875,  875,  875,  875,  875,  875,  875, 1051,
 /*   280 */   875,  875,  875,  875,  875, 1063, 1062,  875,  875,  875,
 /*   290 */   875,  875,  875,  875,  875,  875,  875,  875, 1095,  875,
 /*   300 */  1087,  875,  875, 1025,  875,  875,  875,  875,  875,  875,
 /*   310 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   320 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   330 */   875,  875,  875,  875,  875, 1170, 1165, 1166, 1163,  875,
 /*   340 */   875,  875, 1162, 1157, 1158,  875,  875,  875, 1155,  875,
 /*   350 */   875,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   360 */   875,  875,  875,  875,  969,  875,  904,  902,  875,  875,
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
    0,  /*     NEEDTS => nothing */
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
  /*   88 */ "NEEDTS",
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
  /*  116 */ "PARTITIONS",
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
  /*  187 */ "IPTOKEN",
  /*  188 */ "SEMI",
  /*  189 */ "NONE",
  /*  190 */ "PREV",
  /*  191 */ "LINEAR",
  /*  192 */ "IMPORT",
  /*  193 */ "TBNAME",
  /*  194 */ "JOIN",
  /*  195 */ "INSERT",
  /*  196 */ "INTO",
  /*  197 */ "VALUES",
  /*  198 */ "error",
  /*  199 */ "program",
  /*  200 */ "cmd",
  /*  201 */ "ids",
  /*  202 */ "dbPrefix",
  /*  203 */ "cpxName",
  /*  204 */ "ifexists",
  /*  205 */ "alter_db_optr",
  /*  206 */ "alter_topic_optr",
  /*  207 */ "acct_optr",
  /*  208 */ "exprlist",
  /*  209 */ "ifnotexists",
  /*  210 */ "needts",
  /*  211 */ "db_optr",
  /*  212 */ "topic_optr",
  /*  213 */ "typename",
  /*  214 */ "bufsize",
  /*  215 */ "pps",
  /*  216 */ "tseries",
  /*  217 */ "dbs",
  /*  218 */ "streams",
  /*  219 */ "storage",
  /*  220 */ "qtime",
  /*  221 */ "users",
  /*  222 */ "conns",
  /*  223 */ "state",
  /*  224 */ "intitemlist",
  /*  225 */ "intitem",
  /*  226 */ "keep",
  /*  227 */ "cache",
  /*  228 */ "replica",
  /*  229 */ "quorum",
  /*  230 */ "days",
  /*  231 */ "minrows",
  /*  232 */ "maxrows",
  /*  233 */ "blocks",
  /*  234 */ "ctime",
  /*  235 */ "wal",
  /*  236 */ "fsync",
  /*  237 */ "comp",
  /*  238 */ "prec",
  /*  239 */ "update",
  /*  240 */ "cachelast",
  /*  241 */ "partitions",
  /*  242 */ "signed",
  /*  243 */ "create_table_args",
  /*  244 */ "create_stable_args",
  /*  245 */ "create_table_list",
  /*  246 */ "create_from_stable",
  /*  247 */ "columnlist",
  /*  248 */ "tagitemlist",
  /*  249 */ "tagNamelist",
  /*  250 */ "select",
  /*  251 */ "column",
  /*  252 */ "tagitem",
  /*  253 */ "selcollist",
  /*  254 */ "from",
  /*  255 */ "where_opt",
  /*  256 */ "interval_option",
  /*  257 */ "sliding_opt",
  /*  258 */ "session_option",
  /*  259 */ "windowstate_option",
  /*  260 */ "fill_opt",
  /*  261 */ "groupby_opt",
  /*  262 */ "having_opt",
  /*  263 */ "orderby_opt",
  /*  264 */ "slimit_opt",
  /*  265 */ "limit_opt",
  /*  266 */ "union",
  /*  267 */ "sclp",
  /*  268 */ "distinct",
  /*  269 */ "expr",
  /*  270 */ "as",
  /*  271 */ "tablelist",
  /*  272 */ "sub",
  /*  273 */ "tmvar",
  /*  274 */ "intervalKey",
  /*  275 */ "sortlist",
  /*  276 */ "sortitem",
  /*  277 */ "item",
  /*  278 */ "sortorder",
  /*  279 */ "grouplist",
  /*  280 */ "expritem",
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
 /*  58 */ "needts ::= NEEDTS",
 /*  59 */ "needts ::=",
 /*  60 */ "cmd ::= CREATE DNODE ids",
 /*  61 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  62 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  63 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  64 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts",
 /*  65 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts",
 /*  66 */ "cmd ::= CREATE USER ids PASS ids",
 /*  67 */ "bufsize ::=",
 /*  68 */ "bufsize ::= BUFSIZE INTEGER",
 /*  69 */ "pps ::=",
 /*  70 */ "pps ::= PPS INTEGER",
 /*  71 */ "tseries ::=",
 /*  72 */ "tseries ::= TSERIES INTEGER",
 /*  73 */ "dbs ::=",
 /*  74 */ "dbs ::= DBS INTEGER",
 /*  75 */ "streams ::=",
 /*  76 */ "streams ::= STREAMS INTEGER",
 /*  77 */ "storage ::=",
 /*  78 */ "storage ::= STORAGE INTEGER",
 /*  79 */ "qtime ::=",
 /*  80 */ "qtime ::= QTIME INTEGER",
 /*  81 */ "users ::=",
 /*  82 */ "users ::= USERS INTEGER",
 /*  83 */ "conns ::=",
 /*  84 */ "conns ::= CONNS INTEGER",
 /*  85 */ "state ::=",
 /*  86 */ "state ::= STATE ids",
 /*  87 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  88 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  89 */ "intitemlist ::= intitem",
 /*  90 */ "intitem ::= INTEGER",
 /*  91 */ "keep ::= KEEP intitemlist",
 /*  92 */ "cache ::= CACHE INTEGER",
 /*  93 */ "replica ::= REPLICA INTEGER",
 /*  94 */ "quorum ::= QUORUM INTEGER",
 /*  95 */ "days ::= DAYS INTEGER",
 /*  96 */ "minrows ::= MINROWS INTEGER",
 /*  97 */ "maxrows ::= MAXROWS INTEGER",
 /*  98 */ "blocks ::= BLOCKS INTEGER",
 /*  99 */ "ctime ::= CTIME INTEGER",
 /* 100 */ "wal ::= WAL INTEGER",
 /* 101 */ "fsync ::= FSYNC INTEGER",
 /* 102 */ "comp ::= COMP INTEGER",
 /* 103 */ "prec ::= PRECISION STRING",
 /* 104 */ "update ::= UPDATE INTEGER",
 /* 105 */ "cachelast ::= CACHELAST INTEGER",
 /* 106 */ "partitions ::= PARTITIONS INTEGER",
 /* 107 */ "db_optr ::=",
 /* 108 */ "db_optr ::= db_optr cache",
 /* 109 */ "db_optr ::= db_optr replica",
 /* 110 */ "db_optr ::= db_optr quorum",
 /* 111 */ "db_optr ::= db_optr days",
 /* 112 */ "db_optr ::= db_optr minrows",
 /* 113 */ "db_optr ::= db_optr maxrows",
 /* 114 */ "db_optr ::= db_optr blocks",
 /* 115 */ "db_optr ::= db_optr ctime",
 /* 116 */ "db_optr ::= db_optr wal",
 /* 117 */ "db_optr ::= db_optr fsync",
 /* 118 */ "db_optr ::= db_optr comp",
 /* 119 */ "db_optr ::= db_optr prec",
 /* 120 */ "db_optr ::= db_optr keep",
 /* 121 */ "db_optr ::= db_optr update",
 /* 122 */ "db_optr ::= db_optr cachelast",
 /* 123 */ "topic_optr ::= db_optr",
 /* 124 */ "topic_optr ::= topic_optr partitions",
 /* 125 */ "alter_db_optr ::=",
 /* 126 */ "alter_db_optr ::= alter_db_optr replica",
 /* 127 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 128 */ "alter_db_optr ::= alter_db_optr keep",
 /* 129 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 130 */ "alter_db_optr ::= alter_db_optr comp",
 /* 131 */ "alter_db_optr ::= alter_db_optr update",
 /* 132 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 133 */ "alter_topic_optr ::= alter_db_optr",
 /* 134 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 135 */ "typename ::= ids",
 /* 136 */ "typename ::= ids LP signed RP",
 /* 137 */ "typename ::= ids UNSIGNED",
 /* 138 */ "signed ::= INTEGER",
 /* 139 */ "signed ::= PLUS INTEGER",
 /* 140 */ "signed ::= MINUS INTEGER",
 /* 141 */ "cmd ::= CREATE TABLE create_table_args",
 /* 142 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 143 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 144 */ "cmd ::= CREATE TABLE create_table_list",
 /* 145 */ "create_table_list ::= create_from_stable",
 /* 146 */ "create_table_list ::= create_table_list create_from_stable",
 /* 147 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 148 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 149 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 150 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 151 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 152 */ "tagNamelist ::= ids",
 /* 153 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 154 */ "columnlist ::= columnlist COMMA column",
 /* 155 */ "columnlist ::= column",
 /* 156 */ "column ::= ids typename",
 /* 157 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 158 */ "tagitemlist ::= tagitem",
 /* 159 */ "tagitem ::= INTEGER",
 /* 160 */ "tagitem ::= FLOAT",
 /* 161 */ "tagitem ::= STRING",
 /* 162 */ "tagitem ::= BOOL",
 /* 163 */ "tagitem ::= NULL",
 /* 164 */ "tagitem ::= NOW",
 /* 165 */ "tagitem ::= MINUS INTEGER",
 /* 166 */ "tagitem ::= MINUS FLOAT",
 /* 167 */ "tagitem ::= PLUS INTEGER",
 /* 168 */ "tagitem ::= PLUS FLOAT",
 /* 169 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 170 */ "select ::= LP select RP",
 /* 171 */ "union ::= select",
 /* 172 */ "union ::= union UNION ALL select",
 /* 173 */ "cmd ::= union",
 /* 174 */ "select ::= SELECT selcollist",
 /* 175 */ "sclp ::= selcollist COMMA",
 /* 176 */ "sclp ::=",
 /* 177 */ "selcollist ::= sclp distinct expr as",
 /* 178 */ "selcollist ::= sclp STAR",
 /* 179 */ "as ::= AS ids",
 /* 180 */ "as ::= ids",
 /* 181 */ "as ::=",
 /* 182 */ "distinct ::= DISTINCT",
 /* 183 */ "distinct ::=",
 /* 184 */ "from ::= FROM tablelist",
 /* 185 */ "from ::= FROM sub",
 /* 186 */ "sub ::= LP union RP",
 /* 187 */ "sub ::= LP union RP ids",
 /* 188 */ "sub ::= sub COMMA LP union RP ids",
 /* 189 */ "tablelist ::= ids cpxName",
 /* 190 */ "tablelist ::= ids cpxName ids",
 /* 191 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 192 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 193 */ "tmvar ::= VARIABLE",
 /* 194 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 195 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 196 */ "interval_option ::=",
 /* 197 */ "intervalKey ::= INTERVAL",
 /* 198 */ "intervalKey ::= EVERY",
 /* 199 */ "session_option ::=",
 /* 200 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 201 */ "windowstate_option ::=",
 /* 202 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 203 */ "fill_opt ::=",
 /* 204 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 205 */ "fill_opt ::= FILL LP ID RP",
 /* 206 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 207 */ "sliding_opt ::=",
 /* 208 */ "orderby_opt ::=",
 /* 209 */ "orderby_opt ::= ORDER BY sortlist",
 /* 210 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 211 */ "sortlist ::= item sortorder",
 /* 212 */ "item ::= ids cpxName",
 /* 213 */ "sortorder ::= ASC",
 /* 214 */ "sortorder ::= DESC",
 /* 215 */ "sortorder ::=",
 /* 216 */ "groupby_opt ::=",
 /* 217 */ "groupby_opt ::= GROUP BY grouplist",
 /* 218 */ "grouplist ::= grouplist COMMA item",
 /* 219 */ "grouplist ::= item",
 /* 220 */ "having_opt ::=",
 /* 221 */ "having_opt ::= HAVING expr",
 /* 222 */ "limit_opt ::=",
 /* 223 */ "limit_opt ::= LIMIT signed",
 /* 224 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 225 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 226 */ "slimit_opt ::=",
 /* 227 */ "slimit_opt ::= SLIMIT signed",
 /* 228 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 229 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 230 */ "where_opt ::=",
 /* 231 */ "where_opt ::= WHERE expr",
 /* 232 */ "expr ::= LP expr RP",
 /* 233 */ "expr ::= ID",
 /* 234 */ "expr ::= ID DOT ID",
 /* 235 */ "expr ::= ID DOT STAR",
 /* 236 */ "expr ::= INTEGER",
 /* 237 */ "expr ::= MINUS INTEGER",
 /* 238 */ "expr ::= PLUS INTEGER",
 /* 239 */ "expr ::= FLOAT",
 /* 240 */ "expr ::= MINUS FLOAT",
 /* 241 */ "expr ::= PLUS FLOAT",
 /* 242 */ "expr ::= STRING",
 /* 243 */ "expr ::= NOW",
 /* 244 */ "expr ::= VARIABLE",
 /* 245 */ "expr ::= PLUS VARIABLE",
 /* 246 */ "expr ::= MINUS VARIABLE",
 /* 247 */ "expr ::= BOOL",
 /* 248 */ "expr ::= NULL",
 /* 249 */ "expr ::= ID LP exprlist RP",
 /* 250 */ "expr ::= ID LP STAR RP",
 /* 251 */ "expr ::= expr IS NULL",
 /* 252 */ "expr ::= expr IS NOT NULL",
 /* 253 */ "expr ::= expr LT expr",
 /* 254 */ "expr ::= expr GT expr",
 /* 255 */ "expr ::= expr LE expr",
 /* 256 */ "expr ::= expr GE expr",
 /* 257 */ "expr ::= expr NE expr",
 /* 258 */ "expr ::= expr EQ expr",
 /* 259 */ "expr ::= expr BETWEEN expr AND expr",
 /* 260 */ "expr ::= expr AND expr",
 /* 261 */ "expr ::= expr OR expr",
 /* 262 */ "expr ::= expr PLUS expr",
 /* 263 */ "expr ::= expr MINUS expr",
 /* 264 */ "expr ::= expr STAR expr",
 /* 265 */ "expr ::= expr SLASH expr",
 /* 266 */ "expr ::= expr REM expr",
 /* 267 */ "expr ::= expr LIKE expr",
 /* 268 */ "expr ::= expr MATCH expr",
 /* 269 */ "expr ::= expr NMATCH expr",
 /* 270 */ "expr ::= expr IN LP exprlist RP",
 /* 271 */ "exprlist ::= exprlist COMMA expritem",
 /* 272 */ "exprlist ::= expritem",
 /* 273 */ "expritem ::= expr",
 /* 274 */ "expritem ::=",
 /* 275 */ "cmd ::= RESET QUERY CACHE",
 /* 276 */ "cmd ::= SYNCDB ids REPLICA",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 293 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 294 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 295 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 208: /* exprlist */
    case 253: /* selcollist */
    case 267: /* sclp */
{
tSqlExprListDestroy((yypminor->yy247));
}
      break;
    case 224: /* intitemlist */
    case 226: /* keep */
    case 247: /* columnlist */
    case 248: /* tagitemlist */
    case 249: /* tagNamelist */
    case 260: /* fill_opt */
    case 261: /* groupby_opt */
    case 263: /* orderby_opt */
    case 275: /* sortlist */
    case 279: /* grouplist */
{
taosArrayDestroy((yypminor->yy247));
}
      break;
    case 245: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy336));
}
      break;
    case 250: /* select */
{
destroySqlNode((yypminor->yy246));
}
      break;
    case 254: /* from */
    case 271: /* tablelist */
    case 272: /* sub */
{
destroyRelationInfo((yypminor->yy46));
}
      break;
    case 255: /* where_opt */
    case 262: /* having_opt */
    case 269: /* expr */
    case 280: /* expritem */
{
tSqlExprDestroy((yypminor->yy44));
}
      break;
    case 266: /* union */
{
destroyAllSqlNode((yypminor->yy247));
}
      break;
    case 276: /* sortitem */
{
tVariantDestroy(&(yypminor->yy378));
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
  {  199,   -1 }, /* (0) program ::= cmd */
  {  200,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  200,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  200,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  200,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  200,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  200,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  200,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  200,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  200,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  200,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  200,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  200,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  200,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  200,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  200,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  200,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  202,    0 }, /* (17) dbPrefix ::= */
  {  202,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  203,    0 }, /* (19) cpxName ::= */
  {  203,   -2 }, /* (20) cpxName ::= DOT ids */
  {  200,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  200,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  200,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  200,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  200,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  200,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  200,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  200,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  200,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  200,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  200,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  200,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  200,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  200,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  200,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  200,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  200,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  200,   -2 }, /* (38) cmd ::= USE ids */
  {  200,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  200,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  200,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  200,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  200,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  200,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  200,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  200,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  200,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  200,   -4 }, /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  200,   -4 }, /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  200,   -6 }, /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  200,   -6 }, /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  201,   -1 }, /* (52) ids ::= ID */
  {  201,   -1 }, /* (53) ids ::= STRING */
  {  204,   -2 }, /* (54) ifexists ::= IF EXISTS */
  {  204,    0 }, /* (55) ifexists ::= */
  {  209,   -3 }, /* (56) ifnotexists ::= IF NOT EXISTS */
  {  209,    0 }, /* (57) ifnotexists ::= */
  {  210,   -1 }, /* (58) needts ::= NEEDTS */
  {  210,    0 }, /* (59) needts ::= */
  {  200,   -3 }, /* (60) cmd ::= CREATE DNODE ids */
  {  200,   -6 }, /* (61) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  200,   -5 }, /* (62) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  200,   -5 }, /* (63) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  200,   -9 }, /* (64) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts */
  {  200,  -10 }, /* (65) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts */
  {  200,   -5 }, /* (66) cmd ::= CREATE USER ids PASS ids */
  {  214,    0 }, /* (67) bufsize ::= */
  {  214,   -2 }, /* (68) bufsize ::= BUFSIZE INTEGER */
  {  215,    0 }, /* (69) pps ::= */
  {  215,   -2 }, /* (70) pps ::= PPS INTEGER */
  {  216,    0 }, /* (71) tseries ::= */
  {  216,   -2 }, /* (72) tseries ::= TSERIES INTEGER */
  {  217,    0 }, /* (73) dbs ::= */
  {  217,   -2 }, /* (74) dbs ::= DBS INTEGER */
  {  218,    0 }, /* (75) streams ::= */
  {  218,   -2 }, /* (76) streams ::= STREAMS INTEGER */
  {  219,    0 }, /* (77) storage ::= */
  {  219,   -2 }, /* (78) storage ::= STORAGE INTEGER */
  {  220,    0 }, /* (79) qtime ::= */
  {  220,   -2 }, /* (80) qtime ::= QTIME INTEGER */
  {  221,    0 }, /* (81) users ::= */
  {  221,   -2 }, /* (82) users ::= USERS INTEGER */
  {  222,    0 }, /* (83) conns ::= */
  {  222,   -2 }, /* (84) conns ::= CONNS INTEGER */
  {  223,    0 }, /* (85) state ::= */
  {  223,   -2 }, /* (86) state ::= STATE ids */
  {  207,   -9 }, /* (87) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  224,   -3 }, /* (88) intitemlist ::= intitemlist COMMA intitem */
  {  224,   -1 }, /* (89) intitemlist ::= intitem */
  {  225,   -1 }, /* (90) intitem ::= INTEGER */
  {  226,   -2 }, /* (91) keep ::= KEEP intitemlist */
  {  227,   -2 }, /* (92) cache ::= CACHE INTEGER */
  {  228,   -2 }, /* (93) replica ::= REPLICA INTEGER */
  {  229,   -2 }, /* (94) quorum ::= QUORUM INTEGER */
  {  230,   -2 }, /* (95) days ::= DAYS INTEGER */
  {  231,   -2 }, /* (96) minrows ::= MINROWS INTEGER */
  {  232,   -2 }, /* (97) maxrows ::= MAXROWS INTEGER */
  {  233,   -2 }, /* (98) blocks ::= BLOCKS INTEGER */
  {  234,   -2 }, /* (99) ctime ::= CTIME INTEGER */
  {  235,   -2 }, /* (100) wal ::= WAL INTEGER */
  {  236,   -2 }, /* (101) fsync ::= FSYNC INTEGER */
  {  237,   -2 }, /* (102) comp ::= COMP INTEGER */
  {  238,   -2 }, /* (103) prec ::= PRECISION STRING */
  {  239,   -2 }, /* (104) update ::= UPDATE INTEGER */
  {  240,   -2 }, /* (105) cachelast ::= CACHELAST INTEGER */
  {  241,   -2 }, /* (106) partitions ::= PARTITIONS INTEGER */
  {  211,    0 }, /* (107) db_optr ::= */
  {  211,   -2 }, /* (108) db_optr ::= db_optr cache */
  {  211,   -2 }, /* (109) db_optr ::= db_optr replica */
  {  211,   -2 }, /* (110) db_optr ::= db_optr quorum */
  {  211,   -2 }, /* (111) db_optr ::= db_optr days */
  {  211,   -2 }, /* (112) db_optr ::= db_optr minrows */
  {  211,   -2 }, /* (113) db_optr ::= db_optr maxrows */
  {  211,   -2 }, /* (114) db_optr ::= db_optr blocks */
  {  211,   -2 }, /* (115) db_optr ::= db_optr ctime */
  {  211,   -2 }, /* (116) db_optr ::= db_optr wal */
  {  211,   -2 }, /* (117) db_optr ::= db_optr fsync */
  {  211,   -2 }, /* (118) db_optr ::= db_optr comp */
  {  211,   -2 }, /* (119) db_optr ::= db_optr prec */
  {  211,   -2 }, /* (120) db_optr ::= db_optr keep */
  {  211,   -2 }, /* (121) db_optr ::= db_optr update */
  {  211,   -2 }, /* (122) db_optr ::= db_optr cachelast */
  {  212,   -1 }, /* (123) topic_optr ::= db_optr */
  {  212,   -2 }, /* (124) topic_optr ::= topic_optr partitions */
  {  205,    0 }, /* (125) alter_db_optr ::= */
  {  205,   -2 }, /* (126) alter_db_optr ::= alter_db_optr replica */
  {  205,   -2 }, /* (127) alter_db_optr ::= alter_db_optr quorum */
  {  205,   -2 }, /* (128) alter_db_optr ::= alter_db_optr keep */
  {  205,   -2 }, /* (129) alter_db_optr ::= alter_db_optr blocks */
  {  205,   -2 }, /* (130) alter_db_optr ::= alter_db_optr comp */
  {  205,   -2 }, /* (131) alter_db_optr ::= alter_db_optr update */
  {  205,   -2 }, /* (132) alter_db_optr ::= alter_db_optr cachelast */
  {  206,   -1 }, /* (133) alter_topic_optr ::= alter_db_optr */
  {  206,   -2 }, /* (134) alter_topic_optr ::= alter_topic_optr partitions */
  {  213,   -1 }, /* (135) typename ::= ids */
  {  213,   -4 }, /* (136) typename ::= ids LP signed RP */
  {  213,   -2 }, /* (137) typename ::= ids UNSIGNED */
  {  242,   -1 }, /* (138) signed ::= INTEGER */
  {  242,   -2 }, /* (139) signed ::= PLUS INTEGER */
  {  242,   -2 }, /* (140) signed ::= MINUS INTEGER */
  {  200,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_args */
  {  200,   -3 }, /* (142) cmd ::= CREATE TABLE create_stable_args */
  {  200,   -3 }, /* (143) cmd ::= CREATE STABLE create_stable_args */
  {  200,   -3 }, /* (144) cmd ::= CREATE TABLE create_table_list */
  {  245,   -1 }, /* (145) create_table_list ::= create_from_stable */
  {  245,   -2 }, /* (146) create_table_list ::= create_table_list create_from_stable */
  {  243,   -6 }, /* (147) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  244,  -10 }, /* (148) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  246,  -10 }, /* (149) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  246,  -13 }, /* (150) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  249,   -3 }, /* (151) tagNamelist ::= tagNamelist COMMA ids */
  {  249,   -1 }, /* (152) tagNamelist ::= ids */
  {  243,   -5 }, /* (153) create_table_args ::= ifnotexists ids cpxName AS select */
  {  247,   -3 }, /* (154) columnlist ::= columnlist COMMA column */
  {  247,   -1 }, /* (155) columnlist ::= column */
  {  251,   -2 }, /* (156) column ::= ids typename */
  {  248,   -3 }, /* (157) tagitemlist ::= tagitemlist COMMA tagitem */
  {  248,   -1 }, /* (158) tagitemlist ::= tagitem */
  {  252,   -1 }, /* (159) tagitem ::= INTEGER */
  {  252,   -1 }, /* (160) tagitem ::= FLOAT */
  {  252,   -1 }, /* (161) tagitem ::= STRING */
  {  252,   -1 }, /* (162) tagitem ::= BOOL */
  {  252,   -1 }, /* (163) tagitem ::= NULL */
  {  252,   -1 }, /* (164) tagitem ::= NOW */
  {  252,   -2 }, /* (165) tagitem ::= MINUS INTEGER */
  {  252,   -2 }, /* (166) tagitem ::= MINUS FLOAT */
  {  252,   -2 }, /* (167) tagitem ::= PLUS INTEGER */
  {  252,   -2 }, /* (168) tagitem ::= PLUS FLOAT */
  {  250,  -14 }, /* (169) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  250,   -3 }, /* (170) select ::= LP select RP */
  {  266,   -1 }, /* (171) union ::= select */
  {  266,   -4 }, /* (172) union ::= union UNION ALL select */
  {  200,   -1 }, /* (173) cmd ::= union */
  {  250,   -2 }, /* (174) select ::= SELECT selcollist */
  {  267,   -2 }, /* (175) sclp ::= selcollist COMMA */
  {  267,    0 }, /* (176) sclp ::= */
  {  253,   -4 }, /* (177) selcollist ::= sclp distinct expr as */
  {  253,   -2 }, /* (178) selcollist ::= sclp STAR */
  {  270,   -2 }, /* (179) as ::= AS ids */
  {  270,   -1 }, /* (180) as ::= ids */
  {  270,    0 }, /* (181) as ::= */
  {  268,   -1 }, /* (182) distinct ::= DISTINCT */
  {  268,    0 }, /* (183) distinct ::= */
  {  254,   -2 }, /* (184) from ::= FROM tablelist */
  {  254,   -2 }, /* (185) from ::= FROM sub */
  {  272,   -3 }, /* (186) sub ::= LP union RP */
  {  272,   -4 }, /* (187) sub ::= LP union RP ids */
  {  272,   -6 }, /* (188) sub ::= sub COMMA LP union RP ids */
  {  271,   -2 }, /* (189) tablelist ::= ids cpxName */
  {  271,   -3 }, /* (190) tablelist ::= ids cpxName ids */
  {  271,   -4 }, /* (191) tablelist ::= tablelist COMMA ids cpxName */
  {  271,   -5 }, /* (192) tablelist ::= tablelist COMMA ids cpxName ids */
  {  273,   -1 }, /* (193) tmvar ::= VARIABLE */
  {  256,   -4 }, /* (194) interval_option ::= intervalKey LP tmvar RP */
  {  256,   -6 }, /* (195) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  256,    0 }, /* (196) interval_option ::= */
  {  274,   -1 }, /* (197) intervalKey ::= INTERVAL */
  {  274,   -1 }, /* (198) intervalKey ::= EVERY */
  {  258,    0 }, /* (199) session_option ::= */
  {  258,   -7 }, /* (200) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  259,    0 }, /* (201) windowstate_option ::= */
  {  259,   -4 }, /* (202) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  260,    0 }, /* (203) fill_opt ::= */
  {  260,   -6 }, /* (204) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  260,   -4 }, /* (205) fill_opt ::= FILL LP ID RP */
  {  257,   -4 }, /* (206) sliding_opt ::= SLIDING LP tmvar RP */
  {  257,    0 }, /* (207) sliding_opt ::= */
  {  263,    0 }, /* (208) orderby_opt ::= */
  {  263,   -3 }, /* (209) orderby_opt ::= ORDER BY sortlist */
  {  275,   -4 }, /* (210) sortlist ::= sortlist COMMA item sortorder */
  {  275,   -2 }, /* (211) sortlist ::= item sortorder */
  {  277,   -2 }, /* (212) item ::= ids cpxName */
  {  278,   -1 }, /* (213) sortorder ::= ASC */
  {  278,   -1 }, /* (214) sortorder ::= DESC */
  {  278,    0 }, /* (215) sortorder ::= */
  {  261,    0 }, /* (216) groupby_opt ::= */
  {  261,   -3 }, /* (217) groupby_opt ::= GROUP BY grouplist */
  {  279,   -3 }, /* (218) grouplist ::= grouplist COMMA item */
  {  279,   -1 }, /* (219) grouplist ::= item */
  {  262,    0 }, /* (220) having_opt ::= */
  {  262,   -2 }, /* (221) having_opt ::= HAVING expr */
  {  265,    0 }, /* (222) limit_opt ::= */
  {  265,   -2 }, /* (223) limit_opt ::= LIMIT signed */
  {  265,   -4 }, /* (224) limit_opt ::= LIMIT signed OFFSET signed */
  {  265,   -4 }, /* (225) limit_opt ::= LIMIT signed COMMA signed */
  {  264,    0 }, /* (226) slimit_opt ::= */
  {  264,   -2 }, /* (227) slimit_opt ::= SLIMIT signed */
  {  264,   -4 }, /* (228) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  264,   -4 }, /* (229) slimit_opt ::= SLIMIT signed COMMA signed */
  {  255,    0 }, /* (230) where_opt ::= */
  {  255,   -2 }, /* (231) where_opt ::= WHERE expr */
  {  269,   -3 }, /* (232) expr ::= LP expr RP */
  {  269,   -1 }, /* (233) expr ::= ID */
  {  269,   -3 }, /* (234) expr ::= ID DOT ID */
  {  269,   -3 }, /* (235) expr ::= ID DOT STAR */
  {  269,   -1 }, /* (236) expr ::= INTEGER */
  {  269,   -2 }, /* (237) expr ::= MINUS INTEGER */
  {  269,   -2 }, /* (238) expr ::= PLUS INTEGER */
  {  269,   -1 }, /* (239) expr ::= FLOAT */
  {  269,   -2 }, /* (240) expr ::= MINUS FLOAT */
  {  269,   -2 }, /* (241) expr ::= PLUS FLOAT */
  {  269,   -1 }, /* (242) expr ::= STRING */
  {  269,   -1 }, /* (243) expr ::= NOW */
  {  269,   -1 }, /* (244) expr ::= VARIABLE */
  {  269,   -2 }, /* (245) expr ::= PLUS VARIABLE */
  {  269,   -2 }, /* (246) expr ::= MINUS VARIABLE */
  {  269,   -1 }, /* (247) expr ::= BOOL */
  {  269,   -1 }, /* (248) expr ::= NULL */
  {  269,   -4 }, /* (249) expr ::= ID LP exprlist RP */
  {  269,   -4 }, /* (250) expr ::= ID LP STAR RP */
  {  269,   -3 }, /* (251) expr ::= expr IS NULL */
  {  269,   -4 }, /* (252) expr ::= expr IS NOT NULL */
  {  269,   -3 }, /* (253) expr ::= expr LT expr */
  {  269,   -3 }, /* (254) expr ::= expr GT expr */
  {  269,   -3 }, /* (255) expr ::= expr LE expr */
  {  269,   -3 }, /* (256) expr ::= expr GE expr */
  {  269,   -3 }, /* (257) expr ::= expr NE expr */
  {  269,   -3 }, /* (258) expr ::= expr EQ expr */
  {  269,   -5 }, /* (259) expr ::= expr BETWEEN expr AND expr */
  {  269,   -3 }, /* (260) expr ::= expr AND expr */
  {  269,   -3 }, /* (261) expr ::= expr OR expr */
  {  269,   -3 }, /* (262) expr ::= expr PLUS expr */
  {  269,   -3 }, /* (263) expr ::= expr MINUS expr */
  {  269,   -3 }, /* (264) expr ::= expr STAR expr */
  {  269,   -3 }, /* (265) expr ::= expr SLASH expr */
  {  269,   -3 }, /* (266) expr ::= expr REM expr */
  {  269,   -3 }, /* (267) expr ::= expr LIKE expr */
  {  269,   -3 }, /* (268) expr ::= expr MATCH expr */
  {  269,   -3 }, /* (269) expr ::= expr NMATCH expr */
  {  269,   -5 }, /* (270) expr ::= expr IN LP exprlist RP */
  {  208,   -3 }, /* (271) exprlist ::= exprlist COMMA expritem */
  {  208,   -1 }, /* (272) exprlist ::= expritem */
  {  280,   -1 }, /* (273) expritem ::= expr */
  {  280,    0 }, /* (274) expritem ::= */
  {  200,   -3 }, /* (275) cmd ::= RESET QUERY CACHE */
  {  200,   -3 }, /* (276) cmd ::= SYNCDB ids REPLICA */
  {  200,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (278) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (279) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (281) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (282) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (283) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (286) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (287) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (289) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (290) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (291) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -3 }, /* (293) cmd ::= KILL CONNECTION INTEGER */
  {  200,   -5 }, /* (294) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  200,   -5 }, /* (295) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 141: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==141);
      case 142: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==142);
      case 143: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==143);
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy47);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy247);}
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
      case 59: /* needts ::= */ yytestcase(yyruleno==59);
      case 183: /* distinct ::= */ yytestcase(yyruleno==183);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 58: /* needts ::= NEEDTS */
{ yymsp[0].minor.yy0.n = 1;}
        break;
      case 60: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 62: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 63: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==63);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
        break;
      case 64: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 1);}
        break;
      case 65: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize needts */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 2);}
        break;
      case 66: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 67: /* bufsize ::= */
      case 69: /* pps ::= */ yytestcase(yyruleno==69);
      case 71: /* tseries ::= */ yytestcase(yyruleno==71);
      case 73: /* dbs ::= */ yytestcase(yyruleno==73);
      case 75: /* streams ::= */ yytestcase(yyruleno==75);
      case 77: /* storage ::= */ yytestcase(yyruleno==77);
      case 79: /* qtime ::= */ yytestcase(yyruleno==79);
      case 81: /* users ::= */ yytestcase(yyruleno==81);
      case 83: /* conns ::= */ yytestcase(yyruleno==83);
      case 85: /* state ::= */ yytestcase(yyruleno==85);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 68: /* bufsize ::= BUFSIZE INTEGER */
      case 70: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==70);
      case 72: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==72);
      case 74: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==76);
      case 78: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==78);
      case 80: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==80);
      case 82: /* users ::= USERS INTEGER */ yytestcase(yyruleno==82);
      case 84: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==84);
      case 86: /* state ::= STATE ids */ yytestcase(yyruleno==86);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 87: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy47.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy47.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy47.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy47.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy47.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy47.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy47.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy47 = yylhsminor.yy47;
        break;
      case 88: /* intitemlist ::= intitemlist COMMA intitem */
      case 157: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==157);
{ yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);    }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 89: /* intitemlist ::= intitem */
      case 158: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 90: /* intitem ::= INTEGER */
      case 159: /* tagitem ::= INTEGER */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= FLOAT */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= STRING */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= BOOL */ yytestcase(yyruleno==162);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 91: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy247 = yymsp[0].minor.yy247; }
        break;
      case 92: /* cache ::= CACHE INTEGER */
      case 93: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==93);
      case 94: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==94);
      case 95: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==96);
      case 97: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==97);
      case 98: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==98);
      case 99: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==99);
      case 100: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==100);
      case 101: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==101);
      case 102: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==102);
      case 103: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==103);
      case 104: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==104);
      case 105: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==105);
      case 106: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==106);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 107: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy262); yymsp[1].minor.yy262.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 108: /* db_optr ::= db_optr cache */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 109: /* db_optr ::= db_optr replica */
      case 126: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==126);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 110: /* db_optr ::= db_optr quorum */
      case 127: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==127);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 111: /* db_optr ::= db_optr days */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 112: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 113: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 114: /* db_optr ::= db_optr blocks */
      case 129: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==129);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 115: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 116: /* db_optr ::= db_optr wal */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 117: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 118: /* db_optr ::= db_optr comp */
      case 130: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==130);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 119: /* db_optr ::= db_optr prec */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 120: /* db_optr ::= db_optr keep */
      case 128: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==128);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy247; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 121: /* db_optr ::= db_optr update */
      case 131: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==131);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 122: /* db_optr ::= db_optr cachelast */
      case 132: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==132);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 123: /* topic_optr ::= db_optr */
      case 133: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==133);
{ yylhsminor.yy262 = yymsp[0].minor.yy262; yylhsminor.yy262.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy262 = yylhsminor.yy262;
        break;
      case 124: /* topic_optr ::= topic_optr partitions */
      case 134: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==134);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 125: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262); yymsp[1].minor.yy262.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 135: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy179, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 136: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy403 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy403;  // negative value of name length
    tSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy179 = yylhsminor.yy179;
        break;
      case 137: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy179, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 138: /* signed ::= INTEGER */
{ yylhsminor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 139: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 140: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy403 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 144: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy336;}
        break;
      case 145: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy42);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy336 = pCreateTable;
}
  yymsp[0].minor.yy336 = yylhsminor.yy336;
        break;
      case 146: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy336->childTableInfo, &yymsp[0].minor.yy42);
  yylhsminor.yy336 = yymsp[-1].minor.yy336;
}
  yymsp[-1].minor.yy336 = yylhsminor.yy336;
        break;
      case 147: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy336 = tSetCreateTableInfo(yymsp[-1].minor.yy247, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy336 = yylhsminor.yy336;
        break;
      case 148: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy336 = tSetCreateTableInfo(yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy336 = yylhsminor.yy336;
        break;
      case 149: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy247, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy42 = yylhsminor.yy42;
        break;
      case 150: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy42 = yylhsminor.yy42;
        break;
      case 151: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy0); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 152: /* tagNamelist ::= ids */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 153: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy336 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy246, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy336 = yylhsminor.yy336;
        break;
      case 154: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy179); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 155: /* columnlist ::= column */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy179);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 156: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy179);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 163: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 164: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 165: /* tagitem ::= MINUS INTEGER */
      case 166: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==168);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 169: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy246 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy247, yymsp[-11].minor.yy46, yymsp[-10].minor.yy44, yymsp[-4].minor.yy247, yymsp[-2].minor.yy247, &yymsp[-9].minor.yy430, &yymsp[-7].minor.yy507, &yymsp[-6].minor.yy492, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy247, &yymsp[0].minor.yy204, &yymsp[-1].minor.yy204, yymsp[-3].minor.yy44);
}
  yymsp[-13].minor.yy246 = yylhsminor.yy246;
        break;
      case 170: /* select ::= LP select RP */
{yymsp[-2].minor.yy246 = yymsp[-1].minor.yy246;}
        break;
      case 171: /* union ::= select */
{ yylhsminor.yy247 = setSubclause(NULL, yymsp[0].minor.yy246); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 172: /* union ::= union UNION ALL select */
{ yylhsminor.yy247 = appendSelectClause(yymsp[-3].minor.yy247, yymsp[0].minor.yy246); }
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 173: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy247, NULL, TSDB_SQL_SELECT); }
        break;
      case 174: /* select ::= SELECT selcollist */
{
  yylhsminor.yy246 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy247, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 175: /* sclp ::= selcollist COMMA */
{yylhsminor.yy247 = yymsp[-1].minor.yy247;}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 176: /* sclp ::= */
      case 208: /* orderby_opt ::= */ yytestcase(yyruleno==208);
{yymsp[1].minor.yy247 = 0;}
        break;
      case 177: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy247 = tSqlExprListAppend(yymsp[-3].minor.yy247, yymsp[-1].minor.yy44,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 178: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy247 = tSqlExprListAppend(yymsp[-1].minor.yy247, pNode, 0, 0);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 179: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 180: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 182: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 184: /* from ::= FROM tablelist */
      case 185: /* from ::= FROM sub */ yytestcase(yyruleno==185);
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
        break;
      case 186: /* sub ::= LP union RP */
{yymsp[-2].minor.yy46 = addSubqueryElem(NULL, yymsp[-1].minor.yy247, NULL);}
        break;
      case 187: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy46 = addSubqueryElem(NULL, yymsp[-2].minor.yy247, &yymsp[0].minor.yy0);}
        break;
      case 188: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy46 = addSubqueryElem(yymsp[-5].minor.yy46, yymsp[-2].minor.yy247, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy46 = yylhsminor.yy46;
        break;
      case 189: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 190: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 191: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(yymsp[-3].minor.yy46, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 192: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(yymsp[-4].minor.yy46, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 193: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 194: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy430.interval = yymsp[-1].minor.yy0; yylhsminor.yy430.offset.n = 0; yylhsminor.yy430.token = yymsp[-3].minor.yy2;}
  yymsp[-3].minor.yy430 = yylhsminor.yy430;
        break;
      case 195: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy430.interval = yymsp[-3].minor.yy0; yylhsminor.yy430.offset = yymsp[-1].minor.yy0;   yylhsminor.yy430.token = yymsp[-5].minor.yy2;}
  yymsp[-5].minor.yy430 = yylhsminor.yy430;
        break;
      case 196: /* interval_option ::= */
{memset(&yymsp[1].minor.yy430, 0, sizeof(yymsp[1].minor.yy430));}
        break;
      case 197: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy2 = TK_INTERVAL;}
        break;
      case 198: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy2 = TK_EVERY;   }
        break;
      case 199: /* session_option ::= */
{yymsp[1].minor.yy507.col.n = 0; yymsp[1].minor.yy507.gap.n = 0;}
        break;
      case 200: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy507.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy507.gap = yymsp[-1].minor.yy0;
}
        break;
      case 201: /* windowstate_option ::= */
{ yymsp[1].minor.yy492.col.n = 0; yymsp[1].minor.yy492.col.z = NULL;}
        break;
      case 202: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy492.col = yymsp[-1].minor.yy0; }
        break;
      case 203: /* fill_opt ::= */
{ yymsp[1].minor.yy247 = 0;     }
        break;
      case 204: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy247, &A, -1, 0);
    yymsp[-5].minor.yy247 = yymsp[-1].minor.yy247;
}
        break;
      case 205: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 206: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 207: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 209: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 210: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy247 = tVariantListAppend(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 211: /* sortlist ::= item sortorder */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 212: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 213: /* sortorder ::= ASC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 214: /* sortorder ::= DESC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_DESC;}
        break;
      case 215: /* sortorder ::= */
{ yymsp[1].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 216: /* groupby_opt ::= */
{ yymsp[1].minor.yy247 = 0;}
        break;
      case 217: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 218: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 219: /* grouplist ::= item */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1);
}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 220: /* having_opt ::= */
      case 230: /* where_opt ::= */ yytestcase(yyruleno==230);
      case 274: /* expritem ::= */ yytestcase(yyruleno==274);
{yymsp[1].minor.yy44 = 0;}
        break;
      case 221: /* having_opt ::= HAVING expr */
      case 231: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==231);
{yymsp[-1].minor.yy44 = yymsp[0].minor.yy44;}
        break;
      case 222: /* limit_opt ::= */
      case 226: /* slimit_opt ::= */ yytestcase(yyruleno==226);
{yymsp[1].minor.yy204.limit = -1; yymsp[1].minor.yy204.offset = 0;}
        break;
      case 223: /* limit_opt ::= LIMIT signed */
      case 227: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==227);
{yymsp[-1].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-1].minor.yy204.offset = 0;}
        break;
      case 224: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 225: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 228: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 229: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 232: /* expr ::= LP expr RP */
{yylhsminor.yy44 = yymsp[-1].minor.yy44; yylhsminor.yy44->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy44->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 233: /* expr ::= ID */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 234: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 235: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 236: /* expr ::= INTEGER */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 237: /* expr ::= MINUS INTEGER */
      case 238: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==238);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 239: /* expr ::= FLOAT */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 240: /* expr ::= MINUS FLOAT */
      case 241: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==241);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 242: /* expr ::= STRING */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 243: /* expr ::= NOW */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 244: /* expr ::= VARIABLE */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 245: /* expr ::= PLUS VARIABLE */
      case 246: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==246);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 247: /* expr ::= BOOL */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 248: /* expr ::= NULL */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 249: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy44 = tSqlExprCreateFunction(yymsp[-1].minor.yy247, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 250: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy44 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 251: /* expr ::= expr IS NULL */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 252: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-3].minor.yy44, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 253: /* expr ::= expr LT expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LT);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 254: /* expr ::= expr GT expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_GT);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 255: /* expr ::= expr LE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 256: /* expr ::= expr GE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_GE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 257: /* expr ::= expr NE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_NE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 258: /* expr ::= expr EQ expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_EQ);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 259: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy44); yylhsminor.yy44 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy44, yymsp[-2].minor.yy44, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy44, TK_LE), TK_AND);}
  yymsp[-4].minor.yy44 = yylhsminor.yy44;
        break;
      case 260: /* expr ::= expr AND expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_AND);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 261: /* expr ::= expr OR expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_OR); }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 262: /* expr ::= expr PLUS expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_PLUS);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 263: /* expr ::= expr MINUS expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_MINUS); }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 264: /* expr ::= expr STAR expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_STAR);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 265: /* expr ::= expr SLASH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_DIVIDE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 266: /* expr ::= expr REM expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_REM);   }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 267: /* expr ::= expr LIKE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LIKE);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 268: /* expr ::= expr MATCH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_MATCH);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 269: /* expr ::= expr NMATCH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_NMATCH);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 270: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-4].minor.yy44, (tSqlExpr*)yymsp[-1].minor.yy247, TK_IN); }
  yymsp[-4].minor.yy44 = yylhsminor.yy44;
        break;
      case 271: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy247 = tSqlExprListAppend(yymsp[-2].minor.yy247,yymsp[0].minor.yy44,0, 0);}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 272: /* exprlist ::= expritem */
{yylhsminor.yy247 = tSqlExprListAppend(0,yymsp[0].minor.yy44,0, 0);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 273: /* expritem ::= expr */
{yylhsminor.yy44 = yymsp[0].minor.yy44;}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 275: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 276: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 283: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 291: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 294: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 295: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
