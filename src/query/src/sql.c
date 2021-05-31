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
#include "tstoken.h"
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
#define YYNOCODE 265
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy96;
  SRelationInfo* yy148;
  tSqlExpr* yy178;
  SCreateAcctInfo yy187;
  SArray* yy285;
  TAOS_FIELD yy295;
  SSqlNode* yy344;
  tVariant yy362;
  SIntervalVal yy376;
  SLimitVal yy438;
  int yy460;
  SCreateTableSql* yy470;
  SSessionWindowVal yy523;
  int64_t yy525;
  SCreateDbInfo yy526;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             322
#define YYNRULE              271
#define YYNTOKEN             188
#define YY_MAX_SHIFT         321
#define YY_MIN_SHIFTREDUCE   517
#define YY_MAX_SHIFTREDUCE   787
#define YY_ERROR_ACTION      788
#define YY_ACCEPT_ACTION     789
#define YY_NO_ACTION         790
#define YY_MIN_REDUCE        791
#define YY_MAX_REDUCE        1061
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
#define YY_ACTTAB_COUNT (695)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   951,  566,  933,  223,  207,  319,  222,  227,   18,  567,
 /*    10 */   212,  566,  960,   49,   50,  210,   53,   54, 1053,  567,
 /*    20 */   226,   43,  185,   52,  281,   57,   55,   58,   56,   81,
 /*    30 */   837, 1042,  182,   48,   47,  168,  939,   46,   45,   44,
 /*    40 */    49,   50,   72,   53,   54,  219,  135,  226,   43,  566,
 /*    50 */    52,  281,   57,   55,   58,   56,  645,  567,  183,  185,
 /*    60 */    48,   47,  185,  185,   46,   45,   44,   50, 1043,   53,
 /*    70 */    54, 1043, 1043,  226,   43,  957,   52,  281,   57,   55,
 /*    80 */    58,   56,  789,  321,  295,  294,   48,   47,  142,   31,
 /*    90 */    46,   45,   44,  518,  519,  520,  521,  522,  523,  524,
 /*   100 */   525,  526,  527,  528,  529,  530,  320,   86,  731,  208,
 /*   110 */   251,   71,   49,   50,  142,   53,   54,    1,  156,  226,
 /*   120 */    43,  231,   52,  281,   57,   55,   58,   56,  279,  951,
 /*   130 */   228,  209,   48,   47,  936,  218,   46,   45,   44,   49,
 /*   140 */    51,  927,   53,   54,  246,  142,  226,   43,  681,   52,
 /*   150 */   281,   57,   55,   58,   56,   25,  992,  937,  261,   48,
 /*   160 */    47,  939,  925,   46,   45,   44,   24,  277,  314,  313,
 /*   170 */   276,  275,  274,  312,  273,  311,  310,  309,  272,  308,
 /*   180 */   307,  899,  991,  887,  888,  889,  890,  891,  892,  893,
 /*   190 */   894,  895,  896,  897,  898,  900,  901,   53,   54,   19,
 /*   200 */   229,  226,   43,  287,   52,  281,   57,   55,   58,   56,
 /*   210 */   220,  263,  684,   79,   48,   47,  193,   31,   46,   45,
 /*   220 */    44,   48,   47,  194,  305,   46,   45,   44,  119,  118,
 /*   230 */   192,  566,  142,   83,  286,   75,  939,  225,  746,  567,
 /*   240 */   188,  735,   31,  738,  279,  741,  907,  225,  746,  905,
 /*   250 */   906,  735,   62,  738,  908,  741,  910,  911,  909,  216,
 /*   260 */   912,  913,  936, 1039,  922,  923,   30,  926,   37,  204,
 /*   270 */   205,   75,   31,  282,   63,   24,  847,  314,  313,  204,
 /*   280 */   205,  168,  312,  230,  311,  310,  309,  935,  308,  307,
 /*   290 */   245,  688,   69,   31,   57,   55,   58,   56,  200,   13,
 /*   300 */    80, 1038,   48,   47,   37,  315,   46,   45,   44, 1037,
 /*   310 */    26,  104,   98,  109,  217,  238,   59,  936,  108,  114,
 /*   320 */   117,  107,  283,  242,  241,  202,   59,  111,   85,  669,
 /*   330 */    82,  939,  666,  106,  667,  288,  668,   31,  936,   31,
 /*   340 */   247,  305,    5,   34,  158,   31,    3,  169,  747,  157,
 /*   350 */    93,   88,   92,  232,  743,  938,  292,  291,  747,   70,
 /*   360 */   233,  234,  203,  838,  743,  176,  174,  172,  168,   32,
 /*   370 */   742,  691,  171,  122,  121,  120,   46,   45,   44,  289,
 /*   380 */   742,  293,  936,  737,  936,  740,  736,  297,  739,  697,
 /*   390 */   936,  924,  703,  318,  317,  127,  704,  133,  131,  130,
 /*   400 */   655,  712,  713,   65,  249,  266,  657,  224,  733,   29,
 /*   410 */   186,  268,  656,  767,  748,  187, 1002,  565,  137,  750,
 /*   420 */    68,   61,  189,   66,  673,   21,  674,    6,  184,   32,
 /*   430 */    97,   96,  190,  243,   32,   61,  671,  269,  672,  670,
 /*   440 */    84,   61,   20,   20,  734, 1001,   20,  644,   15,   14,
 /*   450 */   191,  103,  102,   17,   16,  116,  115,  196,  197,  198,
 /*   460 */   195,  181,  214,  998,  997,  134,  215,  296,  952,  959,
 /*   470 */    40,  967,  984,  969,  136,  250,  140,  153,  934,  983,
 /*   480 */   152,  932,  132,  252,  154,  211,  155,  850,  271,   38,
 /*   490 */   179,   35,  280,  846, 1058,  696,  744,   94, 1057,  745,
 /*   500 */   254,  259, 1055,   67,  949,  159,   64,  143,  290, 1052,
 /*   510 */   100,   42, 1051, 1049,  160,  868,  144,  264,   36,  145,
 /*   520 */   262,  146,  260,  147,  258,  256,   33,  148,  253,   39,
 /*   530 */   180,  149,   41,  834,  110,  832,  112,  113,  830,  306,
 /*   540 */   829,  235,  170,  827,  826,  825,  824,  823,  822,  173,
 /*   550 */   175,  819,  817,  815,  813,  177,  810,  178,  105,  248,
 /*   560 */    73,   76,  255,  985,  298,  201,  299,  300,  301,  221,
 /*   570 */   303,  302,  304,  270,  316,  787,  236,  237,  786,  206,
 /*   580 */   199,  239,  240,   89,   90,  785,  773,  772,  244,    9,
 /*   590 */   249,  828,  265,   74,  676,  163,  123,  162,  869,  161,
 /*   600 */   124,  165,  164,  166,  167,  821,    4,  125,  820,  903,
 /*   610 */   126,  812,   77,  150,  151,  698,  811,    2,  138,  915,
 /*   620 */   701,  139,   78,  213,  257,   27,  705,  141,   10,   28,
 /*   630 */    11,   12,   22,  267,   23,   85,   87,  608,  604,  602,
 /*   640 */   601,  600,  597,   91,  570,  278,    7,  751,  284,  749,
 /*   650 */   285,    8,   95,   32,   60,  647,  646,  643,   99,  592,
 /*   660 */   101,  590,  582,  588,  584,  586,  580,  578,  611,  610,
 /*   670 */   609,  607,  606,  605,  603,  599,  598,  568,  534,   61,
 /*   680 */   532,  791,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   690 */   790,  790,  790,  128,  129,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   236,    1,  192,  198,  191,  192,  198,  198,  254,    9,
 /*    10 */   212,    1,  192,   13,   14,  251,   16,   17,  238,    9,
 /*    20 */    20,   21,  254,   23,   24,   25,   26,   27,   28,  239,
 /*    30 */   197,  263,  254,   33,   34,  202,  238,   37,   38,   39,
 /*    40 */    13,   14,  252,   16,   17,  235,  192,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,    5,    9,  254,  254,
 /*    60 */    33,   34,  254,  254,   37,   38,   39,   14,  263,   16,
 /*    70 */    17,  263,  263,   20,   21,  255,   23,   24,   25,   26,
 /*    80 */    27,   28,  189,  190,   33,   34,   33,   34,  192,  192,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  199,   81,   61,
 /*   110 */   256,  111,   13,   14,  192,   16,   17,  200,  201,   20,
 /*   120 */    21,  192,   23,   24,   25,   26,   27,   28,   82,  236,
 /*   130 */    68,  234,   33,   34,  237,  212,   37,   38,   39,   13,
 /*   140 */    14,  233,   16,   17,  251,  192,   20,   21,  110,   23,
 /*   150 */    24,   25,   26,   27,   28,  117,  260,  228,  262,   33,
 /*   160 */    34,  238,    0,   37,   38,   39,   91,   92,   93,   94,
 /*   170 */    95,   96,   97,   98,   99,  100,  101,  102,  103,  104,
 /*   180 */   105,  211,  260,  213,  214,  215,  216,  217,  218,  219,
 /*   190 */   220,  221,  222,  223,  224,  225,  226,   16,   17,   44,
 /*   200 */   138,   20,   21,  141,   23,   24,   25,   26,   27,   28,
 /*   210 */   212,  258,   37,  260,   33,   34,   61,  192,   37,   38,
 /*   220 */    39,   33,   34,   68,   84,   37,   38,   39,   73,   74,
 /*   230 */    75,    1,  192,  199,   79,   80,  238,    1,    2,    9,
 /*   240 */   254,    5,  192,    7,   82,    9,  211,    1,    2,  214,
 /*   250 */   215,    5,  110,    7,  219,    9,  221,  222,  223,  234,
 /*   260 */   225,  226,  237,  254,  230,  231,  232,  233,  113,   33,
 /*   270 */    34,   80,  192,   37,  132,   91,  197,   93,   94,   33,
 /*   280 */    34,  202,   98,   68,  100,  101,  102,  237,  104,  105,
 /*   290 */   135,  116,  137,  192,   25,   26,   27,   28,  143,   80,
 /*   300 */   260,  254,   33,   34,  113,  212,   37,   38,   39,  254,
 /*   310 */    80,   62,   63,   64,  234,  136,   80,  237,   69,   70,
 /*   320 */    71,   72,   15,  144,  145,  254,   80,   78,  109,    2,
 /*   330 */   111,  238,    5,   76,    7,  234,    9,  192,  237,  192,
 /*   340 */    81,   84,   62,   63,   64,  192,  195,  196,  112,   69,
 /*   350 */    70,   71,   72,  138,  118,  238,  141,  142,  112,  199,
 /*   360 */    33,   34,  254,  197,  118,   62,   63,   64,  202,  110,
 /*   370 */   134,   81,   69,   70,   71,   72,   37,   38,   39,  234,
 /*   380 */   134,  234,  237,    5,  237,    7,    5,  234,    7,   81,
 /*   390 */   237,  231,   81,   65,   66,   67,   81,   62,   63,   64,
 /*   400 */    81,  125,  126,  110,  114,   81,   81,   60,    1,   80,
 /*   410 */   254,   81,   81,   81,   81,  254,  229,   81,  110,  112,
 /*   420 */    80,  110,  254,  130,    5,  110,    7,   80,  254,  110,
 /*   430 */   139,  140,  254,  192,  110,  110,    5,  108,    7,  112,
 /*   440 */   110,  110,  110,  110,   37,  229,  110,  107,  139,  140,
 /*   450 */   254,  139,  140,  139,  140,   76,   77,  254,  254,  254,
 /*   460 */   254,  254,  229,  229,  229,  192,  229,  229,  236,  192,
 /*   470 */   253,  192,  261,  192,  192,  236,  192,  192,  236,  261,
 /*   480 */   240,  192,   60,  257,  192,  257,  192,  192,  192,  192,
 /*   490 */   192,  192,  192,  192,  192,  118,  118,  192,  192,  118,
 /*   500 */   257,  257,  192,  129,  250,  192,  131,  249,  192,  192,
 /*   510 */   192,  128,  192,  192,  192,  192,  248,  123,  192,  247,
 /*   520 */   127,  246,  122,  245,  121,  120,  192,  244,  119,  192,
 /*   530 */   192,  243,  133,  192,  192,  192,  192,  192,  192,  106,
 /*   540 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   550 */   192,  192,  192,  192,  192,  192,  192,  192,   90,  193,
 /*   560 */   193,  193,  193,  193,   89,  193,   50,   86,   88,  193,
 /*   570 */    87,   54,   85,  193,   82,    5,  146,    5,    5,  193,
 /*   580 */   193,  146,    5,  199,  199,    5,   93,   92,  136,   80,
 /*   590 */   114,  193,  108,  115,   81,  204,  194,  208,  210,  209,
 /*   600 */   194,  205,  207,  206,  203,  193,  195,  194,  193,  227,
 /*   610 */   194,  193,  110,  242,  241,   81,  193,  200,   80,  227,
 /*   620 */    81,  110,   80,    1,   80,  110,   81,   80,  124,  110,
 /*   630 */   124,   80,   80,  108,   80,  109,   76,    9,    5,    5,
 /*   640 */     5,    5,    5,   76,   83,   15,   80,  112,   24,   81,
 /*   650 */    58,   80,  140,  110,   16,    5,    5,   81,  140,    5,
 /*   660 */   140,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   670 */     5,    5,    5,    5,    5,    5,    5,   83,   60,  110,
 /*   680 */    59,    0,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   690 */   264,  264,  264,   21,   21,  264,  264,  264,  264,  264,
 /*   700 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   710 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   720 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   730 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   740 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   750 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   760 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   770 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   780 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   790 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   800 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   810 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   820 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   830 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   840 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   850 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   860 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   870 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   880 */   264,  264,  264,
};
#define YY_SHIFT_COUNT    (321)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (681)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   155,   75,   75,  184,  184,   46,  236,  246,  246,   10,
 /*    10 */    10,   10,   10,   10,   10,   10,   10,   10,    0,   48,
 /*    20 */   246,  327,  327,  327,  327,  230,  191,   10,   10,   10,
 /*    30 */   162,   10,   10,  257,   46,  140,  140,  695,  695,  695,
 /*    40 */   246,  246,  246,  246,  246,  246,  246,  246,  246,  246,
 /*    50 */   246,  246,  246,  246,  246,  246,  246,  246,  246,  246,
 /*    60 */   327,  327,   51,   51,   51,   51,   51,   51,   51,   10,
 /*    70 */    10,   10,  175,   10,  191,  191,   10,   10,   10,  276,
 /*    80 */   276,   38,  191,   10,   10,   10,   10,   10,   10,   10,
 /*    90 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   100 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   110 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   120 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   130 */    10,   10,   10,   10,  422,  422,  422,  377,  377,  377,
 /*   140 */   422,  377,  422,  374,  375,  383,  394,  393,  400,  403,
 /*   150 */   405,  409,  399,  422,  422,  422,  433,   46,   46,  422,
 /*   160 */   422,  468,  475,  516,  481,  480,  517,  483,  487,  433,
 /*   170 */   422,  492,  492,  422,  492,  422,  492,  422,  422,  695,
 /*   180 */   695,   27,   99,   99,  126,   99,   53,  181,  269,  269,
 /*   190 */   269,  269,  249,  280,  303,  188,  188,  188,  188,  215,
 /*   200 */   179,  219,  339,  339,  378,  381,   62,  328,  335,  259,
 /*   210 */   290,  308,  311,  315,  142,  293,  319,  324,  325,  330,
 /*   220 */   331,  329,  332,  333,  407,  347,  307,  336,  291,  309,
 /*   230 */   312,  340,  314,  419,  431,  379,  570,  430,  572,  573,
 /*   240 */   435,  577,  580,  493,  495,  452,  476,  484,  509,  478,
 /*   250 */   513,  502,  534,  538,  539,  511,  542,  622,  544,  545,
 /*   260 */   547,  515,  504,  519,  506,  551,  484,  552,  525,  554,
 /*   270 */   526,  560,  628,  633,  634,  635,  636,  637,  561,  630,
 /*   280 */   567,  566,  568,  535,  571,  624,  592,  512,  543,  543,
 /*   290 */   638,  518,  520,  543,  650,  651,  576,  543,  654,  656,
 /*   300 */   657,  658,  659,  660,  661,  662,  663,  664,  665,  666,
 /*   310 */   667,  668,  669,  670,  671,  569,  594,  672,  673,  618,
 /*   320 */   621,  681,
};
#define YY_REDUCE_COUNT (180)
#define YY_REDUCE_MIN   (-246)
#define YY_REDUCE_MAX   (423)
static const short yy_reduce_ofst[] = {
 /*     0 */  -107,  -30,  -30,   35,   35,   34, -195, -192, -191, -103,
 /*    10 */  -104,  -47,   25,   80,  101,  145,  147,  153, -180, -187,
 /*    20 */  -232, -202,  -77,   -2,   93, -146, -236,  -78,   40, -190,
 /*    30 */   -92,  -71,   50, -167,  160,   79,  166, -210,  -83,  151,
 /*    40 */  -246, -222, -196,  -14,    9,   47,   55,   71,  108,  156,
 /*    50 */   161,  168,  174,  178,  196,  203,  204,  205,  206,  207,
 /*    60 */  -220,  117,  187,  216,  233,  234,  235,  237,  238,  241,
 /*    70 */   273,  277,  217,  279,  232,  239,  281,  282,  284,  211,
 /*    80 */   218,  240,  242,  285,  289,  292,  294,  295,  296,  297,
 /*    90 */   298,  299,  300,  301,  302,  305,  306,  310,  313,  316,
 /*   100 */   317,  318,  320,  321,  322,  323,  326,  334,  337,  338,
 /*   110 */   341,  342,  343,  344,  345,  346,  348,  349,  350,  351,
 /*   120 */   352,  353,  354,  355,  356,  357,  358,  359,  360,  361,
 /*   130 */   362,  363,  364,  365,  366,  367,  368,  226,  228,  243,
 /*   140 */   369,  244,  370,  254,  258,  268,  272,  275,  278,  283,
 /*   150 */   288,  371,  373,  372,  376,  380,  382,  384,  385,  386,
 /*   160 */   387,  388,  390,  389,  391,  395,  396,  397,  401,  392,
 /*   170 */   398,  402,  406,  412,  413,  415,  416,  418,  423,  417,
 /*   180 */   411,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   788,  902,  848,  914,  835,  845, 1045, 1045, 1045,  788,
 /*    10 */   788,  788,  788,  788,  788,  788,  788,  788,  961,  807,
 /*    20 */  1045,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    30 */   845,  788,  788,  851,  845,  851,  851,  956,  886,  904,
 /*    40 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    50 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    60 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    70 */   788,  788,  963,  966,  788,  788,  968,  788,  788,  988,
 /*    80 */   988,  954,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    90 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   100 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   110 */   833,  788,  831,  788,  788,  788,  788,  788,  788,  788,
 /*   120 */   788,  788,  788,  788,  788,  788,  788,  818,  788,  788,
 /*   130 */   788,  788,  788,  788,  809,  809,  809,  788,  788,  788,
 /*   140 */   809,  788,  809,  995,  999,  993,  981,  989,  980,  976,
 /*   150 */   974,  973, 1003,  809,  809,  809,  849,  845,  845,  809,
 /*   160 */   809,  867,  865,  863,  855,  861,  857,  859,  853,  836,
 /*   170 */   809,  843,  843,  809,  843,  809,  843,  809,  809,  886,
 /*   180 */   904,  788, 1004,  994,  788, 1044, 1034, 1033, 1040, 1032,
 /*   190 */  1031, 1030,  788,  788,  788, 1026, 1029, 1028, 1027,  788,
 /*   200 */   788,  788, 1036, 1035,  788,  788,  788,  788,  788,  788,
 /*   210 */   788,  788,  788,  788, 1000,  996,  788,  788,  788,  788,
 /*   220 */   788,  788,  788,  788,  788, 1006,  788,  788,  788,  788,
 /*   230 */   788,  916,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   240 */   788,  788,  788,  788,  788,  788,  953,  788,  788,  788,
 /*   250 */   788,  964,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   260 */   788,  990,  788,  982,  788,  788,  928,  788,  788,  788,
 /*   270 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   280 */   788,  788,  788,  788,  788,  788,  788,  788, 1056, 1054,
 /*   290 */   788,  788,  788, 1050,  788,  788,  788, 1048,  788,  788,
 /*   300 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   310 */   788,  788,  788,  788,  788,  870,  788,  816,  814,  788,
 /*   320 */   805,  788,
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
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
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
    0,  /*      COMMA => nothing */
    0,  /*         AS => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
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
    1,  /*        NOW => ID */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
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
  /*   47 */ "MNODES",
  /*   48 */ "DNODES",
  /*   49 */ "ACCOUNTS",
  /*   50 */ "USERS",
  /*   51 */ "MODULES",
  /*   52 */ "QUERIES",
  /*   53 */ "CONNECTIONS",
  /*   54 */ "STREAMS",
  /*   55 */ "VARIABLES",
  /*   56 */ "SCORES",
  /*   57 */ "GRANTS",
  /*   58 */ "VNODES",
  /*   59 */ "IPTOKEN",
  /*   60 */ "DOT",
  /*   61 */ "CREATE",
  /*   62 */ "TABLE",
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "TOPIC",
  /*   70 */ "DNODE",
  /*   71 */ "USER",
  /*   72 */ "ACCOUNT",
  /*   73 */ "USE",
  /*   74 */ "DESCRIBE",
  /*   75 */ "ALTER",
  /*   76 */ "PASS",
  /*   77 */ "PRIVILEGE",
  /*   78 */ "LOCAL",
  /*   79 */ "COMPACT",
  /*   80 */ "LP",
  /*   81 */ "RP",
  /*   82 */ "IF",
  /*   83 */ "EXISTS",
  /*   84 */ "PPS",
  /*   85 */ "TSERIES",
  /*   86 */ "DBS",
  /*   87 */ "STORAGE",
  /*   88 */ "QTIME",
  /*   89 */ "CONNS",
  /*   90 */ "STATE",
  /*   91 */ "KEEP",
  /*   92 */ "CACHE",
  /*   93 */ "REPLICA",
  /*   94 */ "QUORUM",
  /*   95 */ "DAYS",
  /*   96 */ "MINROWS",
  /*   97 */ "MAXROWS",
  /*   98 */ "BLOCKS",
  /*   99 */ "CTIME",
  /*  100 */ "WAL",
  /*  101 */ "FSYNC",
  /*  102 */ "COMP",
  /*  103 */ "PRECISION",
  /*  104 */ "UPDATE",
  /*  105 */ "CACHELAST",
  /*  106 */ "PARTITIONS",
  /*  107 */ "UNSIGNED",
  /*  108 */ "TAGS",
  /*  109 */ "USING",
  /*  110 */ "COMMA",
  /*  111 */ "AS",
  /*  112 */ "NULL",
  /*  113 */ "SELECT",
  /*  114 */ "UNION",
  /*  115 */ "ALL",
  /*  116 */ "DISTINCT",
  /*  117 */ "FROM",
  /*  118 */ "VARIABLE",
  /*  119 */ "INTERVAL",
  /*  120 */ "SESSION",
  /*  121 */ "FILL",
  /*  122 */ "SLIDING",
  /*  123 */ "ORDER",
  /*  124 */ "BY",
  /*  125 */ "ASC",
  /*  126 */ "DESC",
  /*  127 */ "GROUP",
  /*  128 */ "HAVING",
  /*  129 */ "LIMIT",
  /*  130 */ "OFFSET",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "WHERE",
  /*  134 */ "NOW",
  /*  135 */ "RESET",
  /*  136 */ "QUERY",
  /*  137 */ "SYNCDB",
  /*  138 */ "ADD",
  /*  139 */ "COLUMN",
  /*  140 */ "TAG",
  /*  141 */ "CHANGE",
  /*  142 */ "SET",
  /*  143 */ "KILL",
  /*  144 */ "CONNECTION",
  /*  145 */ "STREAM",
  /*  146 */ "COLON",
  /*  147 */ "ABORT",
  /*  148 */ "AFTER",
  /*  149 */ "ATTACH",
  /*  150 */ "BEFORE",
  /*  151 */ "BEGIN",
  /*  152 */ "CASCADE",
  /*  153 */ "CLUSTER",
  /*  154 */ "CONFLICT",
  /*  155 */ "COPY",
  /*  156 */ "DEFERRED",
  /*  157 */ "DELIMITERS",
  /*  158 */ "DETACH",
  /*  159 */ "EACH",
  /*  160 */ "END",
  /*  161 */ "EXPLAIN",
  /*  162 */ "FAIL",
  /*  163 */ "FOR",
  /*  164 */ "IGNORE",
  /*  165 */ "IMMEDIATE",
  /*  166 */ "INITIALLY",
  /*  167 */ "INSTEAD",
  /*  168 */ "MATCH",
  /*  169 */ "KEY",
  /*  170 */ "OF",
  /*  171 */ "RAISE",
  /*  172 */ "REPLACE",
  /*  173 */ "RESTRICT",
  /*  174 */ "ROW",
  /*  175 */ "STATEMENT",
  /*  176 */ "TRIGGER",
  /*  177 */ "VIEW",
  /*  178 */ "SEMI",
  /*  179 */ "NONE",
  /*  180 */ "PREV",
  /*  181 */ "LINEAR",
  /*  182 */ "IMPORT",
  /*  183 */ "TBNAME",
  /*  184 */ "JOIN",
  /*  185 */ "INSERT",
  /*  186 */ "INTO",
  /*  187 */ "VALUES",
  /*  188 */ "error",
  /*  189 */ "program",
  /*  190 */ "cmd",
  /*  191 */ "dbPrefix",
  /*  192 */ "ids",
  /*  193 */ "cpxName",
  /*  194 */ "ifexists",
  /*  195 */ "alter_db_optr",
  /*  196 */ "alter_topic_optr",
  /*  197 */ "acct_optr",
  /*  198 */ "exprlist",
  /*  199 */ "ifnotexists",
  /*  200 */ "db_optr",
  /*  201 */ "topic_optr",
  /*  202 */ "pps",
  /*  203 */ "tseries",
  /*  204 */ "dbs",
  /*  205 */ "streams",
  /*  206 */ "storage",
  /*  207 */ "qtime",
  /*  208 */ "users",
  /*  209 */ "conns",
  /*  210 */ "state",
  /*  211 */ "keep",
  /*  212 */ "tagitemlist",
  /*  213 */ "cache",
  /*  214 */ "replica",
  /*  215 */ "quorum",
  /*  216 */ "days",
  /*  217 */ "minrows",
  /*  218 */ "maxrows",
  /*  219 */ "blocks",
  /*  220 */ "ctime",
  /*  221 */ "wal",
  /*  222 */ "fsync",
  /*  223 */ "comp",
  /*  224 */ "prec",
  /*  225 */ "update",
  /*  226 */ "cachelast",
  /*  227 */ "partitions",
  /*  228 */ "typename",
  /*  229 */ "signed",
  /*  230 */ "create_table_args",
  /*  231 */ "create_stable_args",
  /*  232 */ "create_table_list",
  /*  233 */ "create_from_stable",
  /*  234 */ "columnlist",
  /*  235 */ "tagNamelist",
  /*  236 */ "select",
  /*  237 */ "column",
  /*  238 */ "tagitem",
  /*  239 */ "selcollist",
  /*  240 */ "from",
  /*  241 */ "where_opt",
  /*  242 */ "interval_opt",
  /*  243 */ "session_option",
  /*  244 */ "fill_opt",
  /*  245 */ "sliding_opt",
  /*  246 */ "groupby_opt",
  /*  247 */ "orderby_opt",
  /*  248 */ "having_opt",
  /*  249 */ "slimit_opt",
  /*  250 */ "limit_opt",
  /*  251 */ "union",
  /*  252 */ "sclp",
  /*  253 */ "distinct",
  /*  254 */ "expr",
  /*  255 */ "as",
  /*  256 */ "tablelist",
  /*  257 */ "tmvar",
  /*  258 */ "sortlist",
  /*  259 */ "sortitem",
  /*  260 */ "item",
  /*  261 */ "sortorder",
  /*  262 */ "grouplist",
  /*  263 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW MNODES",
 /*   4 */ "cmd ::= SHOW DNODES",
 /*   5 */ "cmd ::= SHOW ACCOUNTS",
 /*   6 */ "cmd ::= SHOW USERS",
 /*   7 */ "cmd ::= SHOW MODULES",
 /*   8 */ "cmd ::= SHOW QUERIES",
 /*   9 */ "cmd ::= SHOW CONNECTIONS",
 /*  10 */ "cmd ::= SHOW STREAMS",
 /*  11 */ "cmd ::= SHOW VARIABLES",
 /*  12 */ "cmd ::= SHOW SCORES",
 /*  13 */ "cmd ::= SHOW GRANTS",
 /*  14 */ "cmd ::= SHOW VNODES",
 /*  15 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  16 */ "dbPrefix ::=",
 /*  17 */ "dbPrefix ::= ids DOT",
 /*  18 */ "cpxName ::=",
 /*  19 */ "cpxName ::= DOT ids",
 /*  20 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  22 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  29 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  32 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  49 */ "ids ::= ID",
 /*  50 */ "ids ::= STRING",
 /*  51 */ "ifexists ::= IF EXISTS",
 /*  52 */ "ifexists ::=",
 /*  53 */ "ifnotexists ::= IF NOT EXISTS",
 /*  54 */ "ifnotexists ::=",
 /*  55 */ "cmd ::= CREATE DNODE ids",
 /*  56 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  57 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  58 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  59 */ "cmd ::= CREATE USER ids PASS ids",
 /*  60 */ "pps ::=",
 /*  61 */ "pps ::= PPS INTEGER",
 /*  62 */ "tseries ::=",
 /*  63 */ "tseries ::= TSERIES INTEGER",
 /*  64 */ "dbs ::=",
 /*  65 */ "dbs ::= DBS INTEGER",
 /*  66 */ "streams ::=",
 /*  67 */ "streams ::= STREAMS INTEGER",
 /*  68 */ "storage ::=",
 /*  69 */ "storage ::= STORAGE INTEGER",
 /*  70 */ "qtime ::=",
 /*  71 */ "qtime ::= QTIME INTEGER",
 /*  72 */ "users ::=",
 /*  73 */ "users ::= USERS INTEGER",
 /*  74 */ "conns ::=",
 /*  75 */ "conns ::= CONNS INTEGER",
 /*  76 */ "state ::=",
 /*  77 */ "state ::= STATE ids",
 /*  78 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  79 */ "keep ::= KEEP tagitemlist",
 /*  80 */ "cache ::= CACHE INTEGER",
 /*  81 */ "replica ::= REPLICA INTEGER",
 /*  82 */ "quorum ::= QUORUM INTEGER",
 /*  83 */ "days ::= DAYS INTEGER",
 /*  84 */ "minrows ::= MINROWS INTEGER",
 /*  85 */ "maxrows ::= MAXROWS INTEGER",
 /*  86 */ "blocks ::= BLOCKS INTEGER",
 /*  87 */ "ctime ::= CTIME INTEGER",
 /*  88 */ "wal ::= WAL INTEGER",
 /*  89 */ "fsync ::= FSYNC INTEGER",
 /*  90 */ "comp ::= COMP INTEGER",
 /*  91 */ "prec ::= PRECISION STRING",
 /*  92 */ "update ::= UPDATE INTEGER",
 /*  93 */ "cachelast ::= CACHELAST INTEGER",
 /*  94 */ "partitions ::= PARTITIONS INTEGER",
 /*  95 */ "db_optr ::=",
 /*  96 */ "db_optr ::= db_optr cache",
 /*  97 */ "db_optr ::= db_optr replica",
 /*  98 */ "db_optr ::= db_optr quorum",
 /*  99 */ "db_optr ::= db_optr days",
 /* 100 */ "db_optr ::= db_optr minrows",
 /* 101 */ "db_optr ::= db_optr maxrows",
 /* 102 */ "db_optr ::= db_optr blocks",
 /* 103 */ "db_optr ::= db_optr ctime",
 /* 104 */ "db_optr ::= db_optr wal",
 /* 105 */ "db_optr ::= db_optr fsync",
 /* 106 */ "db_optr ::= db_optr comp",
 /* 107 */ "db_optr ::= db_optr prec",
 /* 108 */ "db_optr ::= db_optr keep",
 /* 109 */ "db_optr ::= db_optr update",
 /* 110 */ "db_optr ::= db_optr cachelast",
 /* 111 */ "topic_optr ::= db_optr",
 /* 112 */ "topic_optr ::= topic_optr partitions",
 /* 113 */ "alter_db_optr ::=",
 /* 114 */ "alter_db_optr ::= alter_db_optr replica",
 /* 115 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 116 */ "alter_db_optr ::= alter_db_optr keep",
 /* 117 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 118 */ "alter_db_optr ::= alter_db_optr comp",
 /* 119 */ "alter_db_optr ::= alter_db_optr wal",
 /* 120 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 121 */ "alter_db_optr ::= alter_db_optr update",
 /* 122 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 123 */ "alter_topic_optr ::= alter_db_optr",
 /* 124 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 125 */ "typename ::= ids",
 /* 126 */ "typename ::= ids LP signed RP",
 /* 127 */ "typename ::= ids UNSIGNED",
 /* 128 */ "signed ::= INTEGER",
 /* 129 */ "signed ::= PLUS INTEGER",
 /* 130 */ "signed ::= MINUS INTEGER",
 /* 131 */ "cmd ::= CREATE TABLE create_table_args",
 /* 132 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE TABLE create_table_list",
 /* 135 */ "create_table_list ::= create_from_stable",
 /* 136 */ "create_table_list ::= create_table_list create_from_stable",
 /* 137 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 138 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 141 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 142 */ "tagNamelist ::= ids",
 /* 143 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 144 */ "columnlist ::= columnlist COMMA column",
 /* 145 */ "columnlist ::= column",
 /* 146 */ "column ::= ids typename",
 /* 147 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 148 */ "tagitemlist ::= tagitem",
 /* 149 */ "tagitem ::= INTEGER",
 /* 150 */ "tagitem ::= FLOAT",
 /* 151 */ "tagitem ::= STRING",
 /* 152 */ "tagitem ::= BOOL",
 /* 153 */ "tagitem ::= NULL",
 /* 154 */ "tagitem ::= MINUS INTEGER",
 /* 155 */ "tagitem ::= MINUS FLOAT",
 /* 156 */ "tagitem ::= PLUS INTEGER",
 /* 157 */ "tagitem ::= PLUS FLOAT",
 /* 158 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 159 */ "select ::= LP select RP",
 /* 160 */ "union ::= select",
 /* 161 */ "union ::= union UNION ALL select",
 /* 162 */ "cmd ::= union",
 /* 163 */ "select ::= SELECT selcollist",
 /* 164 */ "sclp ::= selcollist COMMA",
 /* 165 */ "sclp ::=",
 /* 166 */ "selcollist ::= sclp distinct expr as",
 /* 167 */ "selcollist ::= sclp STAR",
 /* 168 */ "as ::= AS ids",
 /* 169 */ "as ::= ids",
 /* 170 */ "as ::=",
 /* 171 */ "distinct ::= DISTINCT",
 /* 172 */ "distinct ::=",
 /* 173 */ "from ::= FROM tablelist",
 /* 174 */ "from ::= FROM LP union RP",
 /* 175 */ "tablelist ::= ids cpxName",
 /* 176 */ "tablelist ::= ids cpxName ids",
 /* 177 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 178 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 179 */ "tmvar ::= VARIABLE",
 /* 180 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 181 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 182 */ "interval_opt ::=",
 /* 183 */ "session_option ::=",
 /* 184 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 185 */ "fill_opt ::=",
 /* 186 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 187 */ "fill_opt ::= FILL LP ID RP",
 /* 188 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 189 */ "sliding_opt ::=",
 /* 190 */ "orderby_opt ::=",
 /* 191 */ "orderby_opt ::= ORDER BY sortlist",
 /* 192 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 193 */ "sortlist ::= item sortorder",
 /* 194 */ "item ::= ids cpxName",
 /* 195 */ "sortorder ::= ASC",
 /* 196 */ "sortorder ::= DESC",
 /* 197 */ "sortorder ::=",
 /* 198 */ "groupby_opt ::=",
 /* 199 */ "groupby_opt ::= GROUP BY grouplist",
 /* 200 */ "grouplist ::= grouplist COMMA item",
 /* 201 */ "grouplist ::= item",
 /* 202 */ "having_opt ::=",
 /* 203 */ "having_opt ::= HAVING expr",
 /* 204 */ "limit_opt ::=",
 /* 205 */ "limit_opt ::= LIMIT signed",
 /* 206 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 207 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 208 */ "slimit_opt ::=",
 /* 209 */ "slimit_opt ::= SLIMIT signed",
 /* 210 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 211 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 212 */ "where_opt ::=",
 /* 213 */ "where_opt ::= WHERE expr",
 /* 214 */ "expr ::= LP expr RP",
 /* 215 */ "expr ::= ID",
 /* 216 */ "expr ::= ID DOT ID",
 /* 217 */ "expr ::= ID DOT STAR",
 /* 218 */ "expr ::= INTEGER",
 /* 219 */ "expr ::= MINUS INTEGER",
 /* 220 */ "expr ::= PLUS INTEGER",
 /* 221 */ "expr ::= FLOAT",
 /* 222 */ "expr ::= MINUS FLOAT",
 /* 223 */ "expr ::= PLUS FLOAT",
 /* 224 */ "expr ::= STRING",
 /* 225 */ "expr ::= NOW",
 /* 226 */ "expr ::= VARIABLE",
 /* 227 */ "expr ::= PLUS VARIABLE",
 /* 228 */ "expr ::= MINUS VARIABLE",
 /* 229 */ "expr ::= BOOL",
 /* 230 */ "expr ::= NULL",
 /* 231 */ "expr ::= ID LP exprlist RP",
 /* 232 */ "expr ::= ID LP STAR RP",
 /* 233 */ "expr ::= expr IS NULL",
 /* 234 */ "expr ::= expr IS NOT NULL",
 /* 235 */ "expr ::= expr LT expr",
 /* 236 */ "expr ::= expr GT expr",
 /* 237 */ "expr ::= expr LE expr",
 /* 238 */ "expr ::= expr GE expr",
 /* 239 */ "expr ::= expr NE expr",
 /* 240 */ "expr ::= expr EQ expr",
 /* 241 */ "expr ::= expr BETWEEN expr AND expr",
 /* 242 */ "expr ::= expr AND expr",
 /* 243 */ "expr ::= expr OR expr",
 /* 244 */ "expr ::= expr PLUS expr",
 /* 245 */ "expr ::= expr MINUS expr",
 /* 246 */ "expr ::= expr STAR expr",
 /* 247 */ "expr ::= expr SLASH expr",
 /* 248 */ "expr ::= expr REM expr",
 /* 249 */ "expr ::= expr LIKE expr",
 /* 250 */ "expr ::= expr IN LP exprlist RP",
 /* 251 */ "exprlist ::= exprlist COMMA expritem",
 /* 252 */ "exprlist ::= expritem",
 /* 253 */ "expritem ::= expr",
 /* 254 */ "expritem ::=",
 /* 255 */ "cmd ::= RESET QUERY CACHE",
 /* 256 */ "cmd ::= SYNCDB ids REPLICA",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 263 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 268 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 269 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 270 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 198: /* exprlist */
    case 239: /* selcollist */
    case 252: /* sclp */
{
tSqlExprListDestroy((yypminor->yy285));
}
      break;
    case 211: /* keep */
    case 212: /* tagitemlist */
    case 234: /* columnlist */
    case 235: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 258: /* sortlist */
    case 262: /* grouplist */
{
taosArrayDestroy((yypminor->yy285));
}
      break;
    case 232: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy470));
}
      break;
    case 236: /* select */
{
destroySqlNode((yypminor->yy344));
}
      break;
    case 240: /* from */
    case 256: /* tablelist */
{
destroyRelationInfo((yypminor->yy148));
}
      break;
    case 241: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 263: /* expritem */
{
tSqlExprDestroy((yypminor->yy178));
}
      break;
    case 251: /* union */
{
destroyAllSqlNode((yypminor->yy285));
}
      break;
    case 259: /* sortitem */
{
tVariantDestroy(&(yypminor->yy362));
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
  {  189,   -1 }, /* (0) program ::= cmd */
  {  190,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  190,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  190,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  190,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  190,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  190,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  190,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  190,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  190,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  190,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  190,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  190,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  190,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  190,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  190,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  191,    0 }, /* (16) dbPrefix ::= */
  {  191,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  193,    0 }, /* (18) cpxName ::= */
  {  193,   -2 }, /* (19) cpxName ::= DOT ids */
  {  190,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  190,   -5 }, /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  190,   -4 }, /* (22) cmd ::= SHOW CREATE DATABASE ids */
  {  190,   -3 }, /* (23) cmd ::= SHOW dbPrefix TABLES */
  {  190,   -5 }, /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  190,   -3 }, /* (25) cmd ::= SHOW dbPrefix STABLES */
  {  190,   -5 }, /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  190,   -3 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS */
  {  190,   -4 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  190,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  190,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  190,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  190,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  190,   -3 }, /* (33) cmd ::= DROP DNODE ids */
  {  190,   -3 }, /* (34) cmd ::= DROP USER ids */
  {  190,   -3 }, /* (35) cmd ::= DROP ACCOUNT ids */
  {  190,   -2 }, /* (36) cmd ::= USE ids */
  {  190,   -3 }, /* (37) cmd ::= DESCRIBE ids cpxName */
  {  190,   -5 }, /* (38) cmd ::= ALTER USER ids PASS ids */
  {  190,   -5 }, /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  190,   -4 }, /* (40) cmd ::= ALTER DNODE ids ids */
  {  190,   -5 }, /* (41) cmd ::= ALTER DNODE ids ids ids */
  {  190,   -3 }, /* (42) cmd ::= ALTER LOCAL ids */
  {  190,   -4 }, /* (43) cmd ::= ALTER LOCAL ids ids */
  {  190,   -4 }, /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  190,   -4 }, /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  190,   -4 }, /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  190,   -6 }, /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  190,   -6 }, /* (48) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  192,   -1 }, /* (49) ids ::= ID */
  {  192,   -1 }, /* (50) ids ::= STRING */
  {  194,   -2 }, /* (51) ifexists ::= IF EXISTS */
  {  194,    0 }, /* (52) ifexists ::= */
  {  199,   -3 }, /* (53) ifnotexists ::= IF NOT EXISTS */
  {  199,    0 }, /* (54) ifnotexists ::= */
  {  190,   -3 }, /* (55) cmd ::= CREATE DNODE ids */
  {  190,   -6 }, /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  190,   -5 }, /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  190,   -5 }, /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  190,   -5 }, /* (59) cmd ::= CREATE USER ids PASS ids */
  {  202,    0 }, /* (60) pps ::= */
  {  202,   -2 }, /* (61) pps ::= PPS INTEGER */
  {  203,    0 }, /* (62) tseries ::= */
  {  203,   -2 }, /* (63) tseries ::= TSERIES INTEGER */
  {  204,    0 }, /* (64) dbs ::= */
  {  204,   -2 }, /* (65) dbs ::= DBS INTEGER */
  {  205,    0 }, /* (66) streams ::= */
  {  205,   -2 }, /* (67) streams ::= STREAMS INTEGER */
  {  206,    0 }, /* (68) storage ::= */
  {  206,   -2 }, /* (69) storage ::= STORAGE INTEGER */
  {  207,    0 }, /* (70) qtime ::= */
  {  207,   -2 }, /* (71) qtime ::= QTIME INTEGER */
  {  208,    0 }, /* (72) users ::= */
  {  208,   -2 }, /* (73) users ::= USERS INTEGER */
  {  209,    0 }, /* (74) conns ::= */
  {  209,   -2 }, /* (75) conns ::= CONNS INTEGER */
  {  210,    0 }, /* (76) state ::= */
  {  210,   -2 }, /* (77) state ::= STATE ids */
  {  197,   -9 }, /* (78) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  211,   -2 }, /* (79) keep ::= KEEP tagitemlist */
  {  213,   -2 }, /* (80) cache ::= CACHE INTEGER */
  {  214,   -2 }, /* (81) replica ::= REPLICA INTEGER */
  {  215,   -2 }, /* (82) quorum ::= QUORUM INTEGER */
  {  216,   -2 }, /* (83) days ::= DAYS INTEGER */
  {  217,   -2 }, /* (84) minrows ::= MINROWS INTEGER */
  {  218,   -2 }, /* (85) maxrows ::= MAXROWS INTEGER */
  {  219,   -2 }, /* (86) blocks ::= BLOCKS INTEGER */
  {  220,   -2 }, /* (87) ctime ::= CTIME INTEGER */
  {  221,   -2 }, /* (88) wal ::= WAL INTEGER */
  {  222,   -2 }, /* (89) fsync ::= FSYNC INTEGER */
  {  223,   -2 }, /* (90) comp ::= COMP INTEGER */
  {  224,   -2 }, /* (91) prec ::= PRECISION STRING */
  {  225,   -2 }, /* (92) update ::= UPDATE INTEGER */
  {  226,   -2 }, /* (93) cachelast ::= CACHELAST INTEGER */
  {  227,   -2 }, /* (94) partitions ::= PARTITIONS INTEGER */
  {  200,    0 }, /* (95) db_optr ::= */
  {  200,   -2 }, /* (96) db_optr ::= db_optr cache */
  {  200,   -2 }, /* (97) db_optr ::= db_optr replica */
  {  200,   -2 }, /* (98) db_optr ::= db_optr quorum */
  {  200,   -2 }, /* (99) db_optr ::= db_optr days */
  {  200,   -2 }, /* (100) db_optr ::= db_optr minrows */
  {  200,   -2 }, /* (101) db_optr ::= db_optr maxrows */
  {  200,   -2 }, /* (102) db_optr ::= db_optr blocks */
  {  200,   -2 }, /* (103) db_optr ::= db_optr ctime */
  {  200,   -2 }, /* (104) db_optr ::= db_optr wal */
  {  200,   -2 }, /* (105) db_optr ::= db_optr fsync */
  {  200,   -2 }, /* (106) db_optr ::= db_optr comp */
  {  200,   -2 }, /* (107) db_optr ::= db_optr prec */
  {  200,   -2 }, /* (108) db_optr ::= db_optr keep */
  {  200,   -2 }, /* (109) db_optr ::= db_optr update */
  {  200,   -2 }, /* (110) db_optr ::= db_optr cachelast */
  {  201,   -1 }, /* (111) topic_optr ::= db_optr */
  {  201,   -2 }, /* (112) topic_optr ::= topic_optr partitions */
  {  195,    0 }, /* (113) alter_db_optr ::= */
  {  195,   -2 }, /* (114) alter_db_optr ::= alter_db_optr replica */
  {  195,   -2 }, /* (115) alter_db_optr ::= alter_db_optr quorum */
  {  195,   -2 }, /* (116) alter_db_optr ::= alter_db_optr keep */
  {  195,   -2 }, /* (117) alter_db_optr ::= alter_db_optr blocks */
  {  195,   -2 }, /* (118) alter_db_optr ::= alter_db_optr comp */
  {  195,   -2 }, /* (119) alter_db_optr ::= alter_db_optr wal */
  {  195,   -2 }, /* (120) alter_db_optr ::= alter_db_optr fsync */
  {  195,   -2 }, /* (121) alter_db_optr ::= alter_db_optr update */
  {  195,   -2 }, /* (122) alter_db_optr ::= alter_db_optr cachelast */
  {  196,   -1 }, /* (123) alter_topic_optr ::= alter_db_optr */
  {  196,   -2 }, /* (124) alter_topic_optr ::= alter_topic_optr partitions */
  {  228,   -1 }, /* (125) typename ::= ids */
  {  228,   -4 }, /* (126) typename ::= ids LP signed RP */
  {  228,   -2 }, /* (127) typename ::= ids UNSIGNED */
  {  229,   -1 }, /* (128) signed ::= INTEGER */
  {  229,   -2 }, /* (129) signed ::= PLUS INTEGER */
  {  229,   -2 }, /* (130) signed ::= MINUS INTEGER */
  {  190,   -3 }, /* (131) cmd ::= CREATE TABLE create_table_args */
  {  190,   -3 }, /* (132) cmd ::= CREATE TABLE create_stable_args */
  {  190,   -3 }, /* (133) cmd ::= CREATE STABLE create_stable_args */
  {  190,   -3 }, /* (134) cmd ::= CREATE TABLE create_table_list */
  {  232,   -1 }, /* (135) create_table_list ::= create_from_stable */
  {  232,   -2 }, /* (136) create_table_list ::= create_table_list create_from_stable */
  {  230,   -6 }, /* (137) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  231,  -10 }, /* (138) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  233,  -10 }, /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  233,  -13 }, /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  235,   -3 }, /* (141) tagNamelist ::= tagNamelist COMMA ids */
  {  235,   -1 }, /* (142) tagNamelist ::= ids */
  {  230,   -5 }, /* (143) create_table_args ::= ifnotexists ids cpxName AS select */
  {  234,   -3 }, /* (144) columnlist ::= columnlist COMMA column */
  {  234,   -1 }, /* (145) columnlist ::= column */
  {  237,   -2 }, /* (146) column ::= ids typename */
  {  212,   -3 }, /* (147) tagitemlist ::= tagitemlist COMMA tagitem */
  {  212,   -1 }, /* (148) tagitemlist ::= tagitem */
  {  238,   -1 }, /* (149) tagitem ::= INTEGER */
  {  238,   -1 }, /* (150) tagitem ::= FLOAT */
  {  238,   -1 }, /* (151) tagitem ::= STRING */
  {  238,   -1 }, /* (152) tagitem ::= BOOL */
  {  238,   -1 }, /* (153) tagitem ::= NULL */
  {  238,   -2 }, /* (154) tagitem ::= MINUS INTEGER */
  {  238,   -2 }, /* (155) tagitem ::= MINUS FLOAT */
  {  238,   -2 }, /* (156) tagitem ::= PLUS INTEGER */
  {  238,   -2 }, /* (157) tagitem ::= PLUS FLOAT */
  {  236,  -13 }, /* (158) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  236,   -3 }, /* (159) select ::= LP select RP */
  {  251,   -1 }, /* (160) union ::= select */
  {  251,   -4 }, /* (161) union ::= union UNION ALL select */
  {  190,   -1 }, /* (162) cmd ::= union */
  {  236,   -2 }, /* (163) select ::= SELECT selcollist */
  {  252,   -2 }, /* (164) sclp ::= selcollist COMMA */
  {  252,    0 }, /* (165) sclp ::= */
  {  239,   -4 }, /* (166) selcollist ::= sclp distinct expr as */
  {  239,   -2 }, /* (167) selcollist ::= sclp STAR */
  {  255,   -2 }, /* (168) as ::= AS ids */
  {  255,   -1 }, /* (169) as ::= ids */
  {  255,    0 }, /* (170) as ::= */
  {  253,   -1 }, /* (171) distinct ::= DISTINCT */
  {  253,    0 }, /* (172) distinct ::= */
  {  240,   -2 }, /* (173) from ::= FROM tablelist */
  {  240,   -4 }, /* (174) from ::= FROM LP union RP */
  {  256,   -2 }, /* (175) tablelist ::= ids cpxName */
  {  256,   -3 }, /* (176) tablelist ::= ids cpxName ids */
  {  256,   -4 }, /* (177) tablelist ::= tablelist COMMA ids cpxName */
  {  256,   -5 }, /* (178) tablelist ::= tablelist COMMA ids cpxName ids */
  {  257,   -1 }, /* (179) tmvar ::= VARIABLE */
  {  242,   -4 }, /* (180) interval_opt ::= INTERVAL LP tmvar RP */
  {  242,   -6 }, /* (181) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  242,    0 }, /* (182) interval_opt ::= */
  {  243,    0 }, /* (183) session_option ::= */
  {  243,   -7 }, /* (184) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  244,    0 }, /* (185) fill_opt ::= */
  {  244,   -6 }, /* (186) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  244,   -4 }, /* (187) fill_opt ::= FILL LP ID RP */
  {  245,   -4 }, /* (188) sliding_opt ::= SLIDING LP tmvar RP */
  {  245,    0 }, /* (189) sliding_opt ::= */
  {  247,    0 }, /* (190) orderby_opt ::= */
  {  247,   -3 }, /* (191) orderby_opt ::= ORDER BY sortlist */
  {  258,   -4 }, /* (192) sortlist ::= sortlist COMMA item sortorder */
  {  258,   -2 }, /* (193) sortlist ::= item sortorder */
  {  260,   -2 }, /* (194) item ::= ids cpxName */
  {  261,   -1 }, /* (195) sortorder ::= ASC */
  {  261,   -1 }, /* (196) sortorder ::= DESC */
  {  261,    0 }, /* (197) sortorder ::= */
  {  246,    0 }, /* (198) groupby_opt ::= */
  {  246,   -3 }, /* (199) groupby_opt ::= GROUP BY grouplist */
  {  262,   -3 }, /* (200) grouplist ::= grouplist COMMA item */
  {  262,   -1 }, /* (201) grouplist ::= item */
  {  248,    0 }, /* (202) having_opt ::= */
  {  248,   -2 }, /* (203) having_opt ::= HAVING expr */
  {  250,    0 }, /* (204) limit_opt ::= */
  {  250,   -2 }, /* (205) limit_opt ::= LIMIT signed */
  {  250,   -4 }, /* (206) limit_opt ::= LIMIT signed OFFSET signed */
  {  250,   -4 }, /* (207) limit_opt ::= LIMIT signed COMMA signed */
  {  249,    0 }, /* (208) slimit_opt ::= */
  {  249,   -2 }, /* (209) slimit_opt ::= SLIMIT signed */
  {  249,   -4 }, /* (210) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  249,   -4 }, /* (211) slimit_opt ::= SLIMIT signed COMMA signed */
  {  241,    0 }, /* (212) where_opt ::= */
  {  241,   -2 }, /* (213) where_opt ::= WHERE expr */
  {  254,   -3 }, /* (214) expr ::= LP expr RP */
  {  254,   -1 }, /* (215) expr ::= ID */
  {  254,   -3 }, /* (216) expr ::= ID DOT ID */
  {  254,   -3 }, /* (217) expr ::= ID DOT STAR */
  {  254,   -1 }, /* (218) expr ::= INTEGER */
  {  254,   -2 }, /* (219) expr ::= MINUS INTEGER */
  {  254,   -2 }, /* (220) expr ::= PLUS INTEGER */
  {  254,   -1 }, /* (221) expr ::= FLOAT */
  {  254,   -2 }, /* (222) expr ::= MINUS FLOAT */
  {  254,   -2 }, /* (223) expr ::= PLUS FLOAT */
  {  254,   -1 }, /* (224) expr ::= STRING */
  {  254,   -1 }, /* (225) expr ::= NOW */
  {  254,   -1 }, /* (226) expr ::= VARIABLE */
  {  254,   -2 }, /* (227) expr ::= PLUS VARIABLE */
  {  254,   -2 }, /* (228) expr ::= MINUS VARIABLE */
  {  254,   -1 }, /* (229) expr ::= BOOL */
  {  254,   -1 }, /* (230) expr ::= NULL */
  {  254,   -4 }, /* (231) expr ::= ID LP exprlist RP */
  {  254,   -4 }, /* (232) expr ::= ID LP STAR RP */
  {  254,   -3 }, /* (233) expr ::= expr IS NULL */
  {  254,   -4 }, /* (234) expr ::= expr IS NOT NULL */
  {  254,   -3 }, /* (235) expr ::= expr LT expr */
  {  254,   -3 }, /* (236) expr ::= expr GT expr */
  {  254,   -3 }, /* (237) expr ::= expr LE expr */
  {  254,   -3 }, /* (238) expr ::= expr GE expr */
  {  254,   -3 }, /* (239) expr ::= expr NE expr */
  {  254,   -3 }, /* (240) expr ::= expr EQ expr */
  {  254,   -5 }, /* (241) expr ::= expr BETWEEN expr AND expr */
  {  254,   -3 }, /* (242) expr ::= expr AND expr */
  {  254,   -3 }, /* (243) expr ::= expr OR expr */
  {  254,   -3 }, /* (244) expr ::= expr PLUS expr */
  {  254,   -3 }, /* (245) expr ::= expr MINUS expr */
  {  254,   -3 }, /* (246) expr ::= expr STAR expr */
  {  254,   -3 }, /* (247) expr ::= expr SLASH expr */
  {  254,   -3 }, /* (248) expr ::= expr REM expr */
  {  254,   -3 }, /* (249) expr ::= expr LIKE expr */
  {  254,   -5 }, /* (250) expr ::= expr IN LP exprlist RP */
  {  198,   -3 }, /* (251) exprlist ::= exprlist COMMA expritem */
  {  198,   -1 }, /* (252) exprlist ::= expritem */
  {  263,   -1 }, /* (253) expritem ::= expr */
  {  263,    0 }, /* (254) expritem ::= */
  {  190,   -3 }, /* (255) cmd ::= RESET QUERY CACHE */
  {  190,   -3 }, /* (256) cmd ::= SYNCDB ids REPLICA */
  {  190,   -7 }, /* (257) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (258) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (259) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (260) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (261) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -9 }, /* (262) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  190,   -7 }, /* (263) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (264) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (265) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (266) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (267) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -3 }, /* (268) cmd ::= KILL CONNECTION INTEGER */
  {  190,   -5 }, /* (269) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  190,   -5 }, /* (270) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 131: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==133);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 10: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 15: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 16: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 18: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 20: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 29: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 30: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 31: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 32: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy187);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 48: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy285);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 51: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 172: /* distinct ::= */ yytestcase(yyruleno==172);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 60: /* pps ::= */
      case 62: /* tseries ::= */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= */ yytestcase(yyruleno==64);
      case 66: /* streams ::= */ yytestcase(yyruleno==66);
      case 68: /* storage ::= */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= */ yytestcase(yyruleno==70);
      case 72: /* users ::= */ yytestcase(yyruleno==72);
      case 74: /* conns ::= */ yytestcase(yyruleno==74);
      case 76: /* state ::= */ yytestcase(yyruleno==76);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 61: /* pps ::= PPS INTEGER */
      case 63: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==63);
      case 65: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==69);
      case 71: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==71);
      case 73: /* users ::= USERS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* state ::= STATE ids */ yytestcase(yyruleno==77);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 78: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy187.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy187.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy187.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy187.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy187.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy187.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy187.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy187 = yylhsminor.yy187;
        break;
      case 79: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy285 = yymsp[0].minor.yy285; }
        break;
      case 80: /* cache ::= CACHE INTEGER */
      case 81: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==81);
      case 82: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==82);
      case 83: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==87);
      case 88: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==88);
      case 89: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==89);
      case 90: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==90);
      case 91: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==91);
      case 92: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==92);
      case 93: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==93);
      case 94: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==94);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 95: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 96: /* db_optr ::= db_optr cache */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 97: /* db_optr ::= db_optr replica */
      case 114: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==114);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 98: /* db_optr ::= db_optr quorum */
      case 115: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==115);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 99: /* db_optr ::= db_optr days */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 100: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 101: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 102: /* db_optr ::= db_optr blocks */
      case 117: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==117);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 103: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 104: /* db_optr ::= db_optr wal */
      case 119: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==119);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 105: /* db_optr ::= db_optr fsync */
      case 120: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==120);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 106: /* db_optr ::= db_optr comp */
      case 118: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==118);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 107: /* db_optr ::= db_optr prec */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 108: /* db_optr ::= db_optr keep */
      case 116: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==116);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.keep = yymsp[0].minor.yy285; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 109: /* db_optr ::= db_optr update */
      case 121: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==121);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 110: /* db_optr ::= db_optr cachelast */
      case 122: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==122);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 111: /* topic_optr ::= db_optr */
      case 123: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==123);
{ yylhsminor.yy526 = yymsp[0].minor.yy526; yylhsminor.yy526.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 112: /* topic_optr ::= topic_optr partitions */
      case 124: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==124);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 113: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 125: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy295, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy295 = yylhsminor.yy295;
        break;
      case 126: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy525 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy525;  // negative value of name length
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy295 = yylhsminor.yy295;
        break;
      case 127: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy295, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 128: /* signed ::= INTEGER */
{ yylhsminor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 129: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 130: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy525 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 134: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy470;}
        break;
      case 135: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy470 = pCreateTable;
}
  yymsp[0].minor.yy470 = yylhsminor.yy470;
        break;
      case 136: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy470->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy470 = yymsp[-1].minor.yy470;
}
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 137: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy470 = tSetCreateTableInfo(yymsp[-1].minor.yy285, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy470 = yylhsminor.yy470;
        break;
      case 138: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy470 = tSetCreateTableInfo(yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy470 = yylhsminor.yy470;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy285, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 141: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy0); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 142: /* tagNamelist ::= ids */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 143: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy470 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy344, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy470 = yylhsminor.yy470;
        break;
      case 144: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy295); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 145: /* columnlist ::= column */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy295);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 146: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy295, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy295);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 147: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);    }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 148: /* tagitemlist ::= tagitem */
{ yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1); }
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 149: /* tagitem ::= INTEGER */
      case 150: /* tagitem ::= FLOAT */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= STRING */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= BOOL */ yytestcase(yyruleno==152);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 153: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 154: /* tagitem ::= MINUS INTEGER */
      case 155: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==157);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 158: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy344 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy285, yymsp[-10].minor.yy148, yymsp[-9].minor.yy178, yymsp[-4].minor.yy285, yymsp[-3].minor.yy285, &yymsp[-8].minor.yy376, &yymsp[-7].minor.yy523, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy285, &yymsp[0].minor.yy438, &yymsp[-1].minor.yy438, yymsp[-2].minor.yy178);
}
  yymsp[-12].minor.yy344 = yylhsminor.yy344;
        break;
      case 159: /* select ::= LP select RP */
{yymsp[-2].minor.yy344 = yymsp[-1].minor.yy344;}
        break;
      case 160: /* union ::= select */
{ yylhsminor.yy285 = setSubclause(NULL, yymsp[0].minor.yy344); }
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 161: /* union ::= union UNION ALL select */
{ yylhsminor.yy285 = appendSelectClause(yymsp[-3].minor.yy285, yymsp[0].minor.yy344); }
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 162: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy285, NULL, TSDB_SQL_SELECT); }
        break;
      case 163: /* select ::= SELECT selcollist */
{
  yylhsminor.yy344 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy285, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 164: /* sclp ::= selcollist COMMA */
{yylhsminor.yy285 = yymsp[-1].minor.yy285;}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 165: /* sclp ::= */
      case 190: /* orderby_opt ::= */ yytestcase(yyruleno==190);
{yymsp[1].minor.yy285 = 0;}
        break;
      case 166: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-3].minor.yy285, yymsp[-1].minor.yy178,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 167: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-1].minor.yy285, pNode, 0, 0);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 168: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 169: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 170: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 171: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 173: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy148 = yymsp[0].minor.yy148;}
        break;
      case 174: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy148 = setSubquery(NULL, yymsp[-1].minor.yy285);}
        break;
      case 175: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 176: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy148 = yylhsminor.yy148;
        break;
      case 177: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(yymsp[-3].minor.yy148, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy148 = yylhsminor.yy148;
        break;
      case 178: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(yymsp[-4].minor.yy148, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy148 = yylhsminor.yy148;
        break;
      case 179: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 180: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy376.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy376.offset.n = 0;}
        break;
      case 181: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy376.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy376.offset = yymsp[-1].minor.yy0;}
        break;
      case 182: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy376, 0, sizeof(yymsp[1].minor.yy376));}
        break;
      case 183: /* session_option ::= */
{yymsp[1].minor.yy523.col.n = 0; yymsp[1].minor.yy523.gap.n = 0;}
        break;
      case 184: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy523.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy523.gap = yymsp[-1].minor.yy0;
}
        break;
      case 185: /* fill_opt ::= */
{ yymsp[1].minor.yy285 = 0;     }
        break;
      case 186: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy285, &A, -1, 0);
    yymsp[-5].minor.yy285 = yymsp[-1].minor.yy285;
}
        break;
      case 187: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy285 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 188: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 189: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 191: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 192: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy285 = tVariantListAppend(yymsp[-3].minor.yy285, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 193: /* sortlist ::= item sortorder */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 194: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 195: /* sortorder ::= ASC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 196: /* sortorder ::= DESC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_DESC;}
        break;
      case 197: /* sortorder ::= */
{ yymsp[1].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 198: /* groupby_opt ::= */
{ yymsp[1].minor.yy285 = 0;}
        break;
      case 199: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 200: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);
}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 201: /* grouplist ::= item */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1);
}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 202: /* having_opt ::= */
      case 212: /* where_opt ::= */ yytestcase(yyruleno==212);
      case 254: /* expritem ::= */ yytestcase(yyruleno==254);
{yymsp[1].minor.yy178 = 0;}
        break;
      case 203: /* having_opt ::= HAVING expr */
      case 213: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==213);
{yymsp[-1].minor.yy178 = yymsp[0].minor.yy178;}
        break;
      case 204: /* limit_opt ::= */
      case 208: /* slimit_opt ::= */ yytestcase(yyruleno==208);
{yymsp[1].minor.yy438.limit = -1; yymsp[1].minor.yy438.offset = 0;}
        break;
      case 205: /* limit_opt ::= LIMIT signed */
      case 209: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==209);
{yymsp[-1].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-1].minor.yy438.offset = 0;}
        break;
      case 206: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 207: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 210: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 211: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 214: /* expr ::= LP expr RP */
{yylhsminor.yy178 = yymsp[-1].minor.yy178; yylhsminor.yy178->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy178->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 215: /* expr ::= ID */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 216: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 217: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 218: /* expr ::= INTEGER */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 219: /* expr ::= MINUS INTEGER */
      case 220: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==220);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 221: /* expr ::= FLOAT */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 222: /* expr ::= MINUS FLOAT */
      case 223: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==223);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 224: /* expr ::= STRING */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 225: /* expr ::= NOW */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 226: /* expr ::= VARIABLE */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 227: /* expr ::= PLUS VARIABLE */
      case 228: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 229: /* expr ::= BOOL */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 230: /* expr ::= NULL */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 231: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(yymsp[-1].minor.yy285, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 232: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 233: /* expr ::= expr IS NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 234: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-3].minor.yy178, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 235: /* expr ::= expr LT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 236: /* expr ::= expr GT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 237: /* expr ::= expr LE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 238: /* expr ::= expr GE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 239: /* expr ::= expr NE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_NE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 240: /* expr ::= expr EQ expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_EQ);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 241: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy178); yylhsminor.yy178 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy178, yymsp[-2].minor.yy178, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy178, TK_LE), TK_AND);}
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 242: /* expr ::= expr AND expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_AND);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 243: /* expr ::= expr OR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_OR); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 244: /* expr ::= expr PLUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_PLUS);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 245: /* expr ::= expr MINUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_MINUS); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 246: /* expr ::= expr STAR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_STAR);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 247: /* expr ::= expr SLASH expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_DIVIDE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 248: /* expr ::= expr REM expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_REM);   }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 249: /* expr ::= expr LIKE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LIKE);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 250: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-4].minor.yy178, (tSqlExpr*)yymsp[-1].minor.yy285, TK_IN); }
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 251: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy285 = tSqlExprListAppend(yymsp[-2].minor.yy285,yymsp[0].minor.yy178,0, 0);}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 252: /* exprlist ::= expritem */
{yylhsminor.yy285 = tSqlExprListAppend(0,yymsp[0].minor.yy178,0, 0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 253: /* expritem ::= expr */
{yylhsminor.yy178 = yymsp[0].minor.yy178;}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 255: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 256: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 262: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy362, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 268: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 269: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 270: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
