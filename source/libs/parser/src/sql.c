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

#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"
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
#define YYCODETYPE unsigned char
#define YYNOCODE 218
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SToken yy5;
  bool yy25;
  SNodeList* yy40;
  ENullOrder yy53;
  EOrder yy54;
  SNode* yy68;
  EJoinType yy92;
  EFillMode yy94;
  SDatabaseOptions* yy339;
  SDataType yy372;
  EOperatorType yy416;
  STableOptions* yy418;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  SAstCreateContext* pCxt ;
#define ParseARG_PDECL , SAstCreateContext* pCxt 
#define ParseARG_PARAM ,pCxt 
#define ParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define ParseARG_STORE yypParser->pCxt =pCxt ;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYNSTATE             290
#define YYNRULE              248
#define YYNTOKEN             140
#define YY_MAX_SHIFT         289
#define YY_MIN_SHIFTREDUCE   454
#define YY_MAX_SHIFTREDUCE   701
#define YY_ERROR_ACTION      702
#define YY_ACCEPT_ACTION     703
#define YY_NO_ACTION         704
#define YY_MIN_REDUCE        705
#define YY_MAX_REDUCE        952
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
#define YY_ACTTAB_COUNT (929)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    41,  155,   23,   96,  793,  200,  791,  845,  200,  884,
 /*    10 */   845,  750,   30,   28,   26,   25,   24,  190,  830,  819,
 /*    20 */   201,  172,  143,  124,  831,  156,  834,  881,   30,   28,
 /*    30 */    26,   25,   24,  137,  716,  821,  819,  177,  821,  819,
 /*    40 */   200,  232,  845,  931,   30,   28,   26,   25,   24,  137,
 /*    50 */   830,  819,  201,  232,  584,   55,  831,   77,  834,  870,
 /*    60 */   744,  929,  159,  869,  866,  793,   10,  791,  574,  282,
 /*    70 */   281,  280,  279,  278,  277,  276,  275,  274,  273,  272,
 /*    80 */   271,  270,  269,  268,  267,  266,  584,   30,   28,   26,
 /*    90 */    25,   24,  220,  608,  200,  220,  845,   22,  147,  573,
 /*   100 */   602,  603,  604,  605,  606,  607,  610,  611,  612,  608,
 /*   110 */   176,  265,  233,   22,  147,  219,  602,  603,  604,  605,
 /*   120 */   606,  607,  610,  611,  612,  232,   78,  754,  258,  668,
 /*   130 */   576,  509,  256,  255,  254,  513,  253,  515,  516,  252,
 /*   140 */   518,  249,  203,  524,  246,  526,  527,  243,  240,  200,
 /*   150 */    10,  845,  931,  168,  666,  667,  669,  670,  190,  830,
 /*   160 */   819,  201,  142,  572,   53,  831,  930,  834,  870,   41,
 /*   170 */   929,  233,  136,  866,  751,  889,   62,  639,  609,  135,
 /*   180 */   749,  565,   20,  229,  931,  132,  754,  563,  884,  186,
 /*   190 */   259,  845,  105,  613,  233,  183,  810,  230,   77,  830,
 /*   200 */   819,  201,  929,  102,   54,  831,  880,  834,  870,  754,
 /*   210 */    61,   94,  145,  866,   72,  193,  217,  216,  215,  286,
 /*   220 */   285,  210,  209,  208,  207,  206,   95,  204,   59,  793,
 /*   230 */   234,  792,  162,  897,  186,  154,  845,  185,   73,  877,
 /*   240 */   878,   56,  882,   78,  830,  819,  201,  756,   68,   54,
 /*   250 */   831,   97,  834,  870,  564,  566,  569,  145,  866,   72,
 /*   260 */   169,  200,  742,  845,  884,   30,   28,   26,   25,   24,
 /*   270 */   188,  830,  819,  201,   78,  577,   54,  831,  898,  834,
 /*   280 */   870,  200,  879,  845,  145,  866,  943,  233,  822,  819,
 /*   290 */   231,  830,  819,  201,  265,  904,   54,  831,   45,  834,
 /*   300 */   870,  200,  754,  845,  145,  866,  943,  233,  643,  747,
 /*   310 */   114,  830,  819,  201,   89,  927,   54,  831,  158,  834,
 /*   320 */   870,  462,  754,  900,  145,  866,  943,  223,   29,   27,
 /*   330 */   756,   68,   29,   27,  644,  888,  599,  565,  757,   68,
 /*   340 */   184,  565,  192,  563,  152,  160,  846,  563,  152,   11,
 /*   350 */    29,   27,  119,   11,  703,  784,   80,  756,   68,  565,
 /*   360 */   233,   29,   27,  161,    2,  563,  152,  665,   19,    1,
 /*   370 */   565,   11,   57,    1,   99,  754,  563,  152,   30,   28,
 /*   380 */    26,   25,   24,  202,   29,   27,  234,   51,  697,  698,
 /*   390 */   234,    1,  222,  565,  901,   63,   86,  462,  746,  563,
 /*   400 */   152,   83,    7,  651,  463,  464,    6,  639,  234,  931,
 /*   410 */   564,  566,  569,  577,  564,  566,  569,  574,  642,  234,
 /*   420 */   178,  173,  171,   77,  911,    1,  183,  929,  197,   78,
 /*   430 */   700,  701,  564,  566,  569,    9,    8,  200,  194,  845,
 /*   440 */   170,   61,  234,  564,  566,  569,  614,  830,  819,  201,
 /*   450 */   190,   31,   55,  831,  167,  834,  870,    9,    8,   59,
 /*   460 */   187,  866,   78,   29,   27,  189,  564,  566,  569,   92,
 /*   470 */   877,  182,  565,  181,   81,  581,  931,  826,  563,  152,
 /*   480 */    31,  200,  824,  845,  569,  165,   29,   27,  620,  910,
 /*   490 */    77,  830,  819,  201,  929,  565,   55,  831,  166,  834,
 /*   500 */   870,  563,  152,  198,    7,  867,  109,  144,  200,    5,
 /*   510 */   845,  195,   79,   85,   26,   25,   24,  108,  830,  819,
 /*   520 */   201,  234,   21,   69,  831,  111,  834,    7,  891,  211,
 /*   530 */    65,  180,   30,   28,   26,   25,   24,  743,  200,   42,
 /*   540 */   845,   88,  106,  164,  234,  564,  566,  569,  830,  819,
 /*   550 */   201,   71,  502,  130,  831,  151,  834,   66,  228,    4,
 /*   560 */    90,  226,  191,  944,  104,  103,  639,  497,  564,  566,
 /*   570 */   569,  200,   57,  845,   50,   60,  576,   47,  885,   91,
 /*   580 */   262,  830,  819,  201,  261,   32,  130,  831,  163,  834,
 /*   590 */   200,  852,  845,  530,  534,   16,  148,  263,  238,   65,
 /*   600 */   830,  819,  201,  946,  196,  126,  831,  539,  834,  200,
 /*   610 */    67,  845,   66,  928,  199,   65,  260,   98,  572,  830,
 /*   620 */   819,  201,  812,  101,  130,  831,  146,  834,  205,  200,
 /*   630 */   212,  845,  214,   40,  213,  218,  221,  578,  179,  830,
 /*   640 */   819,  201,  107,  224,   69,  831,  183,  834,  200,  153,
 /*   650 */   845,  741,  118,   44,   46,  120,  755,  289,  830,  819,
 /*   660 */   201,   61,  236,  125,  831,  200,  834,  845,  115,  133,
 /*   670 */   134,    3,  121,   31,   14,  830,  819,  201,   82,   59,
 /*   680 */   127,  831,  662,  834,  945,  200,   84,  845,   35,   74,
 /*   690 */   877,  878,  664,  882,  262,  830,  819,  201,  261,   70,
 /*   700 */   122,  831,   87,  834,  658,  200,   37,  845,  657,  174,
 /*   710 */   824,  263,   15,   38,  175,  830,  819,  201,  636,  635,
 /*   720 */   128,  831,   18,  834,  200,   93,  845,  117,   33,   34,
 /*   730 */   260,   76,    8,   58,  830,  819,  201,  582,  116,  123,
 /*   740 */   831,  600,  834,  200,  691,  845,   17,   12,   39,  686,
 /*   750 */   685,  149,  690,  830,  819,  201,  689,  100,  129,  831,
 /*   760 */    52,  834,  200,  112,  845,  150,   13,  813,  556,  805,
 /*   770 */   804,   64,  830,  819,  201,  803,  802,  842,  831,  801,
 /*   780 */   834,  200,  800,  845,  799,  798,  558,  797,  796,  795,
 /*   790 */   794,  830,  819,  201,  718,  745,  841,  831,  470,  834,
 /*   800 */   200,  717,  845,  712,  711,  708,  707,  225,  706,   43,
 /*   810 */   830,  819,  201,  227,   47,  840,  831,  110,  834,  200,
 /*   820 */   823,  845,  531,  567,  113,   36,  237,  157,  235,  830,
 /*   830 */   819,  201,  528,  239,  140,  831,  200,  834,  845,  241,
 /*   840 */   244,  247,  508,  250,  242,  523,  830,  819,  201,  522,
 /*   850 */   525,  139,  831,  521,  834,  520,  200,  257,  845,  538,
 /*   860 */   245,  537,  536,  519,  248,  468,  830,  819,  201,  517,
 /*   870 */   251,  141,  831,   48,  834,   49,  200,  264,  845,  489,
 /*   880 */   488,  183,  487,  486,  485,  484,  830,  819,  201,  483,
 /*   890 */   482,  138,  831,  481,  834,  200,   61,  845,  480,  479,
 /*   900 */   478,  477,  476,  475,  474,  830,  819,  201,  473,  710,
 /*   910 */   131,  831,  283,  834,   59,  284,  709,  705,  287,  288,
 /*   920 */   704,  704,  704,  704,   75,  877,  878,  704,  882,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   147,  159,  180,  181,  162,  160,  164,  162,  160,  173,
 /*    10 */   162,  158,   12,   13,   14,   15,   16,  169,  170,  171,
 /*    20 */   172,  176,  157,  175,  176,  157,  178,  191,   12,   13,
 /*    30 */    14,   15,   16,   33,    0,  170,  171,   28,  170,  171,
 /*    40 */   160,   28,  162,  195,   12,   13,   14,   15,   16,   33,
 /*    50 */   170,  171,  172,   28,   54,  175,  176,  209,  178,  179,
 /*    60 */     0,  213,  159,  183,  184,  162,   53,  164,   28,   35,
 /*    70 */    36,   37,   38,   39,   40,   41,   42,   43,   44,   45,
 /*    80 */    46,   47,   48,   49,   50,   51,   54,   12,   13,   14,
 /*    90 */    15,   16,   32,   93,  160,   32,  162,   97,   98,   28,
 /*   100 */   100,  101,  102,  103,  104,  105,  106,  107,  108,   93,
 /*   110 */   176,   32,  145,   97,   98,  148,  100,  101,  102,  103,
 /*   120 */   104,  105,  106,  107,  108,   28,  113,  160,  163,   99,
 /*   130 */    28,   61,   62,   63,   64,   65,   66,   67,   68,   69,
 /*   140 */    70,   71,  145,   73,   74,   75,   76,   77,   78,  160,
 /*   150 */    53,  162,  195,  123,  124,  125,  126,  127,  169,  170,
 /*   160 */   171,  172,  165,   28,  175,  176,  209,  178,  179,  147,
 /*   170 */   213,  145,  183,  184,  148,  110,  154,  112,   93,   18,
 /*   180 */   158,   21,   97,   22,  195,   24,  160,   27,  173,  160,
 /*   190 */    57,  162,   31,  108,  145,  145,  160,  148,  209,  170,
 /*   200 */   171,  172,  213,  167,  175,  176,  191,  178,  179,  160,
 /*   210 */   160,  109,  183,  184,  185,    3,   81,   82,   83,  142,
 /*   220 */   143,   86,   87,   88,   89,   90,  197,   92,  178,  162,
 /*   230 */    70,  164,  203,  204,  160,  149,  162,  187,  188,  189,
 /*   240 */   190,   80,  192,  113,  170,  171,  172,  161,  162,  175,
 /*   250 */   176,  216,  178,  179,   94,   95,   96,  183,  184,  185,
 /*   260 */   207,  160,    0,  162,  173,   12,   13,   14,   15,   16,
 /*   270 */    33,  170,  171,  172,  113,   28,  175,  176,  204,  178,
 /*   280 */   179,  160,  191,  162,  183,  184,  185,  145,  170,  171,
 /*   290 */   148,  170,  171,  172,   32,  194,  175,  176,  144,  178,
 /*   300 */   179,  160,  160,  162,  183,  184,  185,  145,    4,  155,
 /*   310 */   148,  170,  171,  172,  200,  194,  175,  176,  149,  178,
 /*   320 */   179,   21,  160,  174,  183,  184,  185,   27,   12,   13,
 /*   330 */   161,  162,   12,   13,   14,  194,   99,   21,  161,  162,
 /*   340 */   193,   21,  130,   27,   28,  149,  162,   27,   28,   33,
 /*   350 */    12,   13,  150,   33,  140,  153,  109,  161,  162,   21,
 /*   360 */   145,   12,   13,  148,  196,   27,   28,   54,    2,   53,
 /*   370 */    21,   33,   59,   53,  210,  160,   27,   28,   12,   13,
 /*   380 */    14,   15,   16,  169,   12,   13,   70,  144,  135,  136,
 /*   390 */    70,   53,  142,   21,  174,  152,   54,   21,  155,   27,
 /*   400 */    28,   59,   53,   14,   28,   29,  111,  112,   70,  195,
 /*   410 */    94,   95,   96,   28,   94,   95,   96,   28,  114,   70,
 /*   420 */   118,  119,  120,  209,  206,   53,  145,  213,   59,  113,
 /*   430 */   138,  139,   94,   95,   96,    1,    2,  160,   59,  162,
 /*   440 */   122,  160,   70,   94,   95,   96,   54,  170,  171,  172,
 /*   450 */   169,   59,  175,  176,  121,  178,  179,    1,    2,  178,
 /*   460 */   183,  184,  113,   12,   13,   14,   94,   95,   96,  188,
 /*   470 */   189,  190,   21,  192,  205,   54,  195,   53,   27,   28,
 /*   480 */    59,  160,   58,  162,   96,  171,   12,   13,   54,  206,
 /*   490 */   209,  170,  171,  172,  213,   21,  175,  176,  171,  178,
 /*   500 */   179,   27,   28,  134,   53,  184,   19,  171,  160,  129,
 /*   510 */   162,  132,   25,  205,   14,   15,   16,   30,  170,  171,
 /*   520 */   172,   70,    2,  175,  176,   54,  178,   53,  202,   49,
 /*   530 */    59,  128,   12,   13,   14,   15,   16,    0,  160,   52,
 /*   540 */   162,  201,   55,  116,   70,   94,   95,   96,  170,  171,
 /*   550 */   172,  199,   54,  175,  176,  177,  178,   59,   20,  115,
 /*   560 */   198,   23,  214,  215,   84,   85,  112,   54,   94,   95,
 /*   570 */    96,  160,   59,  162,   53,  160,   28,   56,  173,  186,
 /*   580 */    43,  170,  171,  172,   47,   91,  175,  176,  177,  178,
 /*   590 */   160,  182,  162,   54,   54,   53,  137,   60,   59,   59,
 /*   600 */   170,  171,  172,  217,  131,  175,  176,   54,  178,  160,
 /*   610 */    54,  162,   59,  212,  133,   59,   79,  211,   28,  170,
 /*   620 */   171,  172,  145,   91,  175,  176,  177,  178,  168,  160,
 /*   630 */   166,  162,  166,  147,   93,  145,  145,   28,  208,  170,
 /*   640 */   171,  172,  147,  141,  175,  176,  145,  178,  160,  141,
 /*   650 */   162,    0,  153,  144,   53,  145,  160,  141,  170,  171,
 /*   660 */   172,  160,  156,  175,  176,  160,  178,  162,  144,  151,
 /*   670 */   151,   59,  146,   59,  117,  170,  171,  172,   54,  178,
 /*   680 */   175,  176,   54,  178,  215,  160,   53,  162,   59,  188,
 /*   690 */   189,  190,   54,  192,   43,  170,  171,  172,   47,   53,
 /*   700 */   175,  176,   53,  178,   54,  160,   53,  162,   54,   27,
 /*   710 */    58,   60,  117,   53,   59,  170,  171,  172,   54,   54,
 /*   720 */   175,  176,   59,  178,  160,   58,  162,   19,  110,   59,
 /*   730 */    79,   58,    2,   25,  170,  171,  172,   54,   30,  175,
 /*   740 */   176,   99,  178,  160,   54,  162,   59,  117,    4,   27,
 /*   750 */    27,   27,   27,  170,  171,  172,   27,   58,  175,  176,
 /*   760 */    52,  178,  160,   55,  162,   27,   53,    0,   58,    0,
 /*   770 */     0,   91,  170,  171,  172,    0,    0,  175,  176,    0,
 /*   780 */   178,  160,    0,  162,    0,    0,   21,    0,    0,    0,
 /*   790 */     0,  170,  171,  172,    0,    0,  175,  176,   34,  178,
 /*   800 */   160,    0,  162,    0,    0,    0,    0,   21,    0,   53,
 /*   810 */   170,  171,  172,   21,   56,  175,  176,   19,  178,  160,
 /*   820 */    58,  162,   54,   21,   58,   53,   27,   27,   57,  170,
 /*   830 */   171,  172,   54,   53,  175,  176,  160,  178,  162,   27,
 /*   840 */    27,   27,   21,   27,   53,   72,  170,  171,  172,   72,
 /*   850 */    54,  175,  176,   72,  178,   72,  160,   60,  162,   27,
 /*   860 */    53,   27,   21,   54,   53,   34,  170,  171,  172,   54,
 /*   870 */    53,  175,  176,   53,  178,   53,  160,   33,  162,   27,
 /*   880 */    27,  145,   27,   27,   27,   27,  170,  171,  172,   27,
 /*   890 */    21,  175,  176,   27,  178,  160,  160,  162,   27,   27,
 /*   900 */    27,   27,   27,   27,   27,  170,  171,  172,   27,    0,
 /*   910 */   175,  176,   27,  178,  178,   26,    0,    0,   21,   20,
 /*   920 */   218,  218,  218,  218,  188,  189,  190,  218,  192,  218,
 /*   930 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   940 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   950 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   960 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   970 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   980 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   990 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1000 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1010 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1020 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1030 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1040 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1050 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
};
#define YY_SHIFT_COUNT    (289)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (917)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,  316,  320,  338,  338,  338,  338,  349,  338,  338,
 /*    10 */    13,  372,  474,  451,  474,  474,  474,  474,  474,  474,
 /*    20 */   474,  474,  474,  474,  474,  474,  474,  474,  474,  474,
 /*    30 */   474,  474,   97,   97,   97,  160,  160,    9,    9,  130,
 /*    40 */    25,   25,   63,   40,   25,   25,   40,   25,   40,   40,
 /*    50 */    40,   25,   79,    0,   16,   16,  135,  160,  376,  102,
 /*    60 */   102,  102,   60,  262,   71,   40,   40,  133,   70,  253,
 /*    70 */    30,  302,  247,   65,  295,   65,  389,  212,  304,  300,
 /*    80 */   385,  318,  333,  388,  388,  318,  333,  388,  380,  403,
 /*    90 */   427,  444,  454,   71,  548,  494,  542,  459,  481,  473,
 /*   100 */    40,  590,  532,  541,  541,  590,   63,  590,   63,  609,
 /*   110 */   609,  133,   79,   71,  601,  590,   79,  609,  929,  929,
 /*   120 */   929,   34,  366,  520,   32,   75,   75,   75,   75,   75,
 /*   130 */    75,   75,  487,  537,  651,  708,  434,   85,  500,  500,
 /*   140 */   500,  500,  480,  313,  342,  456,  392,  237,  292,  379,
 /*   150 */   369,  421,  424,  538,  471,  498,  513,  539,  540,  553,
 /*   160 */   556,  521,  612,  614,  557,  624,  628,  633,  629,  638,
 /*   170 */   646,  649,  650,  653,  654,  682,  655,  652,  660,  663,
 /*   180 */   595,  664,  665,  667,  618,  670,  673,  730,  642,  683,
 /*   190 */   690,  687,  630,  744,  722,  723,  724,  725,  729,  738,
 /*   200 */   699,  713,  767,  710,  769,  770,  680,  775,  776,  779,
 /*   210 */   782,  784,  785,  765,  787,  788,  789,  790,  794,  795,
 /*   220 */   764,  801,  803,  804,  805,  806,  786,  808,  792,  798,
 /*   230 */   756,  758,  762,  766,  802,  772,  771,  768,  799,  800,
 /*   240 */   780,  778,  812,  791,  796,  813,  807,  809,  814,  811,
 /*   250 */   815,  816,  817,  773,  777,  781,  783,  821,  797,  820,
 /*   260 */   822,  832,  834,  841,  831,  844,  852,  853,  855,  856,
 /*   270 */   857,  858,  862,  869,  866,  871,  872,  873,  874,  875,
 /*   280 */   876,  877,  881,  909,  885,  889,  916,  917,  897,  899,
};
#define YY_REDUCE_COUNT (120)
#define YY_REDUCE_MIN   (-178)
#define YY_REDUCE_MAX   (736)
static const short yy_reduce_ofst[] = {
 /*     0 */   214,  -11,   29,   74,  101,  121,  141, -152, -120,  277,
 /*    10 */   281,  321,  348,  378,  411,  430,  449,  469,  488,  505,
 /*    20 */   525,  545,  564,  583,  602,  621,  640,  659,  676,  696,
 /*    30 */   716,  735,   50,  501,  736, -135, -132, -155,  -66,  -43,
 /*    40 */   -33,   26,   22,   86,   49,  142, -158,  162,  169,  -97,
 /*    50 */   196,  215,  243, -178, -178, -178,   -3,  118,   77, -164,
 /*    60 */    15,   91, -147,  154,   36,  177,   67,  202,  -35,   35,
 /*    70 */    53,  114,  149,  147,  147,  147,  184,  164,  168,  250,
 /*    80 */   220,  218,  269,  314,  327,  283,  308,  336,  326,  340,
 /*    90 */   352,  362,  147,  415,  405,  393,  409,  386,  401,  406,
 /*   100 */   184,  477,  460,  464,  466,  490,  486,  491,  495,  502,
 /*   110 */   508,  499,  509,  496,  506,  510,  524,  516,  518,  519,
 /*   120 */   526,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*    10 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*    20 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*    30 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*    40 */   702,  702,  722,  702,  702,  702,  702,  702,  702,  702,
 /*    50 */   702,  702,  720,  702,  872,  702,  806,  702,  702,  883,
 /*    60 */   883,  883,  722,  720,  702,  702,  702,  783,  702,  947,
 /*    70 */   702,  907,  899,  875,  889,  876,  702,  932,  892,  702,
 /*    80 */   702,  914,  912,  702,  702,  914,  912,  702,  926,  922,
 /*    90 */   905,  903,  889,  702,  702,  702,  702,  950,  938,  934,
 /*   100 */   702,  702,  811,  808,  808,  702,  722,  702,  722,  702,
 /*   110 */   702,  702,  720,  702,  752,  702,  720,  702,  786,  786,
 /*   120 */   723,  702,  702,  702,  702,  925,  924,  849,  848,  847,
 /*   130 */   843,  844,  702,  702,  702,  702,  702,  702,  838,  839,
 /*   140 */   837,  836,  702,  702,  702,  873,  702,  702,  702,  935,
 /*   150 */   939,  702,  825,  702,  702,  702,  702,  702,  702,  702,
 /*   160 */   702,  702,  896,  906,  702,  702,  702,  702,  702,  702,
 /*   170 */   702,  702,  702,  702,  702,  702,  702,  825,  702,  923,
 /*   180 */   702,  882,  878,  702,  702,  874,  702,  868,  702,  702,
 /*   190 */   702,  933,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   200 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   210 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   220 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   230 */   702,  702,  824,  702,  702,  702,  702,  702,  702,  702,
 /*   240 */   780,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   250 */   702,  702,  702,  765,  763,  762,  761,  702,  758,  702,
 /*   260 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   270 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
 /*   280 */   702,  702,  702,  702,  702,  702,  702,  702,  702,  702,
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
  /*    1 */ "OR",
  /*    2 */ "AND",
  /*    3 */ "UNION",
  /*    4 */ "ALL",
  /*    5 */ "MINUS",
  /*    6 */ "EXCEPT",
  /*    7 */ "INTERSECT",
  /*    8 */ "NK_BITAND",
  /*    9 */ "NK_BITOR",
  /*   10 */ "NK_LSHIFT",
  /*   11 */ "NK_RSHIFT",
  /*   12 */ "NK_PLUS",
  /*   13 */ "NK_MINUS",
  /*   14 */ "NK_STAR",
  /*   15 */ "NK_SLASH",
  /*   16 */ "NK_REM",
  /*   17 */ "NK_CONCAT",
  /*   18 */ "CREATE",
  /*   19 */ "USER",
  /*   20 */ "PASS",
  /*   21 */ "NK_STRING",
  /*   22 */ "ALTER",
  /*   23 */ "PRIVILEGE",
  /*   24 */ "DROP",
  /*   25 */ "DNODE",
  /*   26 */ "PORT",
  /*   27 */ "NK_INTEGER",
  /*   28 */ "NK_ID",
  /*   29 */ "NK_IPTOKEN",
  /*   30 */ "DATABASE",
  /*   31 */ "USE",
  /*   32 */ "IF",
  /*   33 */ "NOT",
  /*   34 */ "EXISTS",
  /*   35 */ "BLOCKS",
  /*   36 */ "CACHE",
  /*   37 */ "CACHELAST",
  /*   38 */ "COMP",
  /*   39 */ "DAYS",
  /*   40 */ "FSYNC",
  /*   41 */ "MAXROWS",
  /*   42 */ "MINROWS",
  /*   43 */ "KEEP",
  /*   44 */ "PRECISION",
  /*   45 */ "QUORUM",
  /*   46 */ "REPLICA",
  /*   47 */ "TTL",
  /*   48 */ "WAL",
  /*   49 */ "VGROUPS",
  /*   50 */ "SINGLE_STABLE",
  /*   51 */ "STREAM_MODE",
  /*   52 */ "TABLE",
  /*   53 */ "NK_LP",
  /*   54 */ "NK_RP",
  /*   55 */ "STABLE",
  /*   56 */ "USING",
  /*   57 */ "TAGS",
  /*   58 */ "NK_DOT",
  /*   59 */ "NK_COMMA",
  /*   60 */ "COMMENT",
  /*   61 */ "BOOL",
  /*   62 */ "TINYINT",
  /*   63 */ "SMALLINT",
  /*   64 */ "INT",
  /*   65 */ "INTEGER",
  /*   66 */ "BIGINT",
  /*   67 */ "FLOAT",
  /*   68 */ "DOUBLE",
  /*   69 */ "BINARY",
  /*   70 */ "TIMESTAMP",
  /*   71 */ "NCHAR",
  /*   72 */ "UNSIGNED",
  /*   73 */ "JSON",
  /*   74 */ "VARCHAR",
  /*   75 */ "MEDIUMBLOB",
  /*   76 */ "BLOB",
  /*   77 */ "VARBINARY",
  /*   78 */ "DECIMAL",
  /*   79 */ "SMA",
  /*   80 */ "SHOW",
  /*   81 */ "DNODES",
  /*   82 */ "USERS",
  /*   83 */ "DATABASES",
  /*   84 */ "TABLES",
  /*   85 */ "STABLES",
  /*   86 */ "MNODES",
  /*   87 */ "MODULES",
  /*   88 */ "QNODES",
  /*   89 */ "FUNCTIONS",
  /*   90 */ "INDEXES",
  /*   91 */ "FROM",
  /*   92 */ "STREAMS",
  /*   93 */ "LIKE",
  /*   94 */ "NK_FLOAT",
  /*   95 */ "NK_BOOL",
  /*   96 */ "NK_VARIABLE",
  /*   97 */ "BETWEEN",
  /*   98 */ "IS",
  /*   99 */ "NULL",
  /*  100 */ "NK_LT",
  /*  101 */ "NK_GT",
  /*  102 */ "NK_LE",
  /*  103 */ "NK_GE",
  /*  104 */ "NK_NE",
  /*  105 */ "NK_EQ",
  /*  106 */ "MATCH",
  /*  107 */ "NMATCH",
  /*  108 */ "IN",
  /*  109 */ "AS",
  /*  110 */ "JOIN",
  /*  111 */ "ON",
  /*  112 */ "INNER",
  /*  113 */ "SELECT",
  /*  114 */ "DISTINCT",
  /*  115 */ "WHERE",
  /*  116 */ "PARTITION",
  /*  117 */ "BY",
  /*  118 */ "SESSION",
  /*  119 */ "STATE_WINDOW",
  /*  120 */ "INTERVAL",
  /*  121 */ "SLIDING",
  /*  122 */ "FILL",
  /*  123 */ "VALUE",
  /*  124 */ "NONE",
  /*  125 */ "PREV",
  /*  126 */ "LINEAR",
  /*  127 */ "NEXT",
  /*  128 */ "GROUP",
  /*  129 */ "HAVING",
  /*  130 */ "ORDER",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "ASC",
  /*  136 */ "DESC",
  /*  137 */ "NULLS",
  /*  138 */ "FIRST",
  /*  139 */ "LAST",
  /*  140 */ "cmd",
  /*  141 */ "user_name",
  /*  142 */ "dnode_endpoint",
  /*  143 */ "dnode_host_name",
  /*  144 */ "not_exists_opt",
  /*  145 */ "db_name",
  /*  146 */ "db_options",
  /*  147 */ "exists_opt",
  /*  148 */ "full_table_name",
  /*  149 */ "column_def_list",
  /*  150 */ "tags_def_opt",
  /*  151 */ "table_options",
  /*  152 */ "multi_create_clause",
  /*  153 */ "tags_def",
  /*  154 */ "multi_drop_clause",
  /*  155 */ "create_subtable_clause",
  /*  156 */ "specific_tags_opt",
  /*  157 */ "literal_list",
  /*  158 */ "drop_table_clause",
  /*  159 */ "col_name_list",
  /*  160 */ "table_name",
  /*  161 */ "column_def",
  /*  162 */ "column_name",
  /*  163 */ "type_name",
  /*  164 */ "col_name",
  /*  165 */ "db_name_cond_opt",
  /*  166 */ "like_pattern_opt",
  /*  167 */ "table_name_cond",
  /*  168 */ "from_db_opt",
  /*  169 */ "query_expression",
  /*  170 */ "literal",
  /*  171 */ "duration_literal",
  /*  172 */ "function_name",
  /*  173 */ "table_alias",
  /*  174 */ "column_alias",
  /*  175 */ "expression",
  /*  176 */ "column_reference",
  /*  177 */ "expression_list",
  /*  178 */ "subquery",
  /*  179 */ "predicate",
  /*  180 */ "compare_op",
  /*  181 */ "in_op",
  /*  182 */ "in_predicate_value",
  /*  183 */ "boolean_value_expression",
  /*  184 */ "boolean_primary",
  /*  185 */ "common_expression",
  /*  186 */ "from_clause",
  /*  187 */ "table_reference_list",
  /*  188 */ "table_reference",
  /*  189 */ "table_primary",
  /*  190 */ "joined_table",
  /*  191 */ "alias_opt",
  /*  192 */ "parenthesized_joined_table",
  /*  193 */ "join_type",
  /*  194 */ "search_condition",
  /*  195 */ "query_specification",
  /*  196 */ "set_quantifier_opt",
  /*  197 */ "select_list",
  /*  198 */ "where_clause_opt",
  /*  199 */ "partition_by_clause_opt",
  /*  200 */ "twindow_clause_opt",
  /*  201 */ "group_by_clause_opt",
  /*  202 */ "having_clause_opt",
  /*  203 */ "select_sublist",
  /*  204 */ "select_item",
  /*  205 */ "sliding_opt",
  /*  206 */ "fill_opt",
  /*  207 */ "fill_mode",
  /*  208 */ "group_by_list",
  /*  209 */ "query_expression_body",
  /*  210 */ "order_by_clause_opt",
  /*  211 */ "slimit_clause_opt",
  /*  212 */ "limit_clause_opt",
  /*  213 */ "query_primary",
  /*  214 */ "sort_specification_list",
  /*  215 */ "sort_specification",
  /*  216 */ "ordering_specification_opt",
  /*  217 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE USER user_name PASS NK_STRING",
 /*   1 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*   2 */ "cmd ::= ALTER USER user_name PRIVILEGE NK_STRING",
 /*   3 */ "cmd ::= DROP USER user_name",
 /*   4 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*   5 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*   6 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*   7 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*   8 */ "dnode_endpoint ::= NK_STRING",
 /*   9 */ "dnode_host_name ::= NK_ID",
 /*  10 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  11 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  12 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  13 */ "cmd ::= USE db_name",
 /*  14 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  15 */ "not_exists_opt ::=",
 /*  16 */ "exists_opt ::= IF EXISTS",
 /*  17 */ "exists_opt ::=",
 /*  18 */ "db_options ::=",
 /*  19 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  20 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  21 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  22 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  23 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  24 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  25 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  26 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  27 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  28 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  29 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  30 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  31 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  32 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  33 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  34 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  35 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  36 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  37 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  38 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  39 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  40 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  41 */ "multi_create_clause ::= create_subtable_clause",
 /*  42 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  43 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  44 */ "multi_drop_clause ::= drop_table_clause",
 /*  45 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  46 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  47 */ "specific_tags_opt ::=",
 /*  48 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  49 */ "full_table_name ::= table_name",
 /*  50 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  51 */ "column_def_list ::= column_def",
 /*  52 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  53 */ "column_def ::= column_name type_name",
 /*  54 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  55 */ "type_name ::= BOOL",
 /*  56 */ "type_name ::= TINYINT",
 /*  57 */ "type_name ::= SMALLINT",
 /*  58 */ "type_name ::= INT",
 /*  59 */ "type_name ::= INTEGER",
 /*  60 */ "type_name ::= BIGINT",
 /*  61 */ "type_name ::= FLOAT",
 /*  62 */ "type_name ::= DOUBLE",
 /*  63 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  64 */ "type_name ::= TIMESTAMP",
 /*  65 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  66 */ "type_name ::= TINYINT UNSIGNED",
 /*  67 */ "type_name ::= SMALLINT UNSIGNED",
 /*  68 */ "type_name ::= INT UNSIGNED",
 /*  69 */ "type_name ::= BIGINT UNSIGNED",
 /*  70 */ "type_name ::= JSON",
 /*  71 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  72 */ "type_name ::= MEDIUMBLOB",
 /*  73 */ "type_name ::= BLOB",
 /*  74 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  75 */ "type_name ::= DECIMAL",
 /*  76 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  77 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  78 */ "tags_def_opt ::=",
 /*  79 */ "tags_def_opt ::= tags_def",
 /*  80 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  81 */ "table_options ::=",
 /*  82 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  83 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  84 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  85 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  86 */ "col_name_list ::= col_name",
 /*  87 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  88 */ "col_name ::= column_name",
 /*  89 */ "cmd ::= SHOW DNODES",
 /*  90 */ "cmd ::= SHOW USERS",
 /*  91 */ "cmd ::= SHOW DATABASES",
 /*  92 */ "cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt",
 /*  93 */ "cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt",
 /*  94 */ "cmd ::= SHOW db_name_cond_opt VGROUPS",
 /*  95 */ "cmd ::= SHOW MNODES",
 /*  96 */ "cmd ::= SHOW MODULES",
 /*  97 */ "cmd ::= SHOW QNODES",
 /*  98 */ "cmd ::= SHOW FUNCTIONS",
 /*  99 */ "cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt",
 /* 100 */ "cmd ::= SHOW STREAMS",
 /* 101 */ "db_name_cond_opt ::=",
 /* 102 */ "db_name_cond_opt ::= db_name NK_DOT",
 /* 103 */ "like_pattern_opt ::=",
 /* 104 */ "like_pattern_opt ::= LIKE NK_STRING",
 /* 105 */ "table_name_cond ::= table_name",
 /* 106 */ "from_db_opt ::=",
 /* 107 */ "from_db_opt ::= FROM db_name",
 /* 108 */ "cmd ::= query_expression",
 /* 109 */ "literal ::= NK_INTEGER",
 /* 110 */ "literal ::= NK_FLOAT",
 /* 111 */ "literal ::= NK_STRING",
 /* 112 */ "literal ::= NK_BOOL",
 /* 113 */ "literal ::= TIMESTAMP NK_STRING",
 /* 114 */ "literal ::= duration_literal",
 /* 115 */ "duration_literal ::= NK_VARIABLE",
 /* 116 */ "literal_list ::= literal",
 /* 117 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 118 */ "db_name ::= NK_ID",
 /* 119 */ "table_name ::= NK_ID",
 /* 120 */ "column_name ::= NK_ID",
 /* 121 */ "function_name ::= NK_ID",
 /* 122 */ "table_alias ::= NK_ID",
 /* 123 */ "column_alias ::= NK_ID",
 /* 124 */ "user_name ::= NK_ID",
 /* 125 */ "expression ::= literal",
 /* 126 */ "expression ::= column_reference",
 /* 127 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 128 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 129 */ "expression ::= subquery",
 /* 130 */ "expression ::= NK_LP expression NK_RP",
 /* 131 */ "expression ::= NK_PLUS expression",
 /* 132 */ "expression ::= NK_MINUS expression",
 /* 133 */ "expression ::= expression NK_PLUS expression",
 /* 134 */ "expression ::= expression NK_MINUS expression",
 /* 135 */ "expression ::= expression NK_STAR expression",
 /* 136 */ "expression ::= expression NK_SLASH expression",
 /* 137 */ "expression ::= expression NK_REM expression",
 /* 138 */ "expression_list ::= expression",
 /* 139 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 140 */ "column_reference ::= column_name",
 /* 141 */ "column_reference ::= table_name NK_DOT column_name",
 /* 142 */ "predicate ::= expression compare_op expression",
 /* 143 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 144 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 145 */ "predicate ::= expression IS NULL",
 /* 146 */ "predicate ::= expression IS NOT NULL",
 /* 147 */ "predicate ::= expression in_op in_predicate_value",
 /* 148 */ "compare_op ::= NK_LT",
 /* 149 */ "compare_op ::= NK_GT",
 /* 150 */ "compare_op ::= NK_LE",
 /* 151 */ "compare_op ::= NK_GE",
 /* 152 */ "compare_op ::= NK_NE",
 /* 153 */ "compare_op ::= NK_EQ",
 /* 154 */ "compare_op ::= LIKE",
 /* 155 */ "compare_op ::= NOT LIKE",
 /* 156 */ "compare_op ::= MATCH",
 /* 157 */ "compare_op ::= NMATCH",
 /* 158 */ "in_op ::= IN",
 /* 159 */ "in_op ::= NOT IN",
 /* 160 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 161 */ "boolean_value_expression ::= boolean_primary",
 /* 162 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 163 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 164 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 165 */ "boolean_primary ::= predicate",
 /* 166 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 167 */ "common_expression ::= expression",
 /* 168 */ "common_expression ::= boolean_value_expression",
 /* 169 */ "from_clause ::= FROM table_reference_list",
 /* 170 */ "table_reference_list ::= table_reference",
 /* 171 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 172 */ "table_reference ::= table_primary",
 /* 173 */ "table_reference ::= joined_table",
 /* 174 */ "table_primary ::= table_name alias_opt",
 /* 175 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 176 */ "table_primary ::= subquery alias_opt",
 /* 177 */ "table_primary ::= parenthesized_joined_table",
 /* 178 */ "alias_opt ::=",
 /* 179 */ "alias_opt ::= table_alias",
 /* 180 */ "alias_opt ::= AS table_alias",
 /* 181 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 182 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 183 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 184 */ "join_type ::=",
 /* 185 */ "join_type ::= INNER",
 /* 186 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 187 */ "set_quantifier_opt ::=",
 /* 188 */ "set_quantifier_opt ::= DISTINCT",
 /* 189 */ "set_quantifier_opt ::= ALL",
 /* 190 */ "select_list ::= NK_STAR",
 /* 191 */ "select_list ::= select_sublist",
 /* 192 */ "select_sublist ::= select_item",
 /* 193 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 194 */ "select_item ::= common_expression",
 /* 195 */ "select_item ::= common_expression column_alias",
 /* 196 */ "select_item ::= common_expression AS column_alias",
 /* 197 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 198 */ "where_clause_opt ::=",
 /* 199 */ "where_clause_opt ::= WHERE search_condition",
 /* 200 */ "partition_by_clause_opt ::=",
 /* 201 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 202 */ "twindow_clause_opt ::=",
 /* 203 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 204 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 205 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 206 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 207 */ "sliding_opt ::=",
 /* 208 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 209 */ "fill_opt ::=",
 /* 210 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 211 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 212 */ "fill_mode ::= NONE",
 /* 213 */ "fill_mode ::= PREV",
 /* 214 */ "fill_mode ::= NULL",
 /* 215 */ "fill_mode ::= LINEAR",
 /* 216 */ "fill_mode ::= NEXT",
 /* 217 */ "group_by_clause_opt ::=",
 /* 218 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 219 */ "group_by_list ::= expression",
 /* 220 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 221 */ "having_clause_opt ::=",
 /* 222 */ "having_clause_opt ::= HAVING search_condition",
 /* 223 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 224 */ "query_expression_body ::= query_primary",
 /* 225 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 226 */ "query_primary ::= query_specification",
 /* 227 */ "order_by_clause_opt ::=",
 /* 228 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 229 */ "slimit_clause_opt ::=",
 /* 230 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 231 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 232 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 233 */ "limit_clause_opt ::=",
 /* 234 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 235 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 236 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 237 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 238 */ "search_condition ::= common_expression",
 /* 239 */ "sort_specification_list ::= sort_specification",
 /* 240 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 241 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 242 */ "ordering_specification_opt ::=",
 /* 243 */ "ordering_specification_opt ::= ASC",
 /* 244 */ "ordering_specification_opt ::= DESC",
 /* 245 */ "null_ordering_opt ::=",
 /* 246 */ "null_ordering_opt ::= NULLS FIRST",
 /* 247 */ "null_ordering_opt ::= NULLS LAST",
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
      /* Default NON-TERMINAL Destructor */
    case 140: /* cmd */
    case 148: /* full_table_name */
    case 155: /* create_subtable_clause */
    case 158: /* drop_table_clause */
    case 161: /* column_def */
    case 164: /* col_name */
    case 165: /* db_name_cond_opt */
    case 166: /* like_pattern_opt */
    case 167: /* table_name_cond */
    case 168: /* from_db_opt */
    case 169: /* query_expression */
    case 170: /* literal */
    case 171: /* duration_literal */
    case 175: /* expression */
    case 176: /* column_reference */
    case 178: /* subquery */
    case 179: /* predicate */
    case 182: /* in_predicate_value */
    case 183: /* boolean_value_expression */
    case 184: /* boolean_primary */
    case 185: /* common_expression */
    case 186: /* from_clause */
    case 187: /* table_reference_list */
    case 188: /* table_reference */
    case 189: /* table_primary */
    case 190: /* joined_table */
    case 192: /* parenthesized_joined_table */
    case 194: /* search_condition */
    case 195: /* query_specification */
    case 198: /* where_clause_opt */
    case 200: /* twindow_clause_opt */
    case 202: /* having_clause_opt */
    case 204: /* select_item */
    case 205: /* sliding_opt */
    case 206: /* fill_opt */
    case 209: /* query_expression_body */
    case 211: /* slimit_clause_opt */
    case 212: /* limit_clause_opt */
    case 213: /* query_primary */
    case 215: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy68)); 
}
      break;
    case 141: /* user_name */
    case 142: /* dnode_endpoint */
    case 143: /* dnode_host_name */
    case 145: /* db_name */
    case 160: /* table_name */
    case 162: /* column_name */
    case 172: /* function_name */
    case 173: /* table_alias */
    case 174: /* column_alias */
    case 191: /* alias_opt */
{
 
}
      break;
    case 144: /* not_exists_opt */
    case 147: /* exists_opt */
    case 196: /* set_quantifier_opt */
{
 
}
      break;
    case 146: /* db_options */
{
 tfree((yypminor->yy339)); 
}
      break;
    case 149: /* column_def_list */
    case 150: /* tags_def_opt */
    case 152: /* multi_create_clause */
    case 153: /* tags_def */
    case 154: /* multi_drop_clause */
    case 156: /* specific_tags_opt */
    case 157: /* literal_list */
    case 159: /* col_name_list */
    case 177: /* expression_list */
    case 197: /* select_list */
    case 199: /* partition_by_clause_opt */
    case 201: /* group_by_clause_opt */
    case 203: /* select_sublist */
    case 208: /* group_by_list */
    case 210: /* order_by_clause_opt */
    case 214: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy40)); 
}
      break;
    case 151: /* table_options */
{
 tfree((yypminor->yy418)); 
}
      break;
    case 163: /* type_name */
{
 
}
      break;
    case 180: /* compare_op */
    case 181: /* in_op */
{
 
}
      break;
    case 193: /* join_type */
{
 
}
      break;
    case 207: /* fill_mode */
{
 
}
      break;
    case 216: /* ordering_specification_opt */
{
 
}
      break;
    case 217: /* null_ordering_opt */
{
 
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
  {  140,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  140,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  140,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  140,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  140,   -3 }, /* (4) cmd ::= CREATE DNODE dnode_endpoint */
  {  140,   -5 }, /* (5) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  140,   -3 }, /* (6) cmd ::= DROP DNODE NK_INTEGER */
  {  140,   -3 }, /* (7) cmd ::= DROP DNODE dnode_endpoint */
  {  142,   -1 }, /* (8) dnode_endpoint ::= NK_STRING */
  {  143,   -1 }, /* (9) dnode_host_name ::= NK_ID */
  {  143,   -1 }, /* (10) dnode_host_name ::= NK_IPTOKEN */
  {  140,   -5 }, /* (11) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  140,   -4 }, /* (12) cmd ::= DROP DATABASE exists_opt db_name */
  {  140,   -2 }, /* (13) cmd ::= USE db_name */
  {  144,   -3 }, /* (14) not_exists_opt ::= IF NOT EXISTS */
  {  144,    0 }, /* (15) not_exists_opt ::= */
  {  147,   -2 }, /* (16) exists_opt ::= IF EXISTS */
  {  147,    0 }, /* (17) exists_opt ::= */
  {  146,    0 }, /* (18) db_options ::= */
  {  146,   -3 }, /* (19) db_options ::= db_options BLOCKS NK_INTEGER */
  {  146,   -3 }, /* (20) db_options ::= db_options CACHE NK_INTEGER */
  {  146,   -3 }, /* (21) db_options ::= db_options CACHELAST NK_INTEGER */
  {  146,   -3 }, /* (22) db_options ::= db_options COMP NK_INTEGER */
  {  146,   -3 }, /* (23) db_options ::= db_options DAYS NK_INTEGER */
  {  146,   -3 }, /* (24) db_options ::= db_options FSYNC NK_INTEGER */
  {  146,   -3 }, /* (25) db_options ::= db_options MAXROWS NK_INTEGER */
  {  146,   -3 }, /* (26) db_options ::= db_options MINROWS NK_INTEGER */
  {  146,   -3 }, /* (27) db_options ::= db_options KEEP NK_INTEGER */
  {  146,   -3 }, /* (28) db_options ::= db_options PRECISION NK_STRING */
  {  146,   -3 }, /* (29) db_options ::= db_options QUORUM NK_INTEGER */
  {  146,   -3 }, /* (30) db_options ::= db_options REPLICA NK_INTEGER */
  {  146,   -3 }, /* (31) db_options ::= db_options TTL NK_INTEGER */
  {  146,   -3 }, /* (32) db_options ::= db_options WAL NK_INTEGER */
  {  146,   -3 }, /* (33) db_options ::= db_options VGROUPS NK_INTEGER */
  {  146,   -3 }, /* (34) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  146,   -3 }, /* (35) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  140,   -9 }, /* (36) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  140,   -3 }, /* (37) cmd ::= CREATE TABLE multi_create_clause */
  {  140,   -9 }, /* (38) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  140,   -3 }, /* (39) cmd ::= DROP TABLE multi_drop_clause */
  {  140,   -4 }, /* (40) cmd ::= DROP STABLE exists_opt full_table_name */
  {  152,   -1 }, /* (41) multi_create_clause ::= create_subtable_clause */
  {  152,   -2 }, /* (42) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  155,   -9 }, /* (43) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  154,   -1 }, /* (44) multi_drop_clause ::= drop_table_clause */
  {  154,   -2 }, /* (45) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  158,   -2 }, /* (46) drop_table_clause ::= exists_opt full_table_name */
  {  156,    0 }, /* (47) specific_tags_opt ::= */
  {  156,   -3 }, /* (48) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  148,   -1 }, /* (49) full_table_name ::= table_name */
  {  148,   -3 }, /* (50) full_table_name ::= db_name NK_DOT table_name */
  {  149,   -1 }, /* (51) column_def_list ::= column_def */
  {  149,   -3 }, /* (52) column_def_list ::= column_def_list NK_COMMA column_def */
  {  161,   -2 }, /* (53) column_def ::= column_name type_name */
  {  161,   -4 }, /* (54) column_def ::= column_name type_name COMMENT NK_STRING */
  {  163,   -1 }, /* (55) type_name ::= BOOL */
  {  163,   -1 }, /* (56) type_name ::= TINYINT */
  {  163,   -1 }, /* (57) type_name ::= SMALLINT */
  {  163,   -1 }, /* (58) type_name ::= INT */
  {  163,   -1 }, /* (59) type_name ::= INTEGER */
  {  163,   -1 }, /* (60) type_name ::= BIGINT */
  {  163,   -1 }, /* (61) type_name ::= FLOAT */
  {  163,   -1 }, /* (62) type_name ::= DOUBLE */
  {  163,   -4 }, /* (63) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (64) type_name ::= TIMESTAMP */
  {  163,   -4 }, /* (65) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  163,   -2 }, /* (66) type_name ::= TINYINT UNSIGNED */
  {  163,   -2 }, /* (67) type_name ::= SMALLINT UNSIGNED */
  {  163,   -2 }, /* (68) type_name ::= INT UNSIGNED */
  {  163,   -2 }, /* (69) type_name ::= BIGINT UNSIGNED */
  {  163,   -1 }, /* (70) type_name ::= JSON */
  {  163,   -4 }, /* (71) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (72) type_name ::= MEDIUMBLOB */
  {  163,   -1 }, /* (73) type_name ::= BLOB */
  {  163,   -4 }, /* (74) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (75) type_name ::= DECIMAL */
  {  163,   -4 }, /* (76) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  163,   -6 }, /* (77) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  150,    0 }, /* (78) tags_def_opt ::= */
  {  150,   -1 }, /* (79) tags_def_opt ::= tags_def */
  {  153,   -4 }, /* (80) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  151,    0 }, /* (81) table_options ::= */
  {  151,   -3 }, /* (82) table_options ::= table_options COMMENT NK_STRING */
  {  151,   -3 }, /* (83) table_options ::= table_options KEEP NK_INTEGER */
  {  151,   -3 }, /* (84) table_options ::= table_options TTL NK_INTEGER */
  {  151,   -5 }, /* (85) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  159,   -1 }, /* (86) col_name_list ::= col_name */
  {  159,   -3 }, /* (87) col_name_list ::= col_name_list NK_COMMA col_name */
  {  164,   -1 }, /* (88) col_name ::= column_name */
  {  140,   -2 }, /* (89) cmd ::= SHOW DNODES */
  {  140,   -2 }, /* (90) cmd ::= SHOW USERS */
  {  140,   -2 }, /* (91) cmd ::= SHOW DATABASES */
  {  140,   -4 }, /* (92) cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt */
  {  140,   -4 }, /* (93) cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
  {  140,   -3 }, /* (94) cmd ::= SHOW db_name_cond_opt VGROUPS */
  {  140,   -2 }, /* (95) cmd ::= SHOW MNODES */
  {  140,   -2 }, /* (96) cmd ::= SHOW MODULES */
  {  140,   -2 }, /* (97) cmd ::= SHOW QNODES */
  {  140,   -2 }, /* (98) cmd ::= SHOW FUNCTIONS */
  {  140,   -5 }, /* (99) cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
  {  140,   -2 }, /* (100) cmd ::= SHOW STREAMS */
  {  165,    0 }, /* (101) db_name_cond_opt ::= */
  {  165,   -2 }, /* (102) db_name_cond_opt ::= db_name NK_DOT */
  {  166,    0 }, /* (103) like_pattern_opt ::= */
  {  166,   -2 }, /* (104) like_pattern_opt ::= LIKE NK_STRING */
  {  167,   -1 }, /* (105) table_name_cond ::= table_name */
  {  168,    0 }, /* (106) from_db_opt ::= */
  {  168,   -2 }, /* (107) from_db_opt ::= FROM db_name */
  {  140,   -1 }, /* (108) cmd ::= query_expression */
  {  170,   -1 }, /* (109) literal ::= NK_INTEGER */
  {  170,   -1 }, /* (110) literal ::= NK_FLOAT */
  {  170,   -1 }, /* (111) literal ::= NK_STRING */
  {  170,   -1 }, /* (112) literal ::= NK_BOOL */
  {  170,   -2 }, /* (113) literal ::= TIMESTAMP NK_STRING */
  {  170,   -1 }, /* (114) literal ::= duration_literal */
  {  171,   -1 }, /* (115) duration_literal ::= NK_VARIABLE */
  {  157,   -1 }, /* (116) literal_list ::= literal */
  {  157,   -3 }, /* (117) literal_list ::= literal_list NK_COMMA literal */
  {  145,   -1 }, /* (118) db_name ::= NK_ID */
  {  160,   -1 }, /* (119) table_name ::= NK_ID */
  {  162,   -1 }, /* (120) column_name ::= NK_ID */
  {  172,   -1 }, /* (121) function_name ::= NK_ID */
  {  173,   -1 }, /* (122) table_alias ::= NK_ID */
  {  174,   -1 }, /* (123) column_alias ::= NK_ID */
  {  141,   -1 }, /* (124) user_name ::= NK_ID */
  {  175,   -1 }, /* (125) expression ::= literal */
  {  175,   -1 }, /* (126) expression ::= column_reference */
  {  175,   -4 }, /* (127) expression ::= function_name NK_LP expression_list NK_RP */
  {  175,   -4 }, /* (128) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  175,   -1 }, /* (129) expression ::= subquery */
  {  175,   -3 }, /* (130) expression ::= NK_LP expression NK_RP */
  {  175,   -2 }, /* (131) expression ::= NK_PLUS expression */
  {  175,   -2 }, /* (132) expression ::= NK_MINUS expression */
  {  175,   -3 }, /* (133) expression ::= expression NK_PLUS expression */
  {  175,   -3 }, /* (134) expression ::= expression NK_MINUS expression */
  {  175,   -3 }, /* (135) expression ::= expression NK_STAR expression */
  {  175,   -3 }, /* (136) expression ::= expression NK_SLASH expression */
  {  175,   -3 }, /* (137) expression ::= expression NK_REM expression */
  {  177,   -1 }, /* (138) expression_list ::= expression */
  {  177,   -3 }, /* (139) expression_list ::= expression_list NK_COMMA expression */
  {  176,   -1 }, /* (140) column_reference ::= column_name */
  {  176,   -3 }, /* (141) column_reference ::= table_name NK_DOT column_name */
  {  179,   -3 }, /* (142) predicate ::= expression compare_op expression */
  {  179,   -5 }, /* (143) predicate ::= expression BETWEEN expression AND expression */
  {  179,   -6 }, /* (144) predicate ::= expression NOT BETWEEN expression AND expression */
  {  179,   -3 }, /* (145) predicate ::= expression IS NULL */
  {  179,   -4 }, /* (146) predicate ::= expression IS NOT NULL */
  {  179,   -3 }, /* (147) predicate ::= expression in_op in_predicate_value */
  {  180,   -1 }, /* (148) compare_op ::= NK_LT */
  {  180,   -1 }, /* (149) compare_op ::= NK_GT */
  {  180,   -1 }, /* (150) compare_op ::= NK_LE */
  {  180,   -1 }, /* (151) compare_op ::= NK_GE */
  {  180,   -1 }, /* (152) compare_op ::= NK_NE */
  {  180,   -1 }, /* (153) compare_op ::= NK_EQ */
  {  180,   -1 }, /* (154) compare_op ::= LIKE */
  {  180,   -2 }, /* (155) compare_op ::= NOT LIKE */
  {  180,   -1 }, /* (156) compare_op ::= MATCH */
  {  180,   -1 }, /* (157) compare_op ::= NMATCH */
  {  181,   -1 }, /* (158) in_op ::= IN */
  {  181,   -2 }, /* (159) in_op ::= NOT IN */
  {  182,   -3 }, /* (160) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  183,   -1 }, /* (161) boolean_value_expression ::= boolean_primary */
  {  183,   -2 }, /* (162) boolean_value_expression ::= NOT boolean_primary */
  {  183,   -3 }, /* (163) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  183,   -3 }, /* (164) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  184,   -1 }, /* (165) boolean_primary ::= predicate */
  {  184,   -3 }, /* (166) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  185,   -1 }, /* (167) common_expression ::= expression */
  {  185,   -1 }, /* (168) common_expression ::= boolean_value_expression */
  {  186,   -2 }, /* (169) from_clause ::= FROM table_reference_list */
  {  187,   -1 }, /* (170) table_reference_list ::= table_reference */
  {  187,   -3 }, /* (171) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  188,   -1 }, /* (172) table_reference ::= table_primary */
  {  188,   -1 }, /* (173) table_reference ::= joined_table */
  {  189,   -2 }, /* (174) table_primary ::= table_name alias_opt */
  {  189,   -4 }, /* (175) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  189,   -2 }, /* (176) table_primary ::= subquery alias_opt */
  {  189,   -1 }, /* (177) table_primary ::= parenthesized_joined_table */
  {  191,    0 }, /* (178) alias_opt ::= */
  {  191,   -1 }, /* (179) alias_opt ::= table_alias */
  {  191,   -2 }, /* (180) alias_opt ::= AS table_alias */
  {  192,   -3 }, /* (181) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  192,   -3 }, /* (182) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  190,   -6 }, /* (183) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  193,    0 }, /* (184) join_type ::= */
  {  193,   -1 }, /* (185) join_type ::= INNER */
  {  195,   -9 }, /* (186) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  196,    0 }, /* (187) set_quantifier_opt ::= */
  {  196,   -1 }, /* (188) set_quantifier_opt ::= DISTINCT */
  {  196,   -1 }, /* (189) set_quantifier_opt ::= ALL */
  {  197,   -1 }, /* (190) select_list ::= NK_STAR */
  {  197,   -1 }, /* (191) select_list ::= select_sublist */
  {  203,   -1 }, /* (192) select_sublist ::= select_item */
  {  203,   -3 }, /* (193) select_sublist ::= select_sublist NK_COMMA select_item */
  {  204,   -1 }, /* (194) select_item ::= common_expression */
  {  204,   -2 }, /* (195) select_item ::= common_expression column_alias */
  {  204,   -3 }, /* (196) select_item ::= common_expression AS column_alias */
  {  204,   -3 }, /* (197) select_item ::= table_name NK_DOT NK_STAR */
  {  198,    0 }, /* (198) where_clause_opt ::= */
  {  198,   -2 }, /* (199) where_clause_opt ::= WHERE search_condition */
  {  199,    0 }, /* (200) partition_by_clause_opt ::= */
  {  199,   -3 }, /* (201) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  200,    0 }, /* (202) twindow_clause_opt ::= */
  {  200,   -6 }, /* (203) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  200,   -4 }, /* (204) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  200,   -6 }, /* (205) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  200,   -8 }, /* (206) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  205,    0 }, /* (207) sliding_opt ::= */
  {  205,   -4 }, /* (208) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  206,    0 }, /* (209) fill_opt ::= */
  {  206,   -4 }, /* (210) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  206,   -6 }, /* (211) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  207,   -1 }, /* (212) fill_mode ::= NONE */
  {  207,   -1 }, /* (213) fill_mode ::= PREV */
  {  207,   -1 }, /* (214) fill_mode ::= NULL */
  {  207,   -1 }, /* (215) fill_mode ::= LINEAR */
  {  207,   -1 }, /* (216) fill_mode ::= NEXT */
  {  201,    0 }, /* (217) group_by_clause_opt ::= */
  {  201,   -3 }, /* (218) group_by_clause_opt ::= GROUP BY group_by_list */
  {  208,   -1 }, /* (219) group_by_list ::= expression */
  {  208,   -3 }, /* (220) group_by_list ::= group_by_list NK_COMMA expression */
  {  202,    0 }, /* (221) having_clause_opt ::= */
  {  202,   -2 }, /* (222) having_clause_opt ::= HAVING search_condition */
  {  169,   -4 }, /* (223) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  209,   -1 }, /* (224) query_expression_body ::= query_primary */
  {  209,   -4 }, /* (225) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  213,   -1 }, /* (226) query_primary ::= query_specification */
  {  210,    0 }, /* (227) order_by_clause_opt ::= */
  {  210,   -3 }, /* (228) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  211,    0 }, /* (229) slimit_clause_opt ::= */
  {  211,   -2 }, /* (230) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  211,   -4 }, /* (231) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  211,   -4 }, /* (232) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  212,    0 }, /* (233) limit_clause_opt ::= */
  {  212,   -2 }, /* (234) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  212,   -4 }, /* (235) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  212,   -4 }, /* (236) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  178,   -3 }, /* (237) subquery ::= NK_LP query_expression NK_RP */
  {  194,   -1 }, /* (238) search_condition ::= common_expression */
  {  214,   -1 }, /* (239) sort_specification_list ::= sort_specification */
  {  214,   -3 }, /* (240) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  215,   -3 }, /* (241) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  216,    0 }, /* (242) ordering_specification_opt ::= */
  {  216,   -1 }, /* (243) ordering_specification_opt ::= ASC */
  {  216,   -1 }, /* (244) ordering_specification_opt ::= DESC */
  {  217,    0 }, /* (245) null_ordering_opt ::= */
  {  217,   -2 }, /* (246) null_ordering_opt ::= NULLS FIRST */
  {  217,   -2 }, /* (247) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0);}
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy5, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0);}
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy5, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0);}
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy5); }
        break;
      case 4: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy5, NULL);}
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0);}
        break;
      case 6: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0);}
        break;
      case 7: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy5);}
        break;
      case 8: /* dnode_endpoint ::= NK_STRING */
      case 9: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==9);
      case 10: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==10);
      case 118: /* db_name ::= NK_ID */ yytestcase(yyruleno==118);
      case 119: /* table_name ::= NK_ID */ yytestcase(yyruleno==119);
      case 120: /* column_name ::= NK_ID */ yytestcase(yyruleno==120);
      case 121: /* function_name ::= NK_ID */ yytestcase(yyruleno==121);
      case 122: /* table_alias ::= NK_ID */ yytestcase(yyruleno==122);
      case 123: /* column_alias ::= NK_ID */ yytestcase(yyruleno==123);
      case 124: /* user_name ::= NK_ID */ yytestcase(yyruleno==124);
{ yylhsminor.yy5 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy5 = yylhsminor.yy5;
        break;
      case 11: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy25, &yymsp[-1].minor.yy5, yymsp[0].minor.yy339);}
        break;
      case 12: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy25, &yymsp[0].minor.yy5); }
        break;
      case 13: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy5);}
        break;
      case 14: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy25 = true; }
        break;
      case 15: /* not_exists_opt ::= */
      case 17: /* exists_opt ::= */ yytestcase(yyruleno==17);
      case 187: /* set_quantifier_opt ::= */ yytestcase(yyruleno==187);
{ yymsp[1].minor.yy25 = false; }
        break;
      case 16: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy25 = true; }
        break;
      case 18: /* db_options ::= */
{ yymsp[1].minor.yy339 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 19: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 20: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 21: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 22: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 23: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 24: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 25: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 26: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 27: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 28: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 29: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 30: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 31: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 32: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 33: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 34: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 35: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy339 = setDatabaseOption(pCxt, yymsp[-2].minor.yy339, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy339 = yylhsminor.yy339;
        break;
      case 36: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 38: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==38);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy25, yymsp[-5].minor.yy68, yymsp[-3].minor.yy40, yymsp[-1].minor.yy40, yymsp[0].minor.yy418);}
        break;
      case 37: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy40);}
        break;
      case 39: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy40); }
        break;
      case 40: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy25, yymsp[0].minor.yy68); }
        break;
      case 41: /* multi_create_clause ::= create_subtable_clause */
      case 44: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==44);
      case 51: /* column_def_list ::= column_def */ yytestcase(yyruleno==51);
      case 86: /* col_name_list ::= col_name */ yytestcase(yyruleno==86);
      case 192: /* select_sublist ::= select_item */ yytestcase(yyruleno==192);
      case 239: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==239);
{ yylhsminor.yy40 = createNodeList(pCxt, yymsp[0].minor.yy68); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 42: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 45: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==45);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-1].minor.yy40, yymsp[0].minor.yy68); }
  yymsp[-1].minor.yy40 = yylhsminor.yy40;
        break;
      case 43: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy68 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy25, yymsp[-7].minor.yy68, yymsp[-5].minor.yy68, yymsp[-4].minor.yy40, yymsp[-1].minor.yy40); }
  yymsp[-8].minor.yy68 = yylhsminor.yy68;
        break;
      case 46: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy68 = createDropTableClause(pCxt, yymsp[-1].minor.yy25, yymsp[0].minor.yy68); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 47: /* specific_tags_opt ::= */
      case 78: /* tags_def_opt ::= */ yytestcase(yyruleno==78);
      case 200: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==200);
      case 217: /* group_by_clause_opt ::= */ yytestcase(yyruleno==217);
      case 227: /* order_by_clause_opt ::= */ yytestcase(yyruleno==227);
{ yymsp[1].minor.yy40 = NULL; }
        break;
      case 48: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy40 = yymsp[-1].minor.yy40; }
        break;
      case 49: /* full_table_name ::= table_name */
{ yylhsminor.yy68 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy5, NULL); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 50: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy68 = createRealTableNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5, NULL); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 52: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 87: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==87);
      case 193: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==193);
      case 240: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==240);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, yymsp[0].minor.yy68); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 53: /* column_def ::= column_name type_name */
{ yylhsminor.yy68 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy5, yymsp[0].minor.yy372, NULL); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 54: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy68 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy5, yymsp[-2].minor.yy372, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 55: /* type_name ::= BOOL */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 56: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 57: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 58: /* type_name ::= INT */
      case 59: /* type_name ::= INTEGER */ yytestcase(yyruleno==59);
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 60: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 61: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 62: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 63: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 64: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 65: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 66: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 67: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 68: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 69: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 70: /* type_name ::= JSON */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 71: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 72: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 73: /* type_name ::= BLOB */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 74: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 75: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 76: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 77: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 79: /* tags_def_opt ::= tags_def */
      case 191: /* select_list ::= select_sublist */ yytestcase(yyruleno==191);
{ yylhsminor.yy40 = yymsp[0].minor.yy40; }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 80: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy40 = yymsp[-1].minor.yy40; }
        break;
      case 81: /* table_options ::= */
{ yymsp[1].minor.yy418 = createDefaultTableOptions(pCxt);}
        break;
      case 82: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy418 = setTableOption(pCxt, yymsp[-2].minor.yy418, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 83: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy418 = setTableOption(pCxt, yymsp[-2].minor.yy418, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 84: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy418 = setTableOption(pCxt, yymsp[-2].minor.yy418, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 85: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy418 = setTableSmaOption(pCxt, yymsp[-4].minor.yy418, yymsp[-1].minor.yy40); }
  yymsp[-4].minor.yy418 = yylhsminor.yy418;
        break;
      case 88: /* col_name ::= column_name */
{ yylhsminor.yy68 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy5); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 89: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL, NULL); }
        break;
      case 90: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL, NULL); }
        break;
      case 91: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL, NULL); }
        break;
      case 92: /* cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, yymsp[-2].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 93: /* cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, yymsp[-2].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 94: /* cmd ::= SHOW db_name_cond_opt VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, yymsp[-1].minor.yy68, NULL); }
        break;
      case 95: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL, NULL); }
        break;
      case 96: /* cmd ::= SHOW MODULES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MODULES_STMT, NULL, NULL); }
        break;
      case 97: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL, NULL); }
        break;
      case 98: /* cmd ::= SHOW FUNCTIONS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT, NULL, NULL); }
        break;
      case 99: /* cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, yymsp[-1].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 100: /* cmd ::= SHOW STREAMS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT, NULL, NULL); }
        break;
      case 101: /* db_name_cond_opt ::= */
      case 103: /* like_pattern_opt ::= */ yytestcase(yyruleno==103);
      case 106: /* from_db_opt ::= */ yytestcase(yyruleno==106);
      case 198: /* where_clause_opt ::= */ yytestcase(yyruleno==198);
      case 202: /* twindow_clause_opt ::= */ yytestcase(yyruleno==202);
      case 207: /* sliding_opt ::= */ yytestcase(yyruleno==207);
      case 209: /* fill_opt ::= */ yytestcase(yyruleno==209);
      case 221: /* having_clause_opt ::= */ yytestcase(yyruleno==221);
      case 229: /* slimit_clause_opt ::= */ yytestcase(yyruleno==229);
      case 233: /* limit_clause_opt ::= */ yytestcase(yyruleno==233);
{ yymsp[1].minor.yy68 = NULL; }
        break;
      case 102: /* db_name_cond_opt ::= db_name NK_DOT */
{ yylhsminor.yy68 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 104: /* like_pattern_opt ::= LIKE NK_STRING */
{ yymsp[-1].minor.yy68 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
        break;
      case 105: /* table_name_cond ::= table_name */
{ yylhsminor.yy68 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy5); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 107: /* from_db_opt ::= FROM db_name */
{ yymsp[-1].minor.yy68 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy5); }
        break;
      case 108: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy68; }
        break;
      case 109: /* literal ::= NK_INTEGER */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 110: /* literal ::= NK_FLOAT */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 111: /* literal ::= NK_STRING */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 112: /* literal ::= NK_BOOL */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 113: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 114: /* literal ::= duration_literal */
      case 125: /* expression ::= literal */ yytestcase(yyruleno==125);
      case 126: /* expression ::= column_reference */ yytestcase(yyruleno==126);
      case 129: /* expression ::= subquery */ yytestcase(yyruleno==129);
      case 161: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==161);
      case 165: /* boolean_primary ::= predicate */ yytestcase(yyruleno==165);
      case 167: /* common_expression ::= expression */ yytestcase(yyruleno==167);
      case 168: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==168);
      case 170: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==170);
      case 172: /* table_reference ::= table_primary */ yytestcase(yyruleno==172);
      case 173: /* table_reference ::= joined_table */ yytestcase(yyruleno==173);
      case 177: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==177);
      case 224: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==224);
      case 226: /* query_primary ::= query_specification */ yytestcase(yyruleno==226);
{ yylhsminor.yy68 = yymsp[0].minor.yy68; }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 115: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 116: /* literal_list ::= literal */
      case 138: /* expression_list ::= expression */ yytestcase(yyruleno==138);
{ yylhsminor.yy40 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68)); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 117: /* literal_list ::= literal_list NK_COMMA literal */
      case 139: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==139);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, releaseRawExprNode(pCxt, yymsp[0].minor.yy68)); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 127: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy5, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy5, yymsp[-1].minor.yy40)); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 128: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy5, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy5, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 130: /* expression ::= NK_LP expression NK_RP */
      case 166: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==166);
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 131: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy68));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 132: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), NULL));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 133: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 134: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 135: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 136: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 137: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 140: /* column_reference ::= column_name */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy5, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy5)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 141: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5, createColumnNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 142: /* predicate ::= expression compare_op expression */
      case 147: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==147);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy416, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 143: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy68), releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-4].minor.yy68 = yylhsminor.yy68;
        break;
      case 144: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[-5].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-5].minor.yy68 = yylhsminor.yy68;
        break;
      case 145: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), NULL));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 146: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), NULL));
                                                                                  }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 148: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy416 = OP_TYPE_LOWER_THAN; }
        break;
      case 149: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy416 = OP_TYPE_GREATER_THAN; }
        break;
      case 150: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy416 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 151: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy416 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 152: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy416 = OP_TYPE_NOT_EQUAL; }
        break;
      case 153: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy416 = OP_TYPE_EQUAL; }
        break;
      case 154: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy416 = OP_TYPE_LIKE; }
        break;
      case 155: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy416 = OP_TYPE_NOT_LIKE; }
        break;
      case 156: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy416 = OP_TYPE_MATCH; }
        break;
      case 157: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy416 = OP_TYPE_NMATCH; }
        break;
      case 158: /* in_op ::= IN */
{ yymsp[0].minor.yy416 = OP_TYPE_IN; }
        break;
      case 159: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy416 = OP_TYPE_NOT_IN; }
        break;
      case 160: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy40)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 162: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), NULL));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 163: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 164: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 169: /* from_clause ::= FROM table_reference_list */
      case 199: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==199);
      case 222: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy68 = yymsp[0].minor.yy68; }
        break;
      case 171: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy68 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy68, yymsp[0].minor.yy68, NULL); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 174: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy68 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy5, &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 175: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy68 = createRealTableNode(pCxt, &yymsp[-3].minor.yy5, &yymsp[-1].minor.yy5, &yymsp[0].minor.yy5); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 176: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy68 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 178: /* alias_opt ::= */
{ yymsp[1].minor.yy5 = nil_token;  }
        break;
      case 179: /* alias_opt ::= table_alias */
{ yylhsminor.yy5 = yymsp[0].minor.yy5; }
  yymsp[0].minor.yy5 = yylhsminor.yy5;
        break;
      case 180: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy5 = yymsp[0].minor.yy5; }
        break;
      case 181: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 182: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==182);
{ yymsp[-2].minor.yy68 = yymsp[-1].minor.yy68; }
        break;
      case 183: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy68 = createJoinTableNode(pCxt, yymsp[-4].minor.yy92, yymsp[-5].minor.yy68, yymsp[-2].minor.yy68, yymsp[0].minor.yy68); }
  yymsp[-5].minor.yy68 = yylhsminor.yy68;
        break;
      case 184: /* join_type ::= */
{ yymsp[1].minor.yy92 = JOIN_TYPE_INNER; }
        break;
      case 185: /* join_type ::= INNER */
{ yymsp[0].minor.yy92 = JOIN_TYPE_INNER; }
        break;
      case 186: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy68 = createSelectStmt(pCxt, yymsp[-7].minor.yy25, yymsp[-6].minor.yy40, yymsp[-5].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addWhereClause(pCxt, yymsp[-8].minor.yy68, yymsp[-4].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addPartitionByClause(pCxt, yymsp[-8].minor.yy68, yymsp[-3].minor.yy40);
                                                                                    yymsp[-8].minor.yy68 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy68, yymsp[-2].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addGroupByClause(pCxt, yymsp[-8].minor.yy68, yymsp[-1].minor.yy40);
                                                                                    yymsp[-8].minor.yy68 = addHavingClause(pCxt, yymsp[-8].minor.yy68, yymsp[0].minor.yy68);
                                                                                  }
        break;
      case 188: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy25 = true; }
        break;
      case 189: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy25 = false; }
        break;
      case 190: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy40 = NULL; }
        break;
      case 194: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), &t);
                                                                                  }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 195: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 196: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 197: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy68 = createColumnNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 201: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 218: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==218);
      case 228: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==228);
{ yymsp[-2].minor.yy40 = yymsp[0].minor.yy40; }
        break;
      case 203: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy68 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), &yymsp[-1].minor.yy0); }
        break;
      case 204: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy68 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68)); }
        break;
      case 205: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy68 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy68, NULL, yymsp[-1].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 206: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy68 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy68, yymsp[-3].minor.yy68, yymsp[-1].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 208: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy68 = yymsp[-1].minor.yy68; }
        break;
      case 210: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy68 = createFillNode(pCxt, yymsp[-1].minor.yy94, NULL); }
        break;
      case 211: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy68 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy40)); }
        break;
      case 212: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy94 = FILL_MODE_NONE; }
        break;
      case 213: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy94 = FILL_MODE_PREV; }
        break;
      case 214: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy94 = FILL_MODE_NULL; }
        break;
      case 215: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy94 = FILL_MODE_LINEAR; }
        break;
      case 216: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy94 = FILL_MODE_NEXT; }
        break;
      case 219: /* group_by_list ::= expression */
{ yylhsminor.yy40 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 220: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 223: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy68 = addOrderByClause(pCxt, yymsp[-3].minor.yy68, yymsp[-2].minor.yy40);
                                                                                    yylhsminor.yy68 = addSlimitClause(pCxt, yylhsminor.yy68, yymsp[-1].minor.yy68);
                                                                                    yylhsminor.yy68 = addLimitClause(pCxt, yylhsminor.yy68, yymsp[0].minor.yy68);
                                                                                  }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 225: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy68 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy68, yymsp[0].minor.yy68); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 230: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 234: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==234);
{ yymsp[-1].minor.yy68 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 231: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 235: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==235);
{ yymsp[-3].minor.yy68 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 232: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 236: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==236);
{ yymsp[-3].minor.yy68 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 237: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy68); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 238: /* search_condition ::= common_expression */
{ yylhsminor.yy68 = releaseRawExprNode(pCxt, yymsp[0].minor.yy68); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 241: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy68 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), yymsp[-1].minor.yy54, yymsp[0].minor.yy53); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 242: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy54 = ORDER_ASC; }
        break;
      case 243: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy54 = ORDER_ASC; }
        break;
      case 244: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy54 = ORDER_DESC; }
        break;
      case 245: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy53 = NULL_ORDER_DEFAULT; }
        break;
      case 246: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy53 = NULL_ORDER_FIRST; }
        break;
      case 247: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy53 = NULL_ORDER_LAST; }
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
  
  if(TOKEN.z) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
  } else {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
  }
  pCxt->valid = false;
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
