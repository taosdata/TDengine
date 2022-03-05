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
#include "ttoken.h"
#include "ttokendef.h"
#include "astCreateFuncs.h"
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
**    NewParseTOKENTYPE     is the data type used for minor type for terminal
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
**                       which is NewParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    NewParseARG_SDECL     A static variable declaration for the %extra_argument
**    NewParseARG_PDECL     A parameter declaration for the %extra_argument
**    NewParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    NewParseARG_STORE     Code to store %extra_argument into yypParser
**    NewParseARG_FETCH     Code to extract %extra_argument from yypParser
**    NewParseCTX_*         As NewParseARG_ except for %extra_context
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
#define YYNOCODE 191
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EJoinType yy28;
  EOrder yy29;
  SDataType yy42;
  EFillMode yy102;
  EOperatorType yy140;
  SToken yy175;
  SNodeList* yy182;
  bool yy187;
  SNode* yy210;
  SDatabaseOptions* yy211;
  STableOptions* yy286;
  ENullOrder yy307;
  STokenPair yy341;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define NewParseARG_SDECL  SAstCreateContext* pCxt ;
#define NewParseARG_PDECL , SAstCreateContext* pCxt 
#define NewParseARG_PARAM ,pCxt 
#define NewParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define NewParseARG_STORE yypParser->pCxt =pCxt ;
#define NewParseCTX_SDECL
#define NewParseCTX_PDECL
#define NewParseCTX_PARAM
#define NewParseCTX_FETCH
#define NewParseCTX_STORE
#define YYNSTATE             239
#define YYNRULE              211
#define YYNTOKEN             123
#define YY_MAX_SHIFT         238
#define YY_MIN_SHIFTREDUCE   390
#define YY_MAX_SHIFTREDUCE   600
#define YY_ERROR_ACTION      601
#define YY_ACCEPT_ACTION     602
#define YY_NO_ACTION         603
#define YY_MIN_REDUCE        604
#define YY_MAX_REDUCE        814
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
#define YY_ACTTAB_COUNT (824)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   122,  214,  135,   23,   84,  793,  746,  684,  682,  684,
 /*    10 */   682,  707,   30,   28,   26,   25,   24,  180,  673,  792,
 /*    20 */   672,  116,  151,  791,  743,   30,   28,   26,   25,   24,
 /*    30 */   604,  221,  133,  707,  116,  685,  682,  692,  682,  180,
 /*    40 */   181,  636,   57,   48,  693,  483,  696,  732,  689,    9,
 /*    50 */     8,  731,  728,  238,  687,  237,  236,  235,  234,  233,
 /*    60 */   232,  231,  230,  229,  156,  228,  227,  226,  225,  224,
 /*    70 */   223,  222,   30,   28,   26,   25,   24,  100,   22,  126,
 /*    80 */   664,  501,  502,  503,  504,  505,  506,  507,  509,  510,
 /*    90 */   511,   22,  126,  519,  501,  502,  503,  504,  505,  506,
 /*   100 */   507,  509,  510,  511,  157,  152,  150,  426,  212,  211,
 /*   110 */   210,  430,  209,  432,  433,  208,  435,  205,   93,  441,
 /*   120 */   202,  443,  444,  199,  196,  707,  474,  630,  170,  692,
 /*   130 */   682,  180,  181,   96,  476,   46,  693,  168,  696,  732,
 /*   140 */    53,   20,  629,  115,  728,   30,   28,   26,   25,   24,
 /*   150 */   508,  137,  707,  512,  627,  793,  692,  682,  166,  181,
 /*   160 */   636,   57,   47,  693,  121,  696,  732,  477,   98,   66,
 /*   170 */   124,  728,   61,  791,  221,  746,  707,   82,  596,  597,
 /*   180 */   692,  682,  180,  181,   83,  746,   58,  693,   89,  696,
 /*   190 */   141,  759,   45,  742,  707,   92,  498,   10,  692,  682,
 /*   200 */   166,  181,  165,  741,   47,  693,   10,  696,  732,  707,
 /*   210 */    68,  165,  124,  728,   61,  180,   44,  707,  132,   95,
 /*   220 */   155,  692,  682,  180,  181,  171,  806,   47,  693,  215,
 /*   230 */   696,  732,   85,  760,  707,  124,  728,  805,  692,  682,
 /*   240 */   180,  181,   67,  173,   47,  693,  766,  696,  732,   67,
 /*   250 */   139,  707,  124,  728,  805,  692,  682,  180,  181,  636,
 /*   260 */    57,   47,  693,  789,  696,  732,   29,   27,  148,  124,
 /*   270 */   728,  805,  134,   67,  673,   11,  671,  751,  463,  538,
 /*   280 */   750,  564,   29,   27,  543,  542,  463,   49,  465,  628,
 /*   290 */   550,   11,  637,   57,  463,  707,  465,  177,    1,  692,
 /*   300 */   682,  180,  181,  131,  465,  111,  693,  130,  696,   30,
 /*   310 */    28,   26,   25,   24,    1,  174,  190,   74,  138,  131,
 /*   320 */   673,  218,  671,   71,  190,  474,  217,    9,    8,  464,
 /*   330 */   466,  469,  190,   26,   25,   24,   77,  464,  466,  469,
 /*   340 */   762,  219,  483,    6,  538,  464,  466,  469,  162,  163,
 /*   350 */    67,  707,  708,  172,  170,  692,  682,  180,  181,  513,
 /*   360 */   216,  105,  693,  178,  696,   31,    2,   52,   29,   27,
 /*   370 */   480,   29,   27,   87,   50,  763,   31,   11,  541,  175,
 /*   380 */   463,  793,  477,  463,   63,  739,  740,  149,  744,  773,
 /*   390 */   465,  184,  183,  465,  146,   66,  599,  600,   69,  791,
 /*   400 */     1,  469,  144,    7,  145,  131,  707,   73,  131,   90,
 /*   410 */   692,  682,  180,  181,  772,   54,   48,  693,  190,  696,
 /*   420 */   732,  190,  419,  417,  167,  728,  123,   19,   55,   49,
 /*   430 */   162,  464,  466,  469,  464,  466,  469,   30,   28,   26,
 /*   440 */    25,   24,  447,    5,  753,  159,  170,  707,  194,   52,
 /*   450 */   567,  692,  682,  180,  181,   67,   50,  111,  693,  142,
 /*   460 */   696,   76,   29,   27,  169,  143,   80,  739,  161,  451,
 /*   470 */   160,  162,   60,  793,  463,   54,  147,  565,  566,  568,
 /*   480 */   569,   29,   27,  456,  465,    4,   56,   66,   78,   55,
 /*   490 */    52,  791,   54,  463,    7,  538,  473,   50,   51,  131,
 /*   500 */   476,  747,   79,  465,   32,   16,  164,   62,  739,  740,
 /*   510 */   707,  744,  190,    1,  692,  682,  180,  181,  131,  127,
 /*   520 */    48,  693,  714,  696,  732,  464,  466,  469,   21,  729,
 /*   530 */   808,  190,  179,  790,  176,   86,   29,   27,   30,   28,
 /*   540 */    26,   25,   24,  472,  464,  466,  469,  185,  463,  707,
 /*   550 */    99,  602,  189,  692,  682,  180,  181,  186,  465,  107,
 /*   560 */   693,   91,  696,  187,   41,  192,   94,  140,    7,  182,
 /*   570 */   707,  101,   97,  131,  692,  682,  180,  181,  102,  113,
 /*   580 */   111,  693,  125,  696,  114,    3,  190,   31,   70,   14,
 /*   590 */   561,   72,  158,   35,  153,  563,  793,   59,  707,  464,
 /*   600 */   466,  469,  692,  682,  180,  181,  626,   75,   58,  693,
 /*   610 */    66,  696,  707,  557,  791,   37,  692,  682,  180,  181,
 /*   620 */   154,  556,  106,  693,  687,  696,  707,   38,   18,   15,
 /*   630 */   692,  682,  180,  181,  535,  534,  108,  693,  218,  696,
 /*   640 */     8,   81,  686,  217,   33,   34,   65,  499,  807,  707,
 /*   650 */   481,   17,  590,  692,  682,  180,  181,   12,  219,  103,
 /*   660 */   693,  585,  696,  707,   39,  584,  128,  692,  682,  180,
 /*   670 */   181,  589,  588,  109,  693,  707,  696,  216,  129,  692,
 /*   680 */   682,  180,  181,   88,   13,  104,  693,  707,  696,  676,
 /*   690 */   675,  692,  682,  180,  181,  674,  625,  110,  693,  707,
 /*   700 */   696,  421,   40,  692,  682,  180,  181,  467,  448,  704,
 /*   710 */   693,   95,  696,  707,   36,  188,  191,  692,  682,  180,
 /*   720 */   181,  193,  136,  703,  693,  707,  696,  195,  197,  692,
 /*   730 */   682,  180,  181,  445,  200,  702,  693,  707,  696,  442,
 /*   740 */   203,  692,  682,  180,  181,  198,  201,  119,  693,  204,
 /*   750 */   696,  707,  436,  434,  206,  692,  682,  180,  181,  207,
 /*   760 */   425,  118,  693,  440,  696,  707,  213,  439,   42,  692,
 /*   770 */   682,  180,  181,  438,  455,  120,  693,  437,  696,   43,
 /*   780 */   454,  453,  162,  391,  220,  403,  410,  707,  409,  408,
 /*   790 */   407,  692,  682,  180,  181,  406,  405,  117,  693,  707,
 /*   800 */   696,   52,  404,  692,  682,  180,  181,  402,   50,  112,
 /*   810 */   693,  401,  696,  400,  399,  398,  397,  396,   64,  739,
 /*   820 */   740,  395,  744,  394,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   135,  139,  135,  153,  154,  168,  146,  142,  143,  142,
 /*    10 */   143,  138,   12,   13,   14,   15,   16,  144,  138,  182,
 /*    20 */   140,   21,  149,  186,  164,   12,   13,   14,   15,   16,
 /*    30 */     0,   20,  128,  138,   21,  142,  143,  142,  143,  144,
 /*    40 */   145,  137,  138,  148,  149,   45,  151,  152,   44,    1,
 /*    50 */     2,  156,  157,   23,   50,   25,   26,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,   49,   35,   36,   37,   38,   39,
 /*    70 */    40,   41,   12,   13,   14,   15,   16,  129,   78,   79,
 /*    80 */   132,   81,   82,   83,   84,   85,   86,   87,   88,   89,
 /*    90 */    90,   78,   79,   45,   81,   82,   83,   84,   85,   86,
 /*   100 */    87,   88,   89,   90,  101,  102,  103,   53,   54,   55,
 /*   110 */    56,   57,   58,   59,   60,   61,   62,   63,  124,   65,
 /*   120 */    66,   67,   68,   69,   70,  138,   49,  133,  141,  142,
 /*   130 */   143,  144,  145,  124,   49,  148,  149,   21,  151,  152,
 /*   140 */   131,   78,  133,  156,  157,   12,   13,   14,   15,   16,
 /*   150 */    87,  128,  138,   90,    0,  168,  142,  143,  144,  145,
 /*   160 */   137,  138,  148,  149,   18,  151,  152,   49,   19,  182,
 /*   170 */   156,  157,  158,  186,   20,  146,  138,   92,  118,  119,
 /*   180 */   142,  143,  144,  145,  170,  146,  148,  149,   42,  151,
 /*   190 */   176,  177,   43,  164,  138,   46,   80,   44,  142,  143,
 /*   200 */   144,  145,   49,  164,  148,  149,   44,  151,  152,  138,
 /*   210 */    92,   49,  156,  157,  158,  144,   44,  138,   72,   47,
 /*   220 */   149,  142,  143,  144,  145,  187,  188,  148,  149,   48,
 /*   230 */   151,  152,  189,  177,  138,  156,  157,  158,  142,  143,
 /*   240 */   144,  145,   96,    3,  148,  149,  167,  151,  152,   96,
 /*   250 */   128,  138,  156,  157,  158,  142,  143,  144,  145,  137,
 /*   260 */   138,  148,  149,  167,  151,  152,   12,   13,  180,  156,
 /*   270 */   157,  158,  136,   96,  138,   21,  140,   93,   24,   95,
 /*   280 */   167,   45,   12,   13,   14,    4,   24,   51,   34,    0,
 /*   290 */    14,   21,  137,  138,   24,  138,   34,   51,   44,  142,
 /*   300 */   143,  144,  145,   49,   34,  148,  149,  150,  151,   12,
 /*   310 */    13,   14,   15,   16,   44,   51,   62,   45,  136,   49,
 /*   320 */   138,   32,  140,   51,   62,   49,   37,    1,    2,   75,
 /*   330 */    76,   77,   62,   14,   15,   16,  173,   75,   76,   77,
 /*   340 */   147,   52,   45,   94,   95,   75,   76,   77,  125,  166,
 /*   350 */    96,  138,  138,  113,  141,  142,  143,  144,  145,   45,
 /*   360 */    71,  148,  149,  117,  151,   51,  169,  144,   12,   13,
 /*   370 */    45,   12,   13,  183,  151,  147,   51,   21,   97,  115,
 /*   380 */    24,  168,   49,   24,  161,  162,  163,  105,  165,  179,
 /*   390 */    34,   73,   74,   34,  104,  182,  121,  122,  178,  186,
 /*   400 */    44,   77,  143,   44,  143,   49,  138,  178,   49,   45,
 /*   410 */   142,  143,  144,  145,  179,   51,  148,  149,   62,  151,
 /*   420 */   152,   62,   45,   45,  156,  157,  143,    2,   51,   51,
 /*   430 */   125,   75,   76,   77,   75,   76,   77,   12,   13,   14,
 /*   440 */    15,   16,   45,  112,  175,  111,  141,  138,   51,  144,
 /*   450 */    80,  142,  143,  144,  145,   96,  151,  148,  149,  150,
 /*   460 */   151,  174,   12,   13,   14,   99,  161,  162,  163,   45,
 /*   470 */   165,  125,  172,  168,   24,   51,  106,  107,  108,  109,
 /*   480 */   110,   12,   13,   45,   34,   98,   45,  182,  171,   51,
 /*   490 */   144,  186,   51,   24,   44,   95,   49,  151,  144,   49,
 /*   500 */    49,  146,  159,   34,   91,   44,  160,  161,  162,  163,
 /*   510 */   138,  165,   62,   44,  142,  143,  144,  145,   49,  120,
 /*   520 */   148,  149,  155,  151,  152,   75,   76,   77,    2,  157,
 /*   530 */   190,   62,  116,  185,  114,  184,   12,   13,   12,   13,
 /*   540 */    14,   15,   16,   49,   75,   76,   77,  125,   24,  138,
 /*   550 */   132,  123,   49,  142,  143,  144,  145,  127,   34,  148,
 /*   560 */   149,  124,  151,  127,   44,  134,  127,  127,   44,  141,
 /*   570 */   138,  125,  124,   49,  142,  143,  144,  145,  126,  130,
 /*   580 */   148,  149,  150,  151,  130,   51,   62,   51,   45,  100,
 /*   590 */    45,   44,  181,   51,   24,   45,  168,   44,  138,   75,
 /*   600 */    76,   77,  142,  143,  144,  145,    0,   44,  148,  149,
 /*   610 */   182,  151,  138,   45,  186,   44,  142,  143,  144,  145,
 /*   620 */    51,   45,  148,  149,   50,  151,  138,   44,   51,  100,
 /*   630 */   142,  143,  144,  145,   45,   45,  148,  149,   32,  151,
 /*   640 */     2,   50,   50,   37,   93,   51,   50,   80,  188,  138,
 /*   650 */    45,   51,   45,  142,  143,  144,  145,  100,   52,  148,
 /*   660 */   149,   24,  151,  138,    4,   24,   24,  142,  143,  144,
 /*   670 */   145,   24,   24,  148,  149,  138,  151,   71,   24,  142,
 /*   680 */   143,  144,  145,   50,   44,  148,  149,  138,  151,    0,
 /*   690 */     0,  142,  143,  144,  145,    0,    0,  148,  149,  138,
 /*   700 */   151,   49,   44,  142,  143,  144,  145,   34,   45,  148,
 /*   710 */   149,   47,  151,  138,   44,   50,   48,  142,  143,  144,
 /*   720 */   145,   24,   24,  148,  149,  138,  151,   44,   24,  142,
 /*   730 */   143,  144,  145,   45,   24,  148,  149,  138,  151,   45,
 /*   740 */    24,  142,  143,  144,  145,   44,   44,  148,  149,   44,
 /*   750 */   151,  138,   45,   45,   24,  142,  143,  144,  145,   44,
 /*   760 */    34,  148,  149,   64,  151,  138,   52,   64,   44,  142,
 /*   770 */   143,  144,  145,   64,   24,  148,  149,   64,  151,   44,
 /*   780 */    24,   34,  125,   22,   21,   34,   24,  138,   24,   24,
 /*   790 */    24,  142,  143,  144,  145,   24,   24,  148,  149,  138,
 /*   800 */   151,  144,   24,  142,  143,  144,  145,   24,  151,  148,
 /*   810 */   149,   24,  151,   24,   24,   24,   24,   24,  161,  162,
 /*   820 */   163,   24,  165,   24,  191,  191,  191,  191,  191,  191,
 /*   830 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   840 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   850 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   860 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   870 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   880 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   890 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   900 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   910 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   920 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   930 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   940 */   191,  191,  191,  191,  191,  191,  191,
};
#define YY_SHIFT_COUNT    (238)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (799)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   146,  254,  270,  356,  356,  356,  356,  359,  356,  356,
 /*    10 */   153,  469,  524,  450,  524,  524,  524,  524,  524,  524,
 /*    20 */   524,  524,  524,  524,  524,  524,  524,  524,  524,  524,
 /*    30 */   524,  524,  162,  162,  162,  262,  262,   15,   15,  177,
 /*    40 */    77,   77,   77,   77,   77,   11,    0,   13,   13,  262,
 /*    50 */    85,   85,   85,  154,   77,   77,  181,   54,   60,  370,
 /*    60 */     3,  118,  184,  249,  184,  276,  240,  281,  333,  282,
 /*    70 */   290,  324,  324,  282,  290,  324,  331,  334,  366,  387,
 /*    80 */   400,  447,  451,  413,  461,  399,  416,  420,   77,  494,
 /*    90 */   181,  503,   11,  503,  520,  503,  503,  494,   11,  824,
 /*   100 */   824,  824,   30,  425,  526,  297,  133,  133,  133,  133,
 /*   110 */   133,  133,  133,  289,  606,   48,   63,  319,  319,  319,
 /*   120 */   319,  149,  236,  272,  326,  314,  116,  275,  264,  246,
 /*   130 */   325,    4,  318,  364,  377,  378,  397,  424,  438,  441,
 /*   140 */   172,  534,  536,  489,  543,  545,  547,  542,  550,  553,
 /*   150 */   563,  568,  571,  576,  570,  569,  574,  583,  577,  529,
 /*   160 */   589,  590,  591,  551,  594,  592,  596,  638,  567,  605,
 /*   170 */   607,  600,  557,  660,  637,  641,  642,  647,  648,  654,
 /*   180 */   633,  640,  689,  690,  695,  696,  658,  664,  652,  665,
 /*   190 */   673,  670,  668,  663,  697,  698,  683,  688,  704,  701,
 /*   200 */   694,  710,  702,  707,  716,  705,  708,  730,  715,  699,
 /*   210 */   703,  709,  713,  726,  714,  724,  735,  750,  756,  747,
 /*   220 */   761,  763,  762,  764,  765,  766,  771,  772,  778,  751,
 /*   230 */   783,  787,  789,  790,  791,  792,  793,  797,  799,
};
#define YY_REDUCE_COUNT (101)
#define YY_REDUCE_MIN   (-163)
#define YY_REDUCE_MAX   (661)
static const short yy_reduce_ofst[] = {
 /*     0 */   428,  -13,   14,   56,   79,   96,  113,  213, -105,  268,
 /*    10 */   305,  372,   38,  157,  309,  411,  432,  460,  474,  488,
 /*    20 */   511,  525,  537,  549,  561,  575,  587,  599,  613,  627,
 /*    30 */   649,  661,  346,  223,  657, -135, -133, -127,   71, -163,
 /*    40 */   -96,  136,   23,  182,  122,    9, -150, -150, -150, -107,
 /*    50 */  -140,   29,   39,   -6,  155, -120,  -52, -138,   43,   88,
 /*    60 */   163,  193,  183,  183,  183,  214,  190,  197,  228,  210,
 /*    70 */   220,  259,  261,  235,  229,  283,  269,  287,  300,  317,
 /*    80 */   183,  354,  355,  343,  367,  340,  348,  351,  214,  422,
 /*    90 */   418,  430,  437,  436,  431,  439,  440,  446,  448,  449,
 /*   100 */   454,  452,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  601,
 /*    10 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  601,
 /*    20 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  601,
 /*    30 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  601,
 /*    40 */   601,  601,  601,  601,  601,  606,  601,  734,  601,  601,
 /*    50 */   745,  745,  745,  606,  601,  601,  663,  601,  809,  601,
 /*    60 */   769,  761,  737,  751,  738,  601,  794,  754,  601,  776,
 /*    70 */   774,  601,  601,  776,  774,  601,  788,  784,  767,  765,
 /*    80 */   751,  601,  601,  601,  601,  812,  800,  796,  601,  601,
 /*    90 */   601,  601,  606,  601,  632,  601,  601,  601,  606,  666,
 /*   100 */   666,  607,  601,  601,  601,  601,  787,  786,  711,  710,
 /*   110 */   709,  705,  706,  601,  601,  601,  601,  700,  701,  699,
 /*   120 */   698,  601,  601,  601,  735,  601,  601,  601,  797,  801,
 /*   130 */   601,  688,  601,  601,  601,  601,  601,  601,  601,  601,
 /*   140 */   601,  758,  768,  601,  601,  601,  601,  601,  601,  601,
 /*   150 */   601,  601,  601,  601,  601,  601,  688,  601,  785,  601,
 /*   160 */   744,  740,  601,  601,  736,  687,  601,  730,  601,  601,
 /*   170 */   601,  795,  601,  601,  601,  601,  601,  601,  601,  601,
 /*   180 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  634,
 /*   190 */   601,  601,  601,  601,  601,  601,  660,  601,  601,  601,
 /*   200 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  645,
 /*   210 */   643,  642,  641,  601,  638,  601,  601,  601,  601,  601,
 /*   220 */   601,  601,  601,  601,  601,  601,  601,  601,  601,  601,
 /*   230 */   601,  601,  601,  601,  601,  601,  601,  601,  601,
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
  NewParseARG_SDECL                /* A place to hold %extra_argument */
  NewParseCTX_SDECL                /* A place to hold %extra_context */
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
void NewParseTrace(FILE *TraceFILE, char *zTracePrompt){
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
  /*   19 */ "DATABASE",
  /*   20 */ "IF",
  /*   21 */ "NOT",
  /*   22 */ "EXISTS",
  /*   23 */ "BLOCKS",
  /*   24 */ "NK_INTEGER",
  /*   25 */ "CACHE",
  /*   26 */ "CACHELAST",
  /*   27 */ "COMP",
  /*   28 */ "DAYS",
  /*   29 */ "FSYNC",
  /*   30 */ "MAXROWS",
  /*   31 */ "MINROWS",
  /*   32 */ "KEEP",
  /*   33 */ "PRECISION",
  /*   34 */ "NK_STRING",
  /*   35 */ "QUORUM",
  /*   36 */ "REPLICA",
  /*   37 */ "TTL",
  /*   38 */ "WAL",
  /*   39 */ "VGROUPS",
  /*   40 */ "SINGLE_STABLE",
  /*   41 */ "STREAM_MODE",
  /*   42 */ "USE",
  /*   43 */ "TABLE",
  /*   44 */ "NK_LP",
  /*   45 */ "NK_RP",
  /*   46 */ "STABLE",
  /*   47 */ "USING",
  /*   48 */ "TAGS",
  /*   49 */ "NK_ID",
  /*   50 */ "NK_DOT",
  /*   51 */ "NK_COMMA",
  /*   52 */ "COMMENT",
  /*   53 */ "BOOL",
  /*   54 */ "TINYINT",
  /*   55 */ "SMALLINT",
  /*   56 */ "INT",
  /*   57 */ "INTEGER",
  /*   58 */ "BIGINT",
  /*   59 */ "FLOAT",
  /*   60 */ "DOUBLE",
  /*   61 */ "BINARY",
  /*   62 */ "TIMESTAMP",
  /*   63 */ "NCHAR",
  /*   64 */ "UNSIGNED",
  /*   65 */ "JSON",
  /*   66 */ "VARCHAR",
  /*   67 */ "MEDIUMBLOB",
  /*   68 */ "BLOB",
  /*   69 */ "VARBINARY",
  /*   70 */ "DECIMAL",
  /*   71 */ "SMA",
  /*   72 */ "SHOW",
  /*   73 */ "DATABASES",
  /*   74 */ "TABLES",
  /*   75 */ "NK_FLOAT",
  /*   76 */ "NK_BOOL",
  /*   77 */ "NK_VARIABLE",
  /*   78 */ "BETWEEN",
  /*   79 */ "IS",
  /*   80 */ "NULL",
  /*   81 */ "NK_LT",
  /*   82 */ "NK_GT",
  /*   83 */ "NK_LE",
  /*   84 */ "NK_GE",
  /*   85 */ "NK_NE",
  /*   86 */ "NK_EQ",
  /*   87 */ "LIKE",
  /*   88 */ "MATCH",
  /*   89 */ "NMATCH",
  /*   90 */ "IN",
  /*   91 */ "FROM",
  /*   92 */ "AS",
  /*   93 */ "JOIN",
  /*   94 */ "ON",
  /*   95 */ "INNER",
  /*   96 */ "SELECT",
  /*   97 */ "DISTINCT",
  /*   98 */ "WHERE",
  /*   99 */ "PARTITION",
  /*  100 */ "BY",
  /*  101 */ "SESSION",
  /*  102 */ "STATE_WINDOW",
  /*  103 */ "INTERVAL",
  /*  104 */ "SLIDING",
  /*  105 */ "FILL",
  /*  106 */ "VALUE",
  /*  107 */ "NONE",
  /*  108 */ "PREV",
  /*  109 */ "LINEAR",
  /*  110 */ "NEXT",
  /*  111 */ "GROUP",
  /*  112 */ "HAVING",
  /*  113 */ "ORDER",
  /*  114 */ "SLIMIT",
  /*  115 */ "SOFFSET",
  /*  116 */ "LIMIT",
  /*  117 */ "OFFSET",
  /*  118 */ "ASC",
  /*  119 */ "DESC",
  /*  120 */ "NULLS",
  /*  121 */ "FIRST",
  /*  122 */ "LAST",
  /*  123 */ "cmd",
  /*  124 */ "exists_opt",
  /*  125 */ "db_name",
  /*  126 */ "db_options",
  /*  127 */ "full_table_name",
  /*  128 */ "column_def_list",
  /*  129 */ "tags_def_opt",
  /*  130 */ "table_options",
  /*  131 */ "multi_create_clause",
  /*  132 */ "tags_def",
  /*  133 */ "create_subtable_clause",
  /*  134 */ "specific_tags_opt",
  /*  135 */ "literal_list",
  /*  136 */ "col_name_list",
  /*  137 */ "column_def",
  /*  138 */ "column_name",
  /*  139 */ "type_name",
  /*  140 */ "col_name",
  /*  141 */ "query_expression",
  /*  142 */ "literal",
  /*  143 */ "duration_literal",
  /*  144 */ "table_name",
  /*  145 */ "function_name",
  /*  146 */ "table_alias",
  /*  147 */ "column_alias",
  /*  148 */ "expression",
  /*  149 */ "column_reference",
  /*  150 */ "expression_list",
  /*  151 */ "subquery",
  /*  152 */ "predicate",
  /*  153 */ "compare_op",
  /*  154 */ "in_op",
  /*  155 */ "in_predicate_value",
  /*  156 */ "boolean_value_expression",
  /*  157 */ "boolean_primary",
  /*  158 */ "common_expression",
  /*  159 */ "from_clause",
  /*  160 */ "table_reference_list",
  /*  161 */ "table_reference",
  /*  162 */ "table_primary",
  /*  163 */ "joined_table",
  /*  164 */ "alias_opt",
  /*  165 */ "parenthesized_joined_table",
  /*  166 */ "join_type",
  /*  167 */ "search_condition",
  /*  168 */ "query_specification",
  /*  169 */ "set_quantifier_opt",
  /*  170 */ "select_list",
  /*  171 */ "where_clause_opt",
  /*  172 */ "partition_by_clause_opt",
  /*  173 */ "twindow_clause_opt",
  /*  174 */ "group_by_clause_opt",
  /*  175 */ "having_clause_opt",
  /*  176 */ "select_sublist",
  /*  177 */ "select_item",
  /*  178 */ "sliding_opt",
  /*  179 */ "fill_opt",
  /*  180 */ "fill_mode",
  /*  181 */ "group_by_list",
  /*  182 */ "query_expression_body",
  /*  183 */ "order_by_clause_opt",
  /*  184 */ "slimit_clause_opt",
  /*  185 */ "limit_clause_opt",
  /*  186 */ "query_primary",
  /*  187 */ "sort_specification_list",
  /*  188 */ "sort_specification",
  /*  189 */ "ordering_specification_opt",
  /*  190 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE DATABASE exists_opt db_name db_options",
 /*   1 */ "exists_opt ::= IF NOT EXISTS",
 /*   2 */ "exists_opt ::=",
 /*   3 */ "db_options ::=",
 /*   4 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*   5 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*   6 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*   7 */ "db_options ::= db_options COMP NK_INTEGER",
 /*   8 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*   9 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  10 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  11 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  12 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  13 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  14 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  15 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  16 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  17 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  18 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  19 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  20 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  21 */ "cmd ::= USE db_name",
 /*  22 */ "cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  23 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  24 */ "cmd ::= CREATE STABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  25 */ "multi_create_clause ::= create_subtable_clause",
 /*  26 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  27 */ "create_subtable_clause ::= exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  28 */ "specific_tags_opt ::=",
 /*  29 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  30 */ "full_table_name ::= NK_ID",
 /*  31 */ "full_table_name ::= NK_ID NK_DOT NK_ID",
 /*  32 */ "column_def_list ::= column_def",
 /*  33 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  34 */ "column_def ::= column_name type_name",
 /*  35 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  36 */ "type_name ::= BOOL",
 /*  37 */ "type_name ::= TINYINT",
 /*  38 */ "type_name ::= SMALLINT",
 /*  39 */ "type_name ::= INT",
 /*  40 */ "type_name ::= INTEGER",
 /*  41 */ "type_name ::= BIGINT",
 /*  42 */ "type_name ::= FLOAT",
 /*  43 */ "type_name ::= DOUBLE",
 /*  44 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  45 */ "type_name ::= TIMESTAMP",
 /*  46 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  47 */ "type_name ::= TINYINT UNSIGNED",
 /*  48 */ "type_name ::= SMALLINT UNSIGNED",
 /*  49 */ "type_name ::= INT UNSIGNED",
 /*  50 */ "type_name ::= BIGINT UNSIGNED",
 /*  51 */ "type_name ::= JSON",
 /*  52 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  53 */ "type_name ::= MEDIUMBLOB",
 /*  54 */ "type_name ::= BLOB",
 /*  55 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  56 */ "type_name ::= DECIMAL",
 /*  57 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  58 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  59 */ "tags_def_opt ::=",
 /*  60 */ "tags_def_opt ::= tags_def",
 /*  61 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  62 */ "table_options ::=",
 /*  63 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  64 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  65 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  66 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  67 */ "col_name_list ::= col_name",
 /*  68 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  69 */ "col_name ::= column_name",
 /*  70 */ "cmd ::= SHOW DATABASES",
 /*  71 */ "cmd ::= SHOW TABLES",
 /*  72 */ "cmd ::= query_expression",
 /*  73 */ "literal ::= NK_INTEGER",
 /*  74 */ "literal ::= NK_FLOAT",
 /*  75 */ "literal ::= NK_STRING",
 /*  76 */ "literal ::= NK_BOOL",
 /*  77 */ "literal ::= TIMESTAMP NK_STRING",
 /*  78 */ "literal ::= duration_literal",
 /*  79 */ "duration_literal ::= NK_VARIABLE",
 /*  80 */ "literal_list ::= literal",
 /*  81 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  82 */ "db_name ::= NK_ID",
 /*  83 */ "table_name ::= NK_ID",
 /*  84 */ "column_name ::= NK_ID",
 /*  85 */ "function_name ::= NK_ID",
 /*  86 */ "table_alias ::= NK_ID",
 /*  87 */ "column_alias ::= NK_ID",
 /*  88 */ "expression ::= literal",
 /*  89 */ "expression ::= column_reference",
 /*  90 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  91 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  92 */ "expression ::= subquery",
 /*  93 */ "expression ::= NK_LP expression NK_RP",
 /*  94 */ "expression ::= NK_PLUS expression",
 /*  95 */ "expression ::= NK_MINUS expression",
 /*  96 */ "expression ::= expression NK_PLUS expression",
 /*  97 */ "expression ::= expression NK_MINUS expression",
 /*  98 */ "expression ::= expression NK_STAR expression",
 /*  99 */ "expression ::= expression NK_SLASH expression",
 /* 100 */ "expression ::= expression NK_REM expression",
 /* 101 */ "expression_list ::= expression",
 /* 102 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 103 */ "column_reference ::= column_name",
 /* 104 */ "column_reference ::= table_name NK_DOT column_name",
 /* 105 */ "predicate ::= expression compare_op expression",
 /* 106 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 107 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 108 */ "predicate ::= expression IS NULL",
 /* 109 */ "predicate ::= expression IS NOT NULL",
 /* 110 */ "predicate ::= expression in_op in_predicate_value",
 /* 111 */ "compare_op ::= NK_LT",
 /* 112 */ "compare_op ::= NK_GT",
 /* 113 */ "compare_op ::= NK_LE",
 /* 114 */ "compare_op ::= NK_GE",
 /* 115 */ "compare_op ::= NK_NE",
 /* 116 */ "compare_op ::= NK_EQ",
 /* 117 */ "compare_op ::= LIKE",
 /* 118 */ "compare_op ::= NOT LIKE",
 /* 119 */ "compare_op ::= MATCH",
 /* 120 */ "compare_op ::= NMATCH",
 /* 121 */ "in_op ::= IN",
 /* 122 */ "in_op ::= NOT IN",
 /* 123 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 124 */ "boolean_value_expression ::= boolean_primary",
 /* 125 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 126 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 127 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 128 */ "boolean_primary ::= predicate",
 /* 129 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 130 */ "common_expression ::= expression",
 /* 131 */ "common_expression ::= boolean_value_expression",
 /* 132 */ "from_clause ::= FROM table_reference_list",
 /* 133 */ "table_reference_list ::= table_reference",
 /* 134 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 135 */ "table_reference ::= table_primary",
 /* 136 */ "table_reference ::= joined_table",
 /* 137 */ "table_primary ::= table_name alias_opt",
 /* 138 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 139 */ "table_primary ::= subquery alias_opt",
 /* 140 */ "table_primary ::= parenthesized_joined_table",
 /* 141 */ "alias_opt ::=",
 /* 142 */ "alias_opt ::= table_alias",
 /* 143 */ "alias_opt ::= AS table_alias",
 /* 144 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 145 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 146 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 147 */ "join_type ::=",
 /* 148 */ "join_type ::= INNER",
 /* 149 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 150 */ "set_quantifier_opt ::=",
 /* 151 */ "set_quantifier_opt ::= DISTINCT",
 /* 152 */ "set_quantifier_opt ::= ALL",
 /* 153 */ "select_list ::= NK_STAR",
 /* 154 */ "select_list ::= select_sublist",
 /* 155 */ "select_sublist ::= select_item",
 /* 156 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 157 */ "select_item ::= common_expression",
 /* 158 */ "select_item ::= common_expression column_alias",
 /* 159 */ "select_item ::= common_expression AS column_alias",
 /* 160 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 161 */ "where_clause_opt ::=",
 /* 162 */ "where_clause_opt ::= WHERE search_condition",
 /* 163 */ "partition_by_clause_opt ::=",
 /* 164 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 165 */ "twindow_clause_opt ::=",
 /* 166 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 167 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 168 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 169 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 170 */ "sliding_opt ::=",
 /* 171 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 172 */ "fill_opt ::=",
 /* 173 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 174 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 175 */ "fill_mode ::= NONE",
 /* 176 */ "fill_mode ::= PREV",
 /* 177 */ "fill_mode ::= NULL",
 /* 178 */ "fill_mode ::= LINEAR",
 /* 179 */ "fill_mode ::= NEXT",
 /* 180 */ "group_by_clause_opt ::=",
 /* 181 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 182 */ "group_by_list ::= expression",
 /* 183 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 184 */ "having_clause_opt ::=",
 /* 185 */ "having_clause_opt ::= HAVING search_condition",
 /* 186 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 187 */ "query_expression_body ::= query_primary",
 /* 188 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 189 */ "query_primary ::= query_specification",
 /* 190 */ "order_by_clause_opt ::=",
 /* 191 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 192 */ "slimit_clause_opt ::=",
 /* 193 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 194 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 195 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 196 */ "limit_clause_opt ::=",
 /* 197 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 198 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 199 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 200 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 201 */ "search_condition ::= common_expression",
 /* 202 */ "sort_specification_list ::= sort_specification",
 /* 203 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 204 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 205 */ "ordering_specification_opt ::=",
 /* 206 */ "ordering_specification_opt ::= ASC",
 /* 207 */ "ordering_specification_opt ::= DESC",
 /* 208 */ "null_ordering_opt ::=",
 /* 209 */ "null_ordering_opt ::= NULLS FIRST",
 /* 210 */ "null_ordering_opt ::= NULLS LAST",
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
** second argument to NewParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void NewParseInit(void *yypRawParser NewParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  NewParseCTX_STORE
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

#ifndef NewParse_ENGINEALWAYSONSTACK
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
** to NewParse and NewParseFree.
*/
void *NewParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) NewParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    NewParseCTX_STORE
    NewParseInit(yypParser NewParseCTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* NewParse_ENGINEALWAYSONSTACK */


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
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
    case 123: /* cmd */
    case 133: /* create_subtable_clause */
    case 137: /* column_def */
    case 140: /* col_name */
    case 141: /* query_expression */
    case 142: /* literal */
    case 143: /* duration_literal */
    case 148: /* expression */
    case 149: /* column_reference */
    case 151: /* subquery */
    case 152: /* predicate */
    case 155: /* in_predicate_value */
    case 156: /* boolean_value_expression */
    case 157: /* boolean_primary */
    case 158: /* common_expression */
    case 159: /* from_clause */
    case 160: /* table_reference_list */
    case 161: /* table_reference */
    case 162: /* table_primary */
    case 163: /* joined_table */
    case 165: /* parenthesized_joined_table */
    case 167: /* search_condition */
    case 168: /* query_specification */
    case 171: /* where_clause_opt */
    case 173: /* twindow_clause_opt */
    case 175: /* having_clause_opt */
    case 177: /* select_item */
    case 178: /* sliding_opt */
    case 179: /* fill_opt */
    case 182: /* query_expression_body */
    case 184: /* slimit_clause_opt */
    case 185: /* limit_clause_opt */
    case 186: /* query_primary */
    case 188: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy210)); 
}
      break;
    case 124: /* exists_opt */
    case 169: /* set_quantifier_opt */
{
 
}
      break;
    case 125: /* db_name */
    case 138: /* column_name */
    case 144: /* table_name */
    case 145: /* function_name */
    case 146: /* table_alias */
    case 147: /* column_alias */
    case 164: /* alias_opt */
{
 
}
      break;
    case 126: /* db_options */
{
 tfree((yypminor->yy211)); 
}
      break;
    case 127: /* full_table_name */
{
 
}
      break;
    case 128: /* column_def_list */
    case 129: /* tags_def_opt */
    case 131: /* multi_create_clause */
    case 132: /* tags_def */
    case 134: /* specific_tags_opt */
    case 135: /* literal_list */
    case 136: /* col_name_list */
    case 150: /* expression_list */
    case 170: /* select_list */
    case 172: /* partition_by_clause_opt */
    case 174: /* group_by_clause_opt */
    case 176: /* select_sublist */
    case 181: /* group_by_list */
    case 183: /* order_by_clause_opt */
    case 187: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy182)); 
}
      break;
    case 130: /* table_options */
{
 tfree((yypminor->yy286)); 
}
      break;
    case 139: /* type_name */
{
 
}
      break;
    case 153: /* compare_op */
    case 154: /* in_op */
{
 
}
      break;
    case 166: /* join_type */
{
 
}
      break;
    case 180: /* fill_mode */
{
 
}
      break;
    case 189: /* ordering_specification_opt */
{
 
}
      break;
    case 190: /* null_ordering_opt */
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
void NewParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef NewParse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void NewParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  NewParseFinalize(p);
  (*freeProc)(p);
}
#endif /* NewParse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int NewParseStackPeak(void *p){
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
int NewParseCoverage(FILE *out){
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
   NewParseARG_FETCH
   NewParseCTX_FETCH
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
   NewParseARG_STORE /* Suppress warning about unused %extra_argument var */
   NewParseCTX_STORE
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
  NewParseTOKENTYPE yyMinor        /* The minor token to shift in */
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
  {  123,   -5 }, /* (0) cmd ::= CREATE DATABASE exists_opt db_name db_options */
  {  124,   -3 }, /* (1) exists_opt ::= IF NOT EXISTS */
  {  124,    0 }, /* (2) exists_opt ::= */
  {  126,    0 }, /* (3) db_options ::= */
  {  126,   -3 }, /* (4) db_options ::= db_options BLOCKS NK_INTEGER */
  {  126,   -3 }, /* (5) db_options ::= db_options CACHE NK_INTEGER */
  {  126,   -3 }, /* (6) db_options ::= db_options CACHELAST NK_INTEGER */
  {  126,   -3 }, /* (7) db_options ::= db_options COMP NK_INTEGER */
  {  126,   -3 }, /* (8) db_options ::= db_options DAYS NK_INTEGER */
  {  126,   -3 }, /* (9) db_options ::= db_options FSYNC NK_INTEGER */
  {  126,   -3 }, /* (10) db_options ::= db_options MAXROWS NK_INTEGER */
  {  126,   -3 }, /* (11) db_options ::= db_options MINROWS NK_INTEGER */
  {  126,   -3 }, /* (12) db_options ::= db_options KEEP NK_INTEGER */
  {  126,   -3 }, /* (13) db_options ::= db_options PRECISION NK_STRING */
  {  126,   -3 }, /* (14) db_options ::= db_options QUORUM NK_INTEGER */
  {  126,   -3 }, /* (15) db_options ::= db_options REPLICA NK_INTEGER */
  {  126,   -3 }, /* (16) db_options ::= db_options TTL NK_INTEGER */
  {  126,   -3 }, /* (17) db_options ::= db_options WAL NK_INTEGER */
  {  126,   -3 }, /* (18) db_options ::= db_options VGROUPS NK_INTEGER */
  {  126,   -3 }, /* (19) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  126,   -3 }, /* (20) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  123,   -2 }, /* (21) cmd ::= USE db_name */
  {  123,   -9 }, /* (22) cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  123,   -3 }, /* (23) cmd ::= CREATE TABLE multi_create_clause */
  {  123,   -9 }, /* (24) cmd ::= CREATE STABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  131,   -1 }, /* (25) multi_create_clause ::= create_subtable_clause */
  {  131,   -2 }, /* (26) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  133,   -9 }, /* (27) create_subtable_clause ::= exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  134,    0 }, /* (28) specific_tags_opt ::= */
  {  134,   -3 }, /* (29) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  127,   -1 }, /* (30) full_table_name ::= NK_ID */
  {  127,   -3 }, /* (31) full_table_name ::= NK_ID NK_DOT NK_ID */
  {  128,   -1 }, /* (32) column_def_list ::= column_def */
  {  128,   -3 }, /* (33) column_def_list ::= column_def_list NK_COMMA column_def */
  {  137,   -2 }, /* (34) column_def ::= column_name type_name */
  {  137,   -4 }, /* (35) column_def ::= column_name type_name COMMENT NK_STRING */
  {  139,   -1 }, /* (36) type_name ::= BOOL */
  {  139,   -1 }, /* (37) type_name ::= TINYINT */
  {  139,   -1 }, /* (38) type_name ::= SMALLINT */
  {  139,   -1 }, /* (39) type_name ::= INT */
  {  139,   -1 }, /* (40) type_name ::= INTEGER */
  {  139,   -1 }, /* (41) type_name ::= BIGINT */
  {  139,   -1 }, /* (42) type_name ::= FLOAT */
  {  139,   -1 }, /* (43) type_name ::= DOUBLE */
  {  139,   -4 }, /* (44) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  139,   -1 }, /* (45) type_name ::= TIMESTAMP */
  {  139,   -4 }, /* (46) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  139,   -2 }, /* (47) type_name ::= TINYINT UNSIGNED */
  {  139,   -2 }, /* (48) type_name ::= SMALLINT UNSIGNED */
  {  139,   -2 }, /* (49) type_name ::= INT UNSIGNED */
  {  139,   -2 }, /* (50) type_name ::= BIGINT UNSIGNED */
  {  139,   -1 }, /* (51) type_name ::= JSON */
  {  139,   -4 }, /* (52) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  139,   -1 }, /* (53) type_name ::= MEDIUMBLOB */
  {  139,   -1 }, /* (54) type_name ::= BLOB */
  {  139,   -4 }, /* (55) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  139,   -1 }, /* (56) type_name ::= DECIMAL */
  {  139,   -4 }, /* (57) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  139,   -6 }, /* (58) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  129,    0 }, /* (59) tags_def_opt ::= */
  {  129,   -1 }, /* (60) tags_def_opt ::= tags_def */
  {  132,   -4 }, /* (61) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  130,    0 }, /* (62) table_options ::= */
  {  130,   -3 }, /* (63) table_options ::= table_options COMMENT NK_STRING */
  {  130,   -3 }, /* (64) table_options ::= table_options KEEP NK_INTEGER */
  {  130,   -3 }, /* (65) table_options ::= table_options TTL NK_INTEGER */
  {  130,   -5 }, /* (66) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  136,   -1 }, /* (67) col_name_list ::= col_name */
  {  136,   -3 }, /* (68) col_name_list ::= col_name_list NK_COMMA col_name */
  {  140,   -1 }, /* (69) col_name ::= column_name */
  {  123,   -2 }, /* (70) cmd ::= SHOW DATABASES */
  {  123,   -2 }, /* (71) cmd ::= SHOW TABLES */
  {  123,   -1 }, /* (72) cmd ::= query_expression */
  {  142,   -1 }, /* (73) literal ::= NK_INTEGER */
  {  142,   -1 }, /* (74) literal ::= NK_FLOAT */
  {  142,   -1 }, /* (75) literal ::= NK_STRING */
  {  142,   -1 }, /* (76) literal ::= NK_BOOL */
  {  142,   -2 }, /* (77) literal ::= TIMESTAMP NK_STRING */
  {  142,   -1 }, /* (78) literal ::= duration_literal */
  {  143,   -1 }, /* (79) duration_literal ::= NK_VARIABLE */
  {  135,   -1 }, /* (80) literal_list ::= literal */
  {  135,   -3 }, /* (81) literal_list ::= literal_list NK_COMMA literal */
  {  125,   -1 }, /* (82) db_name ::= NK_ID */
  {  144,   -1 }, /* (83) table_name ::= NK_ID */
  {  138,   -1 }, /* (84) column_name ::= NK_ID */
  {  145,   -1 }, /* (85) function_name ::= NK_ID */
  {  146,   -1 }, /* (86) table_alias ::= NK_ID */
  {  147,   -1 }, /* (87) column_alias ::= NK_ID */
  {  148,   -1 }, /* (88) expression ::= literal */
  {  148,   -1 }, /* (89) expression ::= column_reference */
  {  148,   -4 }, /* (90) expression ::= function_name NK_LP expression_list NK_RP */
  {  148,   -4 }, /* (91) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  148,   -1 }, /* (92) expression ::= subquery */
  {  148,   -3 }, /* (93) expression ::= NK_LP expression NK_RP */
  {  148,   -2 }, /* (94) expression ::= NK_PLUS expression */
  {  148,   -2 }, /* (95) expression ::= NK_MINUS expression */
  {  148,   -3 }, /* (96) expression ::= expression NK_PLUS expression */
  {  148,   -3 }, /* (97) expression ::= expression NK_MINUS expression */
  {  148,   -3 }, /* (98) expression ::= expression NK_STAR expression */
  {  148,   -3 }, /* (99) expression ::= expression NK_SLASH expression */
  {  148,   -3 }, /* (100) expression ::= expression NK_REM expression */
  {  150,   -1 }, /* (101) expression_list ::= expression */
  {  150,   -3 }, /* (102) expression_list ::= expression_list NK_COMMA expression */
  {  149,   -1 }, /* (103) column_reference ::= column_name */
  {  149,   -3 }, /* (104) column_reference ::= table_name NK_DOT column_name */
  {  152,   -3 }, /* (105) predicate ::= expression compare_op expression */
  {  152,   -5 }, /* (106) predicate ::= expression BETWEEN expression AND expression */
  {  152,   -6 }, /* (107) predicate ::= expression NOT BETWEEN expression AND expression */
  {  152,   -3 }, /* (108) predicate ::= expression IS NULL */
  {  152,   -4 }, /* (109) predicate ::= expression IS NOT NULL */
  {  152,   -3 }, /* (110) predicate ::= expression in_op in_predicate_value */
  {  153,   -1 }, /* (111) compare_op ::= NK_LT */
  {  153,   -1 }, /* (112) compare_op ::= NK_GT */
  {  153,   -1 }, /* (113) compare_op ::= NK_LE */
  {  153,   -1 }, /* (114) compare_op ::= NK_GE */
  {  153,   -1 }, /* (115) compare_op ::= NK_NE */
  {  153,   -1 }, /* (116) compare_op ::= NK_EQ */
  {  153,   -1 }, /* (117) compare_op ::= LIKE */
  {  153,   -2 }, /* (118) compare_op ::= NOT LIKE */
  {  153,   -1 }, /* (119) compare_op ::= MATCH */
  {  153,   -1 }, /* (120) compare_op ::= NMATCH */
  {  154,   -1 }, /* (121) in_op ::= IN */
  {  154,   -2 }, /* (122) in_op ::= NOT IN */
  {  155,   -3 }, /* (123) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  156,   -1 }, /* (124) boolean_value_expression ::= boolean_primary */
  {  156,   -2 }, /* (125) boolean_value_expression ::= NOT boolean_primary */
  {  156,   -3 }, /* (126) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  156,   -3 }, /* (127) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  157,   -1 }, /* (128) boolean_primary ::= predicate */
  {  157,   -3 }, /* (129) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  158,   -1 }, /* (130) common_expression ::= expression */
  {  158,   -1 }, /* (131) common_expression ::= boolean_value_expression */
  {  159,   -2 }, /* (132) from_clause ::= FROM table_reference_list */
  {  160,   -1 }, /* (133) table_reference_list ::= table_reference */
  {  160,   -3 }, /* (134) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  161,   -1 }, /* (135) table_reference ::= table_primary */
  {  161,   -1 }, /* (136) table_reference ::= joined_table */
  {  162,   -2 }, /* (137) table_primary ::= table_name alias_opt */
  {  162,   -4 }, /* (138) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  162,   -2 }, /* (139) table_primary ::= subquery alias_opt */
  {  162,   -1 }, /* (140) table_primary ::= parenthesized_joined_table */
  {  164,    0 }, /* (141) alias_opt ::= */
  {  164,   -1 }, /* (142) alias_opt ::= table_alias */
  {  164,   -2 }, /* (143) alias_opt ::= AS table_alias */
  {  165,   -3 }, /* (144) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  165,   -3 }, /* (145) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  163,   -6 }, /* (146) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  166,    0 }, /* (147) join_type ::= */
  {  166,   -1 }, /* (148) join_type ::= INNER */
  {  168,   -9 }, /* (149) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  169,    0 }, /* (150) set_quantifier_opt ::= */
  {  169,   -1 }, /* (151) set_quantifier_opt ::= DISTINCT */
  {  169,   -1 }, /* (152) set_quantifier_opt ::= ALL */
  {  170,   -1 }, /* (153) select_list ::= NK_STAR */
  {  170,   -1 }, /* (154) select_list ::= select_sublist */
  {  176,   -1 }, /* (155) select_sublist ::= select_item */
  {  176,   -3 }, /* (156) select_sublist ::= select_sublist NK_COMMA select_item */
  {  177,   -1 }, /* (157) select_item ::= common_expression */
  {  177,   -2 }, /* (158) select_item ::= common_expression column_alias */
  {  177,   -3 }, /* (159) select_item ::= common_expression AS column_alias */
  {  177,   -3 }, /* (160) select_item ::= table_name NK_DOT NK_STAR */
  {  171,    0 }, /* (161) where_clause_opt ::= */
  {  171,   -2 }, /* (162) where_clause_opt ::= WHERE search_condition */
  {  172,    0 }, /* (163) partition_by_clause_opt ::= */
  {  172,   -3 }, /* (164) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  173,    0 }, /* (165) twindow_clause_opt ::= */
  {  173,   -6 }, /* (166) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  173,   -4 }, /* (167) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  173,   -6 }, /* (168) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  173,   -8 }, /* (169) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  178,    0 }, /* (170) sliding_opt ::= */
  {  178,   -4 }, /* (171) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  179,    0 }, /* (172) fill_opt ::= */
  {  179,   -4 }, /* (173) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  179,   -6 }, /* (174) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  180,   -1 }, /* (175) fill_mode ::= NONE */
  {  180,   -1 }, /* (176) fill_mode ::= PREV */
  {  180,   -1 }, /* (177) fill_mode ::= NULL */
  {  180,   -1 }, /* (178) fill_mode ::= LINEAR */
  {  180,   -1 }, /* (179) fill_mode ::= NEXT */
  {  174,    0 }, /* (180) group_by_clause_opt ::= */
  {  174,   -3 }, /* (181) group_by_clause_opt ::= GROUP BY group_by_list */
  {  181,   -1 }, /* (182) group_by_list ::= expression */
  {  181,   -3 }, /* (183) group_by_list ::= group_by_list NK_COMMA expression */
  {  175,    0 }, /* (184) having_clause_opt ::= */
  {  175,   -2 }, /* (185) having_clause_opt ::= HAVING search_condition */
  {  141,   -4 }, /* (186) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  182,   -1 }, /* (187) query_expression_body ::= query_primary */
  {  182,   -4 }, /* (188) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  186,   -1 }, /* (189) query_primary ::= query_specification */
  {  183,    0 }, /* (190) order_by_clause_opt ::= */
  {  183,   -3 }, /* (191) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  184,    0 }, /* (192) slimit_clause_opt ::= */
  {  184,   -2 }, /* (193) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  184,   -4 }, /* (194) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  184,   -4 }, /* (195) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  185,    0 }, /* (196) limit_clause_opt ::= */
  {  185,   -2 }, /* (197) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  185,   -4 }, /* (198) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  185,   -4 }, /* (199) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  151,   -3 }, /* (200) subquery ::= NK_LP query_expression NK_RP */
  {  167,   -1 }, /* (201) search_condition ::= common_expression */
  {  187,   -1 }, /* (202) sort_specification_list ::= sort_specification */
  {  187,   -3 }, /* (203) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  188,   -3 }, /* (204) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  189,    0 }, /* (205) ordering_specification_opt ::= */
  {  189,   -1 }, /* (206) ordering_specification_opt ::= ASC */
  {  189,   -1 }, /* (207) ordering_specification_opt ::= DESC */
  {  190,    0 }, /* (208) null_ordering_opt ::= */
  {  190,   -2 }, /* (209) null_ordering_opt ::= NULLS FIRST */
  {  190,   -2 }, /* (210) null_ordering_opt ::= NULLS LAST */
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
  NewParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  NewParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  NewParseARG_FETCH
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
      case 0: /* cmd ::= CREATE DATABASE exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy187, &yymsp[-1].minor.yy175, yymsp[0].minor.yy211);}
        break;
      case 1: /* exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy187 = true; }
        break;
      case 2: /* exists_opt ::= */
      case 150: /* set_quantifier_opt ::= */ yytestcase(yyruleno==150);
{ yymsp[1].minor.yy187 = false; }
        break;
      case 3: /* db_options ::= */
{ yymsp[1].minor.yy211 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 4: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 5: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 6: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 7: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 8: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 9: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 10: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 11: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 12: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 13: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 14: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 15: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 16: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 17: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 18: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 19: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 20: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy211 = setDatabaseOption(pCxt, yymsp[-2].minor.yy211, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy211 = yylhsminor.yy211;
        break;
      case 21: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy175);}
        break;
      case 22: /* cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 24: /* cmd ::= CREATE STABLE exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==24);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy187, &yymsp[-5].minor.yy341, yymsp[-3].minor.yy182, yymsp[-1].minor.yy182, yymsp[0].minor.yy286);}
        break;
      case 23: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy182);}
        break;
      case 25: /* multi_create_clause ::= create_subtable_clause */
      case 32: /* column_def_list ::= column_def */ yytestcase(yyruleno==32);
      case 67: /* col_name_list ::= col_name */ yytestcase(yyruleno==67);
      case 155: /* select_sublist ::= select_item */ yytestcase(yyruleno==155);
      case 202: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==202);
{ yylhsminor.yy182 = createNodeList(pCxt, yymsp[0].minor.yy210); }
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 26: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
{ yylhsminor.yy182 = addNodeToList(pCxt, yymsp[-1].minor.yy182, yymsp[0].minor.yy210); }
  yymsp[-1].minor.yy182 = yylhsminor.yy182;
        break;
      case 27: /* create_subtable_clause ::= exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy210 = createCreateSubTableStmt(pCxt, yymsp[-8].minor.yy187, &yymsp[-7].minor.yy341, &yymsp[-5].minor.yy341, yymsp[-4].minor.yy182, yymsp[-1].minor.yy182); }
  yymsp[-8].minor.yy210 = yylhsminor.yy210;
        break;
      case 28: /* specific_tags_opt ::= */
      case 59: /* tags_def_opt ::= */ yytestcase(yyruleno==59);
      case 163: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==163);
      case 180: /* group_by_clause_opt ::= */ yytestcase(yyruleno==180);
      case 190: /* order_by_clause_opt ::= */ yytestcase(yyruleno==190);
{ yymsp[1].minor.yy182 = NULL; }
        break;
      case 29: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy182 = yymsp[-1].minor.yy182; }
        break;
      case 30: /* full_table_name ::= NK_ID */
{ STokenPair t = { .first = nil_token, .second = yymsp[0].minor.yy0 }; yylhsminor.yy341 = t; }
  yymsp[0].minor.yy341 = yylhsminor.yy341;
        break;
      case 31: /* full_table_name ::= NK_ID NK_DOT NK_ID */
{ STokenPair t = { .first = yymsp[-2].minor.yy0, .second = yymsp[0].minor.yy0 }; yylhsminor.yy341 = t; }
  yymsp[-2].minor.yy341 = yylhsminor.yy341;
        break;
      case 33: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 68: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==68);
      case 156: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==156);
      case 203: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==203);
{ yylhsminor.yy182 = addNodeToList(pCxt, yymsp[-2].minor.yy182, yymsp[0].minor.yy210); }
  yymsp[-2].minor.yy182 = yylhsminor.yy182;
        break;
      case 34: /* column_def ::= column_name type_name */
{ yylhsminor.yy210 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy175, yymsp[0].minor.yy42, NULL); }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 35: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy210 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy175, yymsp[-2].minor.yy42, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 36: /* type_name ::= BOOL */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 37: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 38: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 39: /* type_name ::= INT */
      case 40: /* type_name ::= INTEGER */ yytestcase(yyruleno==40);
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 41: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 42: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 43: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 44: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy42 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 45: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 46: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy42 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 47: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy42 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 48: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy42 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 49: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy42 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 50: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy42 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 51: /* type_name ::= JSON */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 52: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy42 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 53: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 54: /* type_name ::= BLOB */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 55: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy42 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 56: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy42 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 57: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy42 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 58: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy42 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 60: /* tags_def_opt ::= tags_def */
      case 154: /* select_list ::= select_sublist */ yytestcase(yyruleno==154);
{ yylhsminor.yy182 = yymsp[0].minor.yy182; }
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 61: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy182 = yymsp[-1].minor.yy182; }
        break;
      case 62: /* table_options ::= */
{ yymsp[1].minor.yy286 = createDefaultTableOptions(pCxt);}
        break;
      case 63: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy286 = setTableOption(pCxt, yymsp[-2].minor.yy286, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy286 = yylhsminor.yy286;
        break;
      case 64: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy286 = setTableOption(pCxt, yymsp[-2].minor.yy286, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy286 = yylhsminor.yy286;
        break;
      case 65: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy286 = setTableOption(pCxt, yymsp[-2].minor.yy286, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy286 = yylhsminor.yy286;
        break;
      case 66: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy286 = setTableSmaOption(pCxt, yymsp[-4].minor.yy286, yymsp[-1].minor.yy182); }
  yymsp[-4].minor.yy286 = yylhsminor.yy286;
        break;
      case 69: /* col_name ::= column_name */
{ yylhsminor.yy210 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy175); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 70: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT); }
        break;
      case 71: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT); }
        break;
      case 72: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy210; }
        break;
      case 73: /* literal ::= NK_INTEGER */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 74: /* literal ::= NK_FLOAT */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 75: /* literal ::= NK_STRING */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 76: /* literal ::= NK_BOOL */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 77: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 78: /* literal ::= duration_literal */
      case 88: /* expression ::= literal */ yytestcase(yyruleno==88);
      case 89: /* expression ::= column_reference */ yytestcase(yyruleno==89);
      case 92: /* expression ::= subquery */ yytestcase(yyruleno==92);
      case 124: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==124);
      case 128: /* boolean_primary ::= predicate */ yytestcase(yyruleno==128);
      case 130: /* common_expression ::= expression */ yytestcase(yyruleno==130);
      case 131: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==131);
      case 133: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==133);
      case 135: /* table_reference ::= table_primary */ yytestcase(yyruleno==135);
      case 136: /* table_reference ::= joined_table */ yytestcase(yyruleno==136);
      case 140: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==140);
      case 187: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==187);
      case 189: /* query_primary ::= query_specification */ yytestcase(yyruleno==189);
{ yylhsminor.yy210 = yymsp[0].minor.yy210; }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 79: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 80: /* literal_list ::= literal */
      case 101: /* expression_list ::= expression */ yytestcase(yyruleno==101);
{ yylhsminor.yy182 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy210)); }
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 81: /* literal_list ::= literal_list NK_COMMA literal */
      case 102: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==102);
{ yylhsminor.yy182 = addNodeToList(pCxt, yymsp[-2].minor.yy182, releaseRawExprNode(pCxt, yymsp[0].minor.yy210)); }
  yymsp[-2].minor.yy182 = yylhsminor.yy182;
        break;
      case 82: /* db_name ::= NK_ID */
      case 83: /* table_name ::= NK_ID */ yytestcase(yyruleno==83);
      case 84: /* column_name ::= NK_ID */ yytestcase(yyruleno==84);
      case 85: /* function_name ::= NK_ID */ yytestcase(yyruleno==85);
      case 86: /* table_alias ::= NK_ID */ yytestcase(yyruleno==86);
      case 87: /* column_alias ::= NK_ID */ yytestcase(yyruleno==87);
{ yylhsminor.yy175 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy175 = yylhsminor.yy175;
        break;
      case 90: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy175, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy175, yymsp[-1].minor.yy182)); }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 91: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy175, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy175, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 93: /* expression ::= NK_LP expression NK_RP */
      case 129: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==129);
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy210)); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 94: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy210));
                                                                                  }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 95: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy210), NULL));
                                                                                  }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 96: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); 
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 97: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); 
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 98: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); 
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 99: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); 
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 100: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); 
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 103: /* column_reference ::= column_name */
{ yylhsminor.yy210 = createRawExprNode(pCxt, &yymsp[0].minor.yy175, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy175)); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 104: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy175, &yymsp[0].minor.yy175, createColumnNode(pCxt, &yymsp[-2].minor.yy175, &yymsp[0].minor.yy175)); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 105: /* predicate ::= expression compare_op expression */
      case 110: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==110);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy140, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210)));
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 106: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy210), releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210)));
                                                                                  }
  yymsp[-4].minor.yy210 = yylhsminor.yy210;
        break;
      case 107: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[-5].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210)));
                                                                                  }
  yymsp[-5].minor.yy210 = yylhsminor.yy210;
        break;
      case 108: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), NULL));
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 109: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy210), NULL));
                                                                                  }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 111: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy140 = OP_TYPE_LOWER_THAN; }
        break;
      case 112: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy140 = OP_TYPE_GREATER_THAN; }
        break;
      case 113: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy140 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 114: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy140 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 115: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy140 = OP_TYPE_NOT_EQUAL; }
        break;
      case 116: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy140 = OP_TYPE_EQUAL; }
        break;
      case 117: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy140 = OP_TYPE_LIKE; }
        break;
      case 118: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy140 = OP_TYPE_NOT_LIKE; }
        break;
      case 119: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy140 = OP_TYPE_MATCH; }
        break;
      case 120: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy140 = OP_TYPE_NMATCH; }
        break;
      case 121: /* in_op ::= IN */
{ yymsp[0].minor.yy140 = OP_TYPE_IN; }
        break;
      case 122: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy140 = OP_TYPE_NOT_IN; }
        break;
      case 123: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy182)); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 125: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy210), NULL));
                                                                                  }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 126: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210)));
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 127: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy210);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), releaseRawExprNode(pCxt, yymsp[0].minor.yy210)));
                                                                                  }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 132: /* from_clause ::= FROM table_reference_list */
      case 162: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==162);
      case 185: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==185);
{ yymsp[-1].minor.yy210 = yymsp[0].minor.yy210; }
        break;
      case 134: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy210 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy210, yymsp[0].minor.yy210, NULL); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 137: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy210 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy175, &yymsp[0].minor.yy175); }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 138: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy210 = createRealTableNode(pCxt, &yymsp[-3].minor.yy175, &yymsp[-1].minor.yy175, &yymsp[0].minor.yy175); }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 139: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy210 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy210), &yymsp[0].minor.yy175); }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 141: /* alias_opt ::= */
{ yymsp[1].minor.yy175 = nil_token;  }
        break;
      case 142: /* alias_opt ::= table_alias */
{ yylhsminor.yy175 = yymsp[0].minor.yy175; }
  yymsp[0].minor.yy175 = yylhsminor.yy175;
        break;
      case 143: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy175 = yymsp[0].minor.yy175; }
        break;
      case 144: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 145: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==145);
{ yymsp[-2].minor.yy210 = yymsp[-1].minor.yy210; }
        break;
      case 146: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy210 = createJoinTableNode(pCxt, yymsp[-4].minor.yy28, yymsp[-5].minor.yy210, yymsp[-2].minor.yy210, yymsp[0].minor.yy210); }
  yymsp[-5].minor.yy210 = yylhsminor.yy210;
        break;
      case 147: /* join_type ::= */
{ yymsp[1].minor.yy28 = JOIN_TYPE_INNER; }
        break;
      case 148: /* join_type ::= INNER */
{ yymsp[0].minor.yy28 = JOIN_TYPE_INNER; }
        break;
      case 149: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy210 = createSelectStmt(pCxt, yymsp[-7].minor.yy187, yymsp[-6].minor.yy182, yymsp[-5].minor.yy210);
                                                                                    yymsp[-8].minor.yy210 = addWhereClause(pCxt, yymsp[-8].minor.yy210, yymsp[-4].minor.yy210);
                                                                                    yymsp[-8].minor.yy210 = addPartitionByClause(pCxt, yymsp[-8].minor.yy210, yymsp[-3].minor.yy182);
                                                                                    yymsp[-8].minor.yy210 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy210, yymsp[-2].minor.yy210);
                                                                                    yymsp[-8].minor.yy210 = addGroupByClause(pCxt, yymsp[-8].minor.yy210, yymsp[-1].minor.yy182);
                                                                                    yymsp[-8].minor.yy210 = addHavingClause(pCxt, yymsp[-8].minor.yy210, yymsp[0].minor.yy210);
                                                                                  }
        break;
      case 151: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy187 = true; }
        break;
      case 152: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy187 = false; }
        break;
      case 153: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy182 = NULL; }
        break;
      case 157: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy210);
                                                                                    yylhsminor.yy210 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy210), &t);
                                                                                  }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 158: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy210 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy210), &yymsp[0].minor.yy175); }
  yymsp[-1].minor.yy210 = yylhsminor.yy210;
        break;
      case 159: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy210 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), &yymsp[0].minor.yy175); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 160: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy210 = createColumnNode(pCxt, &yymsp[-2].minor.yy175, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 161: /* where_clause_opt ::= */
      case 165: /* twindow_clause_opt ::= */ yytestcase(yyruleno==165);
      case 170: /* sliding_opt ::= */ yytestcase(yyruleno==170);
      case 172: /* fill_opt ::= */ yytestcase(yyruleno==172);
      case 184: /* having_clause_opt ::= */ yytestcase(yyruleno==184);
      case 192: /* slimit_clause_opt ::= */ yytestcase(yyruleno==192);
      case 196: /* limit_clause_opt ::= */ yytestcase(yyruleno==196);
{ yymsp[1].minor.yy210 = NULL; }
        break;
      case 164: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 181: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==181);
      case 191: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==191);
{ yymsp[-2].minor.yy182 = yymsp[0].minor.yy182; }
        break;
      case 166: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy210 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy210), &yymsp[-1].minor.yy0); }
        break;
      case 167: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy210 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy210)); }
        break;
      case 168: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy210 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy210, NULL, yymsp[-1].minor.yy210, yymsp[0].minor.yy210); }
        break;
      case 169: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy210 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy210, yymsp[-3].minor.yy210, yymsp[-1].minor.yy210, yymsp[0].minor.yy210); }
        break;
      case 171: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy210 = yymsp[-1].minor.yy210; }
        break;
      case 173: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy210 = createFillNode(pCxt, yymsp[-1].minor.yy102, NULL); }
        break;
      case 174: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy210 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy182)); }
        break;
      case 175: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy102 = FILL_MODE_NONE; }
        break;
      case 176: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy102 = FILL_MODE_PREV; }
        break;
      case 177: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy102 = FILL_MODE_NULL; }
        break;
      case 178: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy102 = FILL_MODE_LINEAR; }
        break;
      case 179: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy102 = FILL_MODE_NEXT; }
        break;
      case 182: /* group_by_list ::= expression */
{ yylhsminor.yy182 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); }
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 183: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy182 = addNodeToList(pCxt, yymsp[-2].minor.yy182, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy210))); }
  yymsp[-2].minor.yy182 = yylhsminor.yy182;
        break;
      case 186: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy210 = addOrderByClause(pCxt, yymsp[-3].minor.yy210, yymsp[-2].minor.yy182);
                                                                                    yylhsminor.yy210 = addSlimitClause(pCxt, yylhsminor.yy210, yymsp[-1].minor.yy210);
                                                                                    yylhsminor.yy210 = addLimitClause(pCxt, yylhsminor.yy210, yymsp[0].minor.yy210);
                                                                                  }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 188: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy210 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy210, yymsp[0].minor.yy210); }
  yymsp[-3].minor.yy210 = yylhsminor.yy210;
        break;
      case 193: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 197: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==197);
{ yymsp[-1].minor.yy210 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 194: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 198: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==198);
{ yymsp[-3].minor.yy210 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 195: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 199: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-3].minor.yy210 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 200: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy210 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy210); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 201: /* search_condition ::= common_expression */
{ yylhsminor.yy210 = releaseRawExprNode(pCxt, yymsp[0].minor.yy210); }
  yymsp[0].minor.yy210 = yylhsminor.yy210;
        break;
      case 204: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy210 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy210), yymsp[-1].minor.yy29, yymsp[0].minor.yy307); }
  yymsp[-2].minor.yy210 = yylhsminor.yy210;
        break;
      case 205: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy29 = ORDER_ASC; }
        break;
      case 206: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy29 = ORDER_ASC; }
        break;
      case 207: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy29 = ORDER_DESC; }
        break;
      case 208: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy307 = NULL_ORDER_DEFAULT; }
        break;
      case 209: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy307 = NULL_ORDER_FIRST; }
        break;
      case 210: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy307 = NULL_ORDER_LAST; }
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
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  NewParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
  
  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > pCxt->pQueryCxt->msgLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        sprintf(pCxt->pQueryCxt->pMsg, msg, tmpstr);
    } else {
        sprintf(pCxt->pQueryCxt->pMsg, msg, &TOKEN.z[0]);
    }
  } else {
    sprintf(pCxt->pQueryCxt->pMsg, "Incomplete SQL statement");
  }
  pCxt->valid = false;
/************ End %syntax_error code ******************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "NewParseAlloc" which describes the current state of the parser.
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
void NewParse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  NewParseTOKENTYPE yyminor       /* The value for the token */
  NewParseARG_PDECL               /* Optional %extra_argument parameter */
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
  NewParseCTX_FETCH
  NewParseARG_STORE

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
                        yyminor NewParseCTX_PARAM);
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
int NewParseFallback(int iToken){
#ifdef YYFALLBACK
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
