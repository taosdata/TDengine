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
#define YYNOCODE 209
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SNodeList* yy46;
  SDataType yy70;
  SToken yy129;
  ENullOrder yy147;
  bool yy185;
  EOrder yy202;
  SNode* yy256;
  EJoinType yy266;
  EOperatorType yy326;
  STableOptions* yy340;
  EFillMode yy360;
  SDatabaseOptions* yy391;
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
#define YYNSTATE             279
#define YYNRULE              237
#define YYNTOKEN             135
#define YY_MAX_SHIFT         278
#define YY_MIN_SHIFTREDUCE   439
#define YY_MAX_SHIFTREDUCE   675
#define YY_ERROR_ACTION      676
#define YY_ACCEPT_ACTION     677
#define YY_NO_ACTION         678
#define YY_MIN_REDUCE        679
#define YY_MAX_REDUCE        915
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
#define YY_ACTTAB_COUNT (905)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   137,  149,   23,   95,  772,  721,  770,  247,  150,  784,
 /*    10 */   782,  148,   30,   28,   26,   25,   24,  784,  782,  194,
 /*    20 */   177,  808,  201,  735,   66,   30,   28,   26,   25,   24,
 /*    30 */   692,  166,  194,  222,  808,   60,  208,  132,  793,  782,
 /*    40 */   195,  209,  222,   54,  794,  730,  797,  833,  733,   58,
 /*    50 */   132,  139,  829,  906,   19,  785,  782,  733,  558,   73,
 /*    60 */   840,  841,  867,  845,   30,   28,   26,   25,   24,  271,
 /*    70 */   270,  269,  268,  267,  266,  265,  264,  263,  262,  261,
 /*    80 */   260,  259,  258,  257,  256,  255,   26,   25,   24,  550,
 /*    90 */    22,  141,  171,  576,  577,  578,  579,  580,  581,  582,
 /*   100 */   584,  585,  586,   22,  141,  617,  576,  577,  578,  579,
 /*   110 */   580,  581,  582,  584,  585,  586,  275,  274,  499,  245,
 /*   120 */   244,  243,  503,  242,  505,  506,  241,  508,  238,   41,
 /*   130 */   514,  235,  516,  517,  232,  229,  194,  194,  808,  808,
 /*   140 */   729,  184,  793,  782,  195,   77,   45,   53,  794,  170,
 /*   150 */   797,  833,  194,  719,  808,  131,  829,  726,  793,  782,
 /*   160 */   195,  104,   93,  125,  794,  145,  797,  894,  153,   78,
 /*   170 */   221,  772,  180,  770,  808,  103,  736,   66,  793,  782,
 /*   180 */   195,   76,  209,   54,  794,  892,  797,  833,  194,  254,
 /*   190 */   808,  139,  829,   71,  793,  782,  195,  221,   42,   54,
 /*   200 */   794,  101,  797,  833,  772,   94,  771,  139,  829,  906,
 /*   210 */   616,  156,  860,  894,  180,  254,  808,  551,  890,   50,
 /*   220 */   793,  782,  195,   10,   47,   54,  794,  893,  797,  833,
 /*   230 */   194,  892,  808,  139,  829,   71,  793,  782,  195,   29,
 /*   240 */    27,   54,  794,  677,  797,  833,   41,  187,  539,  139,
 /*   250 */   829,  906,  114,   61,  861,  763,  537,  728,  146,  221,
 /*   260 */   851,   29,   27,  618,   11,  194,  182,  808,  196,  548,
 /*   270 */   539,  793,  782,  195,   77,  639,  121,  794,  537,  797,
 /*   280 */   146,  194,   56,  808,    1,   10,   11,  793,  782,  195,
 /*   290 */    79,   51,   55,  794,  894,  797,  833,    9,    8,   62,
 /*   300 */   832,  829,  725,  223,  222,  449,    1,  219,   76,  173,
 /*   310 */   248,  152,  892,  212,  538,  540,  543,  720,  191,  733,
 /*   320 */   194,  573,  808,  735,   66,  223,  793,  782,  195,    6,
 /*   330 */   613,  125,  794,  157,  797,   77,  538,  540,  543,  194,
 /*   340 */   222,  808,   96,  220,  184,  793,  782,  195,   29,   27,
 /*   350 */   119,  794,  163,  797,  594,  733,  847,  539,  172,  167,
 /*   360 */   165,   88,   29,   27,  251,  537,  188,  146,  250,  186,
 /*   370 */   894,  539,  222,   11,  844,  109,  847,  177,  852,  537,
 /*   380 */   613,  146,  192,  252,   76,  863,  154,  733,  892,  194,
 /*   390 */   177,  808,   60,    1,  843,  793,  782,  195,  735,   66,
 /*   400 */    55,  794,  249,  797,  833,   60,   58,    7,  181,  829,
 /*   410 */   184,  449,  223,    9,    8,  179,   72,  840,  841,   58,
 /*   420 */   845,  450,  451,  538,  540,  543,  223,   85,  189,   91,
 /*   430 */   840,  176,  625,  175,   82,  847,  894,  538,  540,  543,
 /*   440 */   178,   98,   29,   27,  183,  222,   29,   27,  155,  548,
 /*   450 */    76,  539,  809,  842,  892,  539,  674,  675,   77,  537,
 /*   460 */   733,  146,  194,  537,  808,  146,   20,    2,  793,  782,
 /*   470 */   195,   29,   27,   55,  794,  583,  797,  833,  587,  211,
 /*   480 */   539,  217,  830,  551,  215,  642,  539,    7,  537,  588,
 /*   490 */   146,    1,  864,  194,  537,  808,   31,  164,  161,  793,
 /*   500 */   782,  195,  555,  874,   68,  794,  223,  797,   80,   31,
 /*   510 */   223,  162,  640,  641,  643,  644,    7,  538,  540,  543,
 /*   520 */   789,  538,  540,  543,  106,  543,  194,  787,  808,  492,
 /*   530 */   159,   63,  793,  782,  195,  223,   64,  125,  794,  140,
 /*   540 */   797,  223,  873,  185,  907,  718,  538,  540,  543,  194,
 /*   550 */   160,  808,  538,  540,  543,  793,  782,  195,  487,   84,
 /*   560 */    68,  794,  138,  797,  194,   56,  808,    5,  854,  174,
 /*   570 */   793,  782,  195,  520,   70,  120,  794,  524,  797,  194,
 /*   580 */   227,  808,    4,   87,   63,  793,  782,  195,  158,  613,
 /*   590 */   122,  794,  251,  797,   89,  194,  250,  808,  529,  547,
 /*   600 */   908,  793,  782,  195,   59,   64,  117,  794,   65,  797,
 /*   610 */   194,  252,  808,  550,  848,   63,  793,  782,  195,   32,
 /*   620 */    16,  123,  794,  142,  797,  194,  815,  808,   90,  909,
 /*   630 */   249,  793,  782,  195,  193,  891,  118,  794,  194,  797,
 /*   640 */   808,   97,  546,  190,  793,  782,  195,  197,  210,  124,
 /*   650 */   794,  194,  797,  808,   40,  102,  552,  793,  782,  195,
 /*   660 */   734,  213,  805,  794,  147,  797,  194,   46,  808,  113,
 /*   670 */    44,  225,  793,  782,  195,  115,  110,  804,  794,  194,
 /*   680 */   797,  808,  278,    3,  128,  793,  782,  195,  129,  116,
 /*   690 */   803,  794,   31,  797,  194,   14,  808,   81,  636,   83,
 /*   700 */   793,  782,  195,   35,  638,  135,  794,  194,  797,  808,
 /*   710 */    69,   86,   37,  793,  782,  195,  632,  631,  134,  794,
 /*   720 */   194,  797,  808,  168,   38,  169,  793,  782,  195,  787,
 /*   730 */   610,  136,  794,   18,  797,  194,   15,  808,  609,   92,
 /*   740 */   207,  793,  782,  195,  206,  546,  133,  794,  205,  797,
 /*   750 */    33,  194,   34,  808,   75,    8,  574,  793,  782,  195,
 /*   760 */   556,  665,  126,  794,   17,  797,  177,  202,   12,   39,
 /*   770 */   660,  659,  143,  664,  204,  203,   30,   28,   26,   25,
 /*   780 */    24,   60,   99,  663,  130,  144,   13,  776,  218,  695,
 /*   790 */   127,   67,  775,  774,  112,   58,  200,  773,  199,  724,
 /*   800 */   198,  100,   57,   21,  723,   74,  840,  841,  111,  845,
 /*   810 */   694,  688,  683,   30,   28,   26,   25,   24,   30,   28,
 /*   820 */    26,   25,   24,  722,  458,  693,   30,   28,   26,   25,
 /*   830 */    24,   52,  687,  686,  107,  682,  681,  214,  680,  216,
 /*   840 */   105,   43,   47,  786,  226,  108,  224,  541,   36,  151,
 /*   850 */   513,  230,  521,  228,  512,  511,  518,  231,  233,  236,
 /*   860 */   515,  509,  234,  239,  558,  237,  510,  507,  498,  528,
 /*   870 */   240,  527,  526,  246,   77,  456,   48,  477,  253,  476,
 /*   880 */   470,   49,  475,  474,  473,  472,  471,  469,  685,  468,
 /*   890 */   467,  466,  465,  464,  671,  672,  463,  462,  461,  272,
 /*   900 */   273,  684,  679,  276,  277,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   152,  154,  171,  172,  157,    0,  159,  158,  152,  161,
 /*    10 */   162,  144,   12,   13,   14,   15,   16,  161,  162,  155,
 /*    20 */   140,  157,  140,  156,  157,   12,   13,   14,   15,   16,
 /*    30 */     0,  167,  155,  140,  157,  155,  143,   37,  161,  162,
 /*    40 */   163,   36,  140,  166,  167,  143,  169,  170,  155,  169,
 /*    50 */    37,  174,  175,  176,    2,  161,  162,  155,   58,  179,
 /*    60 */   180,  181,  185,  183,   12,   13,   14,   15,   16,   39,
 /*    70 */    40,   41,   42,   43,   44,   45,   46,   47,   48,   49,
 /*    80 */    50,   51,   52,   53,   54,   55,   14,   15,   16,   31,
 /*    90 */    90,   91,   31,   93,   94,   95,   96,   97,   98,   99,
 /*   100 */   100,  101,  102,   90,   91,    4,   93,   94,   95,   96,
 /*   110 */    97,   98,   99,  100,  101,  102,  137,  138,   67,   68,
 /*   120 */    69,   70,   71,   72,   73,   74,   75,   76,   77,  142,
 /*   130 */    79,   80,   81,   82,   83,   84,  155,  155,  157,  157,
 /*   140 */   153,  160,  161,  162,  163,  108,  139,  166,  167,  167,
 /*   150 */   169,  170,  155,    0,  157,  174,  175,  150,  161,  162,
 /*   160 */   163,   19,  104,  166,  167,  168,  169,  186,  154,   27,
 /*   170 */    31,  157,  155,  159,  157,   33,  156,  157,  161,  162,
 /*   180 */   163,  200,   36,  166,  167,  204,  169,  170,  155,   36,
 /*   190 */   157,  174,  175,  176,  161,  162,  163,   31,   56,  166,
 /*   200 */   167,   59,  169,  170,  157,  188,  159,  174,  175,  176,
 /*   210 */   109,  194,  195,  186,  155,   36,  157,   31,  185,   57,
 /*   220 */   161,  162,  163,   57,   62,  166,  167,  200,  169,  170,
 /*   230 */   155,  204,  157,  174,  175,  176,  161,  162,  163,   12,
 /*   240 */    13,  166,  167,  135,  169,  170,  142,    3,   21,  174,
 /*   250 */   175,  176,  145,  149,  195,  148,   29,  153,   31,   31,
 /*   260 */   185,   12,   13,   14,   37,  155,   37,  157,  160,   31,
 /*   270 */    21,  161,  162,  163,  108,   58,  166,  167,   29,  169,
 /*   280 */    31,  155,   65,  157,   57,   57,   37,  161,  162,  163,
 /*   290 */   104,  139,  166,  167,  186,  169,  170,    1,    2,  147,
 /*   300 */   174,  175,  150,   76,  140,   21,   57,  143,  200,  199,
 /*   310 */    63,  144,  204,   29,   87,   88,   89,    0,   65,  155,
 /*   320 */   155,   92,  157,  156,  157,   76,  161,  162,  163,  106,
 /*   330 */   107,  166,  167,  168,  169,  108,   87,   88,   89,  155,
 /*   340 */   140,  157,  207,  143,  160,  161,  162,  163,   12,   13,
 /*   350 */   166,  167,  198,  169,   58,  155,  164,   21,  113,  114,
 /*   360 */   115,  191,   12,   13,   47,   29,   65,   31,   51,  125,
 /*   370 */   186,   21,  140,   37,  182,  143,  164,  140,  105,   29,
 /*   380 */   107,   31,  129,   66,  200,  165,  144,  155,  204,  155,
 /*   390 */   140,  157,  155,   57,  182,  161,  162,  163,  156,  157,
 /*   400 */   166,  167,   85,  169,  170,  155,  169,   57,  174,  175,
 /*   410 */   160,   21,   76,    1,    2,  178,  179,  180,  181,  169,
 /*   420 */   183,   31,   32,   87,   88,   89,   76,   58,  127,  179,
 /*   430 */   180,  181,   14,  183,   65,  164,  186,   87,   88,   89,
 /*   440 */   184,  201,   12,   13,   14,  140,   12,   13,  143,   31,
 /*   450 */   200,   21,  157,  182,  204,   21,  133,  134,  108,   29,
 /*   460 */   155,   31,  155,   29,  157,   31,   90,  187,  161,  162,
 /*   470 */   163,   12,   13,  166,  167,   99,  169,  170,  102,  137,
 /*   480 */    21,   20,  175,   31,   23,   92,   21,   57,   29,   58,
 /*   490 */    31,   57,  165,  155,   29,  157,   65,  117,  116,  161,
 /*   500 */   162,  163,   58,  197,  166,  167,   76,  169,  196,   65,
 /*   510 */    76,  118,  119,  120,  121,  122,   57,   87,   88,   89,
 /*   520 */    57,   87,   88,   89,   58,   89,  155,   64,  157,   58,
 /*   530 */   162,   65,  161,  162,  163,   76,   65,  166,  167,  168,
 /*   540 */   169,   76,  197,  205,  206,    0,   87,   88,   89,  155,
 /*   550 */   162,  157,   87,   88,   89,  161,  162,  163,   58,  196,
 /*   560 */   166,  167,  162,  169,  155,   65,  157,  124,  193,  123,
 /*   570 */   161,  162,  163,   58,  190,  166,  167,   58,  169,  155,
 /*   580 */    65,  157,  110,  192,   65,  161,  162,  163,  111,  107,
 /*   590 */   166,  167,   47,  169,  189,  155,   51,  157,   58,   31,
 /*   600 */   206,  161,  162,  163,  155,   65,  166,  167,   58,  169,
 /*   610 */   155,   66,  157,   31,  164,   65,  161,  162,  163,  103,
 /*   620 */    57,  166,  167,  132,  169,  155,  173,  157,  177,  208,
 /*   630 */    85,  161,  162,  163,  128,  203,  166,  167,  155,  169,
 /*   640 */   157,  202,   31,  126,  161,  162,  163,  140,  140,  166,
 /*   650 */   167,  155,  169,  157,  142,  142,   31,  161,  162,  163,
 /*   660 */   155,  136,  166,  167,  136,  169,  155,   57,  157,  148,
 /*   670 */   139,  151,  161,  162,  163,  140,  139,  166,  167,  155,
 /*   680 */   169,  157,  136,   65,  146,  161,  162,  163,  146,  141,
 /*   690 */   166,  167,   65,  169,  155,  112,  157,   58,   58,   57,
 /*   700 */   161,  162,  163,   65,   58,  166,  167,  155,  169,  157,
 /*   710 */    57,   57,   57,  161,  162,  163,   58,   58,  166,  167,
 /*   720 */   155,  169,  157,   29,   57,   65,  161,  162,  163,   64,
 /*   730 */    58,  166,  167,   65,  169,  155,  112,  157,   58,   64,
 /*   740 */    26,  161,  162,  163,   30,   31,  166,  167,   34,  169,
 /*   750 */   105,  155,   65,  157,   64,    2,   92,  161,  162,  163,
 /*   760 */    58,   58,  166,  167,   65,  169,  140,   53,  112,    4,
 /*   770 */    29,   29,   29,   29,   60,   61,   12,   13,   14,   15,
 /*   780 */    16,  155,   64,   29,   18,   29,   57,    0,   22,    0,
 /*   790 */    24,   25,    0,    0,   19,  169,   64,    0,   53,    0,
 /*   800 */    86,   35,   27,    2,    0,  179,  180,  181,   33,  183,
 /*   810 */     0,    0,    0,   12,   13,   14,   15,   16,   12,   13,
 /*   820 */    14,   15,   16,    0,   38,    0,   12,   13,   14,   15,
 /*   830 */    16,   56,    0,    0,   59,    0,    0,   21,    0,   21,
 /*   840 */    19,   57,   62,   64,   29,   64,   63,   21,   57,   29,
 /*   850 */    78,   29,   58,   57,   78,   78,   58,   57,   29,   29,
 /*   860 */    58,   58,   57,   29,   58,   57,   78,   58,   21,   29,
 /*   870 */    57,   29,   21,   66,  108,   38,   57,   29,   37,   29,
 /*   880 */    21,   57,   29,   29,   29,   29,   29,   29,    0,   29,
 /*   890 */    29,   29,   29,   29,  130,  131,   29,   29,   29,   29,
 /*   900 */    28,    0,    0,   21,   20,  209,  209,  209,  209,  209,
 /*   910 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   920 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   930 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   940 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   950 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   960 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   970 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   980 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   990 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*  1000 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*  1010 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*  1020 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*  1030 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
};
#define YY_SHIFT_COUNT    (278)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (902)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   766,  227,  249,  336,  336,  336,  336,  350,  336,  336,
 /*    10 */   166,  434,  459,  430,  459,  459,  459,  459,  459,  459,
 /*    20 */   459,  459,  459,  459,  459,  459,  459,  459,  459,  459,
 /*    30 */   459,  459,  228,  228,  228,  465,  465,   61,   61,   37,
 /*    40 */   139,  139,  146,  238,  139,  139,  238,  139,  238,  238,
 /*    50 */   238,  139,  179,    0,   13,   13,  465,  390,   58,   58,
 /*    60 */    58,    5,  153,  238,  238,  247,   51,  714,  764,  393,
 /*    70 */   245,  186,  273,  223,  273,  418,  244,  101,  284,  452,
 /*    80 */   380,  382,  436,  436,  380,  382,  436,  443,  446,  477,
 /*    90 */   472,  482,  568,  582,  516,  563,  491,  506,  517,  238,
 /*   100 */   611,  146,  611,  146,  625,  625,  247,  179,  568,  610,
 /*   110 */   611,  179,  625,  905,  905,  905,   30,   52,  801,  806,
 /*   120 */   814,  814,  814,  814,  814,  814,  814,  142,  317,  545,
 /*   130 */   775,  296,  376,   72,   72,   72,   72,  217,  369,  412,
 /*   140 */   431,  229,  323,  301,  253,  444,  463,  461,  466,  471,
 /*   150 */   500,  515,  519,  540,  550,  162,  618,  627,  583,  639,
 /*   160 */   640,  642,  638,  646,  653,  654,  658,  655,  659,  694,
 /*   170 */   660,  665,  667,  668,  624,  672,  680,  675,  645,  687,
 /*   180 */   690,  753,  664,  702,  703,  699,  656,  765,  741,  742,
 /*   190 */   743,  744,  754,  756,  718,  729,  787,  789,  792,  793,
 /*   200 */   745,  732,  797,  799,  804,  810,  811,  812,  823,  786,
 /*   210 */   825,  832,  833,  835,  836,  816,  838,  818,  821,  784,
 /*   220 */   780,  779,  781,  826,  791,  783,  794,  815,  820,  796,
 /*   230 */   798,  822,  800,  802,  829,  805,  803,  830,  808,  809,
 /*   240 */   834,  813,  772,  776,  777,  788,  847,  807,  819,  824,
 /*   250 */   840,  842,  851,  837,  841,  848,  850,  853,  854,  855,
 /*   260 */   856,  857,  859,  858,  860,  861,  862,  863,  864,  867,
 /*   270 */   868,  869,  888,  870,  872,  901,  902,  882,  884,
};
#define YY_REDUCE_COUNT (115)
#define YY_REDUCE_MIN   (-169)
#define YY_REDUCE_MAX   (626)
static const short yy_reduce_ofst[] = {
 /*     0 */   108,  -19,   17,   59, -123,   33,   75,  184,  126,  234,
 /*    10 */   250,  307,  338,   -3,  165,  110,  371,  394,  409,  424,
 /*    20 */   440,  455,  470,  483,  496,  511,  524,  539,  552,  565,
 /*    30 */   580,  596,  237, -120,  626, -152, -144, -136,  -18,   27,
 /*    40 */  -107,  -98,  104, -133,  164,  200, -153,  232,  167,   14,
 /*    50 */   242,  305,  152, -169, -169, -169, -106,  -21,  192,  212,
 /*    60 */   271,  -13,    7,   20,   47,  107, -151, -118,  135,  154,
 /*    70 */   170,  220,  256,  256,  256,  295,  240,  280,  342,  327,
 /*    80 */   306,  312,  368,  388,  345,  363,  400,  375,  391,  384,
 /*    90 */   405,  256,  449,  450,  451,  453,  421,  432,  439,  295,
 /*   100 */   507,  512,  508,  513,  525,  528,  521,  531,  505,  520,
 /*   110 */   535,  537,  546,  538,  542,  548,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*    10 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*    20 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*    30 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*    40 */   676,  676,  699,  676,  676,  676,  676,  676,  676,  676,
 /*    50 */   676,  676,  697,  676,  835,  676,  676,  676,  846,  846,
 /*    60 */   846,  699,  697,  676,  676,  762,  676,  676,  910,  676,
 /*    70 */   870,  862,  838,  852,  839,  676,  895,  855,  676,  676,
 /*    80 */   877,  875,  676,  676,  877,  875,  676,  889,  885,  868,
 /*    90 */   866,  852,  676,  676,  676,  676,  913,  901,  897,  676,
 /*   100 */   676,  699,  676,  699,  676,  676,  676,  697,  676,  731,
 /*   110 */   676,  697,  676,  765,  765,  700,  676,  676,  676,  676,
 /*   120 */   888,  887,  812,  811,  810,  806,  807,  676,  676,  676,
 /*   130 */   676,  676,  676,  801,  802,  800,  799,  676,  676,  836,
 /*   140 */   676,  676,  676,  898,  902,  676,  788,  676,  676,  676,
 /*   150 */   676,  676,  676,  676,  676,  676,  859,  869,  676,  676,
 /*   160 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   170 */   676,  788,  676,  886,  676,  845,  841,  676,  676,  837,
 /*   180 */   676,  831,  676,  676,  676,  896,  676,  676,  676,  676,
 /*   190 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   200 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   210 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   220 */   676,  787,  676,  676,  676,  676,  676,  676,  676,  759,
 /*   230 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   240 */   676,  676,  744,  742,  741,  740,  676,  737,  676,  676,
 /*   250 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   260 */   676,  676,  676,  676,  676,  676,  676,  676,  676,  676,
 /*   270 */   676,  676,  676,  676,  676,  676,  676,  676,  676,
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
  /*   25 */ "SHOW",
  /*   26 */ "USERS",
  /*   27 */ "DNODE",
  /*   28 */ "PORT",
  /*   29 */ "NK_INTEGER",
  /*   30 */ "DNODES",
  /*   31 */ "NK_ID",
  /*   32 */ "NK_IPTOKEN",
  /*   33 */ "DATABASE",
  /*   34 */ "DATABASES",
  /*   35 */ "USE",
  /*   36 */ "IF",
  /*   37 */ "NOT",
  /*   38 */ "EXISTS",
  /*   39 */ "BLOCKS",
  /*   40 */ "CACHE",
  /*   41 */ "CACHELAST",
  /*   42 */ "COMP",
  /*   43 */ "DAYS",
  /*   44 */ "FSYNC",
  /*   45 */ "MAXROWS",
  /*   46 */ "MINROWS",
  /*   47 */ "KEEP",
  /*   48 */ "PRECISION",
  /*   49 */ "QUORUM",
  /*   50 */ "REPLICA",
  /*   51 */ "TTL",
  /*   52 */ "WAL",
  /*   53 */ "VGROUPS",
  /*   54 */ "SINGLE_STABLE",
  /*   55 */ "STREAM_MODE",
  /*   56 */ "TABLE",
  /*   57 */ "NK_LP",
  /*   58 */ "NK_RP",
  /*   59 */ "STABLE",
  /*   60 */ "TABLES",
  /*   61 */ "STABLES",
  /*   62 */ "USING",
  /*   63 */ "TAGS",
  /*   64 */ "NK_DOT",
  /*   65 */ "NK_COMMA",
  /*   66 */ "COMMENT",
  /*   67 */ "BOOL",
  /*   68 */ "TINYINT",
  /*   69 */ "SMALLINT",
  /*   70 */ "INT",
  /*   71 */ "INTEGER",
  /*   72 */ "BIGINT",
  /*   73 */ "FLOAT",
  /*   74 */ "DOUBLE",
  /*   75 */ "BINARY",
  /*   76 */ "TIMESTAMP",
  /*   77 */ "NCHAR",
  /*   78 */ "UNSIGNED",
  /*   79 */ "JSON",
  /*   80 */ "VARCHAR",
  /*   81 */ "MEDIUMBLOB",
  /*   82 */ "BLOB",
  /*   83 */ "VARBINARY",
  /*   84 */ "DECIMAL",
  /*   85 */ "SMA",
  /*   86 */ "MNODES",
  /*   87 */ "NK_FLOAT",
  /*   88 */ "NK_BOOL",
  /*   89 */ "NK_VARIABLE",
  /*   90 */ "BETWEEN",
  /*   91 */ "IS",
  /*   92 */ "NULL",
  /*   93 */ "NK_LT",
  /*   94 */ "NK_GT",
  /*   95 */ "NK_LE",
  /*   96 */ "NK_GE",
  /*   97 */ "NK_NE",
  /*   98 */ "NK_EQ",
  /*   99 */ "LIKE",
  /*  100 */ "MATCH",
  /*  101 */ "NMATCH",
  /*  102 */ "IN",
  /*  103 */ "FROM",
  /*  104 */ "AS",
  /*  105 */ "JOIN",
  /*  106 */ "ON",
  /*  107 */ "INNER",
  /*  108 */ "SELECT",
  /*  109 */ "DISTINCT",
  /*  110 */ "WHERE",
  /*  111 */ "PARTITION",
  /*  112 */ "BY",
  /*  113 */ "SESSION",
  /*  114 */ "STATE_WINDOW",
  /*  115 */ "INTERVAL",
  /*  116 */ "SLIDING",
  /*  117 */ "FILL",
  /*  118 */ "VALUE",
  /*  119 */ "NONE",
  /*  120 */ "PREV",
  /*  121 */ "LINEAR",
  /*  122 */ "NEXT",
  /*  123 */ "GROUP",
  /*  124 */ "HAVING",
  /*  125 */ "ORDER",
  /*  126 */ "SLIMIT",
  /*  127 */ "SOFFSET",
  /*  128 */ "LIMIT",
  /*  129 */ "OFFSET",
  /*  130 */ "ASC",
  /*  131 */ "DESC",
  /*  132 */ "NULLS",
  /*  133 */ "FIRST",
  /*  134 */ "LAST",
  /*  135 */ "cmd",
  /*  136 */ "user_name",
  /*  137 */ "dnode_endpoint",
  /*  138 */ "dnode_host_name",
  /*  139 */ "not_exists_opt",
  /*  140 */ "db_name",
  /*  141 */ "db_options",
  /*  142 */ "exists_opt",
  /*  143 */ "full_table_name",
  /*  144 */ "column_def_list",
  /*  145 */ "tags_def_opt",
  /*  146 */ "table_options",
  /*  147 */ "multi_create_clause",
  /*  148 */ "tags_def",
  /*  149 */ "multi_drop_clause",
  /*  150 */ "create_subtable_clause",
  /*  151 */ "specific_tags_opt",
  /*  152 */ "literal_list",
  /*  153 */ "drop_table_clause",
  /*  154 */ "col_name_list",
  /*  155 */ "table_name",
  /*  156 */ "column_def",
  /*  157 */ "column_name",
  /*  158 */ "type_name",
  /*  159 */ "col_name",
  /*  160 */ "query_expression",
  /*  161 */ "literal",
  /*  162 */ "duration_literal",
  /*  163 */ "function_name",
  /*  164 */ "table_alias",
  /*  165 */ "column_alias",
  /*  166 */ "expression",
  /*  167 */ "column_reference",
  /*  168 */ "expression_list",
  /*  169 */ "subquery",
  /*  170 */ "predicate",
  /*  171 */ "compare_op",
  /*  172 */ "in_op",
  /*  173 */ "in_predicate_value",
  /*  174 */ "boolean_value_expression",
  /*  175 */ "boolean_primary",
  /*  176 */ "common_expression",
  /*  177 */ "from_clause",
  /*  178 */ "table_reference_list",
  /*  179 */ "table_reference",
  /*  180 */ "table_primary",
  /*  181 */ "joined_table",
  /*  182 */ "alias_opt",
  /*  183 */ "parenthesized_joined_table",
  /*  184 */ "join_type",
  /*  185 */ "search_condition",
  /*  186 */ "query_specification",
  /*  187 */ "set_quantifier_opt",
  /*  188 */ "select_list",
  /*  189 */ "where_clause_opt",
  /*  190 */ "partition_by_clause_opt",
  /*  191 */ "twindow_clause_opt",
  /*  192 */ "group_by_clause_opt",
  /*  193 */ "having_clause_opt",
  /*  194 */ "select_sublist",
  /*  195 */ "select_item",
  /*  196 */ "sliding_opt",
  /*  197 */ "fill_opt",
  /*  198 */ "fill_mode",
  /*  199 */ "group_by_list",
  /*  200 */ "query_expression_body",
  /*  201 */ "order_by_clause_opt",
  /*  202 */ "slimit_clause_opt",
  /*  203 */ "limit_clause_opt",
  /*  204 */ "query_primary",
  /*  205 */ "sort_specification_list",
  /*  206 */ "sort_specification",
  /*  207 */ "ordering_specification_opt",
  /*  208 */ "null_ordering_opt",
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
 /*   4 */ "cmd ::= SHOW USERS",
 /*   5 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*   6 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*   7 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*   8 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*   9 */ "cmd ::= SHOW DNODES",
 /*  10 */ "dnode_endpoint ::= NK_STRING",
 /*  11 */ "dnode_host_name ::= NK_ID",
 /*  12 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  13 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  14 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  15 */ "cmd ::= SHOW DATABASES",
 /*  16 */ "cmd ::= USE db_name",
 /*  17 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  18 */ "not_exists_opt ::=",
 /*  19 */ "exists_opt ::= IF EXISTS",
 /*  20 */ "exists_opt ::=",
 /*  21 */ "db_options ::=",
 /*  22 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  23 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  24 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  25 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  26 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  27 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  28 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  29 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  30 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  31 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  32 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  33 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  34 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  35 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  36 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  37 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  38 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  39 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  40 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  41 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  42 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  43 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  44 */ "cmd ::= SHOW TABLES",
 /*  45 */ "cmd ::= SHOW STABLES",
 /*  46 */ "multi_create_clause ::= create_subtable_clause",
 /*  47 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  48 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  49 */ "multi_drop_clause ::= drop_table_clause",
 /*  50 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  51 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  52 */ "specific_tags_opt ::=",
 /*  53 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  54 */ "full_table_name ::= table_name",
 /*  55 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  56 */ "column_def_list ::= column_def",
 /*  57 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  58 */ "column_def ::= column_name type_name",
 /*  59 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  60 */ "type_name ::= BOOL",
 /*  61 */ "type_name ::= TINYINT",
 /*  62 */ "type_name ::= SMALLINT",
 /*  63 */ "type_name ::= INT",
 /*  64 */ "type_name ::= INTEGER",
 /*  65 */ "type_name ::= BIGINT",
 /*  66 */ "type_name ::= FLOAT",
 /*  67 */ "type_name ::= DOUBLE",
 /*  68 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  69 */ "type_name ::= TIMESTAMP",
 /*  70 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  71 */ "type_name ::= TINYINT UNSIGNED",
 /*  72 */ "type_name ::= SMALLINT UNSIGNED",
 /*  73 */ "type_name ::= INT UNSIGNED",
 /*  74 */ "type_name ::= BIGINT UNSIGNED",
 /*  75 */ "type_name ::= JSON",
 /*  76 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  77 */ "type_name ::= MEDIUMBLOB",
 /*  78 */ "type_name ::= BLOB",
 /*  79 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  80 */ "type_name ::= DECIMAL",
 /*  81 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  82 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  83 */ "tags_def_opt ::=",
 /*  84 */ "tags_def_opt ::= tags_def",
 /*  85 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  86 */ "table_options ::=",
 /*  87 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  88 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  89 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  90 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  91 */ "col_name_list ::= col_name",
 /*  92 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  93 */ "col_name ::= column_name",
 /*  94 */ "cmd ::= SHOW VGROUPS",
 /*  95 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /*  96 */ "cmd ::= SHOW MNODES",
 /*  97 */ "cmd ::= query_expression",
 /*  98 */ "literal ::= NK_INTEGER",
 /*  99 */ "literal ::= NK_FLOAT",
 /* 100 */ "literal ::= NK_STRING",
 /* 101 */ "literal ::= NK_BOOL",
 /* 102 */ "literal ::= TIMESTAMP NK_STRING",
 /* 103 */ "literal ::= duration_literal",
 /* 104 */ "duration_literal ::= NK_VARIABLE",
 /* 105 */ "literal_list ::= literal",
 /* 106 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 107 */ "db_name ::= NK_ID",
 /* 108 */ "table_name ::= NK_ID",
 /* 109 */ "column_name ::= NK_ID",
 /* 110 */ "function_name ::= NK_ID",
 /* 111 */ "table_alias ::= NK_ID",
 /* 112 */ "column_alias ::= NK_ID",
 /* 113 */ "user_name ::= NK_ID",
 /* 114 */ "expression ::= literal",
 /* 115 */ "expression ::= column_reference",
 /* 116 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 117 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 118 */ "expression ::= subquery",
 /* 119 */ "expression ::= NK_LP expression NK_RP",
 /* 120 */ "expression ::= NK_PLUS expression",
 /* 121 */ "expression ::= NK_MINUS expression",
 /* 122 */ "expression ::= expression NK_PLUS expression",
 /* 123 */ "expression ::= expression NK_MINUS expression",
 /* 124 */ "expression ::= expression NK_STAR expression",
 /* 125 */ "expression ::= expression NK_SLASH expression",
 /* 126 */ "expression ::= expression NK_REM expression",
 /* 127 */ "expression_list ::= expression",
 /* 128 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 129 */ "column_reference ::= column_name",
 /* 130 */ "column_reference ::= table_name NK_DOT column_name",
 /* 131 */ "predicate ::= expression compare_op expression",
 /* 132 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 133 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 134 */ "predicate ::= expression IS NULL",
 /* 135 */ "predicate ::= expression IS NOT NULL",
 /* 136 */ "predicate ::= expression in_op in_predicate_value",
 /* 137 */ "compare_op ::= NK_LT",
 /* 138 */ "compare_op ::= NK_GT",
 /* 139 */ "compare_op ::= NK_LE",
 /* 140 */ "compare_op ::= NK_GE",
 /* 141 */ "compare_op ::= NK_NE",
 /* 142 */ "compare_op ::= NK_EQ",
 /* 143 */ "compare_op ::= LIKE",
 /* 144 */ "compare_op ::= NOT LIKE",
 /* 145 */ "compare_op ::= MATCH",
 /* 146 */ "compare_op ::= NMATCH",
 /* 147 */ "in_op ::= IN",
 /* 148 */ "in_op ::= NOT IN",
 /* 149 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 150 */ "boolean_value_expression ::= boolean_primary",
 /* 151 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 152 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 153 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 154 */ "boolean_primary ::= predicate",
 /* 155 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 156 */ "common_expression ::= expression",
 /* 157 */ "common_expression ::= boolean_value_expression",
 /* 158 */ "from_clause ::= FROM table_reference_list",
 /* 159 */ "table_reference_list ::= table_reference",
 /* 160 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 161 */ "table_reference ::= table_primary",
 /* 162 */ "table_reference ::= joined_table",
 /* 163 */ "table_primary ::= table_name alias_opt",
 /* 164 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 165 */ "table_primary ::= subquery alias_opt",
 /* 166 */ "table_primary ::= parenthesized_joined_table",
 /* 167 */ "alias_opt ::=",
 /* 168 */ "alias_opt ::= table_alias",
 /* 169 */ "alias_opt ::= AS table_alias",
 /* 170 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 171 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 172 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 173 */ "join_type ::=",
 /* 174 */ "join_type ::= INNER",
 /* 175 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 176 */ "set_quantifier_opt ::=",
 /* 177 */ "set_quantifier_opt ::= DISTINCT",
 /* 178 */ "set_quantifier_opt ::= ALL",
 /* 179 */ "select_list ::= NK_STAR",
 /* 180 */ "select_list ::= select_sublist",
 /* 181 */ "select_sublist ::= select_item",
 /* 182 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 183 */ "select_item ::= common_expression",
 /* 184 */ "select_item ::= common_expression column_alias",
 /* 185 */ "select_item ::= common_expression AS column_alias",
 /* 186 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 187 */ "where_clause_opt ::=",
 /* 188 */ "where_clause_opt ::= WHERE search_condition",
 /* 189 */ "partition_by_clause_opt ::=",
 /* 190 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 191 */ "twindow_clause_opt ::=",
 /* 192 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 193 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 194 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 195 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 196 */ "sliding_opt ::=",
 /* 197 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 198 */ "fill_opt ::=",
 /* 199 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 200 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 201 */ "fill_mode ::= NONE",
 /* 202 */ "fill_mode ::= PREV",
 /* 203 */ "fill_mode ::= NULL",
 /* 204 */ "fill_mode ::= LINEAR",
 /* 205 */ "fill_mode ::= NEXT",
 /* 206 */ "group_by_clause_opt ::=",
 /* 207 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 208 */ "group_by_list ::= expression",
 /* 209 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 210 */ "having_clause_opt ::=",
 /* 211 */ "having_clause_opt ::= HAVING search_condition",
 /* 212 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 213 */ "query_expression_body ::= query_primary",
 /* 214 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 215 */ "query_primary ::= query_specification",
 /* 216 */ "order_by_clause_opt ::=",
 /* 217 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 218 */ "slimit_clause_opt ::=",
 /* 219 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 220 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 221 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 222 */ "limit_clause_opt ::=",
 /* 223 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 224 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 225 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 226 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 227 */ "search_condition ::= common_expression",
 /* 228 */ "sort_specification_list ::= sort_specification",
 /* 229 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 230 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 231 */ "ordering_specification_opt ::=",
 /* 232 */ "ordering_specification_opt ::= ASC",
 /* 233 */ "ordering_specification_opt ::= DESC",
 /* 234 */ "null_ordering_opt ::=",
 /* 235 */ "null_ordering_opt ::= NULLS FIRST",
 /* 236 */ "null_ordering_opt ::= NULLS LAST",
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
    case 135: /* cmd */
    case 143: /* full_table_name */
    case 150: /* create_subtable_clause */
    case 153: /* drop_table_clause */
    case 156: /* column_def */
    case 159: /* col_name */
    case 160: /* query_expression */
    case 161: /* literal */
    case 162: /* duration_literal */
    case 166: /* expression */
    case 167: /* column_reference */
    case 169: /* subquery */
    case 170: /* predicate */
    case 173: /* in_predicate_value */
    case 174: /* boolean_value_expression */
    case 175: /* boolean_primary */
    case 176: /* common_expression */
    case 177: /* from_clause */
    case 178: /* table_reference_list */
    case 179: /* table_reference */
    case 180: /* table_primary */
    case 181: /* joined_table */
    case 183: /* parenthesized_joined_table */
    case 185: /* search_condition */
    case 186: /* query_specification */
    case 189: /* where_clause_opt */
    case 191: /* twindow_clause_opt */
    case 193: /* having_clause_opt */
    case 195: /* select_item */
    case 196: /* sliding_opt */
    case 197: /* fill_opt */
    case 200: /* query_expression_body */
    case 202: /* slimit_clause_opt */
    case 203: /* limit_clause_opt */
    case 204: /* query_primary */
    case 206: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy256)); 
}
      break;
    case 136: /* user_name */
    case 137: /* dnode_endpoint */
    case 138: /* dnode_host_name */
    case 140: /* db_name */
    case 155: /* table_name */
    case 157: /* column_name */
    case 163: /* function_name */
    case 164: /* table_alias */
    case 165: /* column_alias */
    case 182: /* alias_opt */
{
 
}
      break;
    case 139: /* not_exists_opt */
    case 142: /* exists_opt */
    case 187: /* set_quantifier_opt */
{
 
}
      break;
    case 141: /* db_options */
{
 tfree((yypminor->yy391)); 
}
      break;
    case 144: /* column_def_list */
    case 145: /* tags_def_opt */
    case 147: /* multi_create_clause */
    case 148: /* tags_def */
    case 149: /* multi_drop_clause */
    case 151: /* specific_tags_opt */
    case 152: /* literal_list */
    case 154: /* col_name_list */
    case 168: /* expression_list */
    case 188: /* select_list */
    case 190: /* partition_by_clause_opt */
    case 192: /* group_by_clause_opt */
    case 194: /* select_sublist */
    case 199: /* group_by_list */
    case 201: /* order_by_clause_opt */
    case 205: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy46)); 
}
      break;
    case 146: /* table_options */
{
 tfree((yypminor->yy340)); 
}
      break;
    case 158: /* type_name */
{
 
}
      break;
    case 171: /* compare_op */
    case 172: /* in_op */
{
 
}
      break;
    case 184: /* join_type */
{
 
}
      break;
    case 198: /* fill_mode */
{
 
}
      break;
    case 207: /* ordering_specification_opt */
{
 
}
      break;
    case 208: /* null_ordering_opt */
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
  {  135,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  135,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  135,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  135,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  135,   -2 }, /* (4) cmd ::= SHOW USERS */
  {  135,   -3 }, /* (5) cmd ::= CREATE DNODE dnode_endpoint */
  {  135,   -5 }, /* (6) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  135,   -3 }, /* (7) cmd ::= DROP DNODE NK_INTEGER */
  {  135,   -3 }, /* (8) cmd ::= DROP DNODE dnode_endpoint */
  {  135,   -2 }, /* (9) cmd ::= SHOW DNODES */
  {  137,   -1 }, /* (10) dnode_endpoint ::= NK_STRING */
  {  138,   -1 }, /* (11) dnode_host_name ::= NK_ID */
  {  138,   -1 }, /* (12) dnode_host_name ::= NK_IPTOKEN */
  {  135,   -5 }, /* (13) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  135,   -4 }, /* (14) cmd ::= DROP DATABASE exists_opt db_name */
  {  135,   -2 }, /* (15) cmd ::= SHOW DATABASES */
  {  135,   -2 }, /* (16) cmd ::= USE db_name */
  {  139,   -3 }, /* (17) not_exists_opt ::= IF NOT EXISTS */
  {  139,    0 }, /* (18) not_exists_opt ::= */
  {  142,   -2 }, /* (19) exists_opt ::= IF EXISTS */
  {  142,    0 }, /* (20) exists_opt ::= */
  {  141,    0 }, /* (21) db_options ::= */
  {  141,   -3 }, /* (22) db_options ::= db_options BLOCKS NK_INTEGER */
  {  141,   -3 }, /* (23) db_options ::= db_options CACHE NK_INTEGER */
  {  141,   -3 }, /* (24) db_options ::= db_options CACHELAST NK_INTEGER */
  {  141,   -3 }, /* (25) db_options ::= db_options COMP NK_INTEGER */
  {  141,   -3 }, /* (26) db_options ::= db_options DAYS NK_INTEGER */
  {  141,   -3 }, /* (27) db_options ::= db_options FSYNC NK_INTEGER */
  {  141,   -3 }, /* (28) db_options ::= db_options MAXROWS NK_INTEGER */
  {  141,   -3 }, /* (29) db_options ::= db_options MINROWS NK_INTEGER */
  {  141,   -3 }, /* (30) db_options ::= db_options KEEP NK_INTEGER */
  {  141,   -3 }, /* (31) db_options ::= db_options PRECISION NK_STRING */
  {  141,   -3 }, /* (32) db_options ::= db_options QUORUM NK_INTEGER */
  {  141,   -3 }, /* (33) db_options ::= db_options REPLICA NK_INTEGER */
  {  141,   -3 }, /* (34) db_options ::= db_options TTL NK_INTEGER */
  {  141,   -3 }, /* (35) db_options ::= db_options WAL NK_INTEGER */
  {  141,   -3 }, /* (36) db_options ::= db_options VGROUPS NK_INTEGER */
  {  141,   -3 }, /* (37) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  141,   -3 }, /* (38) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  135,   -9 }, /* (39) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  135,   -3 }, /* (40) cmd ::= CREATE TABLE multi_create_clause */
  {  135,   -9 }, /* (41) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  135,   -3 }, /* (42) cmd ::= DROP TABLE multi_drop_clause */
  {  135,   -4 }, /* (43) cmd ::= DROP STABLE exists_opt full_table_name */
  {  135,   -2 }, /* (44) cmd ::= SHOW TABLES */
  {  135,   -2 }, /* (45) cmd ::= SHOW STABLES */
  {  147,   -1 }, /* (46) multi_create_clause ::= create_subtable_clause */
  {  147,   -2 }, /* (47) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  150,   -9 }, /* (48) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  149,   -1 }, /* (49) multi_drop_clause ::= drop_table_clause */
  {  149,   -2 }, /* (50) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  153,   -2 }, /* (51) drop_table_clause ::= exists_opt full_table_name */
  {  151,    0 }, /* (52) specific_tags_opt ::= */
  {  151,   -3 }, /* (53) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  143,   -1 }, /* (54) full_table_name ::= table_name */
  {  143,   -3 }, /* (55) full_table_name ::= db_name NK_DOT table_name */
  {  144,   -1 }, /* (56) column_def_list ::= column_def */
  {  144,   -3 }, /* (57) column_def_list ::= column_def_list NK_COMMA column_def */
  {  156,   -2 }, /* (58) column_def ::= column_name type_name */
  {  156,   -4 }, /* (59) column_def ::= column_name type_name COMMENT NK_STRING */
  {  158,   -1 }, /* (60) type_name ::= BOOL */
  {  158,   -1 }, /* (61) type_name ::= TINYINT */
  {  158,   -1 }, /* (62) type_name ::= SMALLINT */
  {  158,   -1 }, /* (63) type_name ::= INT */
  {  158,   -1 }, /* (64) type_name ::= INTEGER */
  {  158,   -1 }, /* (65) type_name ::= BIGINT */
  {  158,   -1 }, /* (66) type_name ::= FLOAT */
  {  158,   -1 }, /* (67) type_name ::= DOUBLE */
  {  158,   -4 }, /* (68) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  158,   -1 }, /* (69) type_name ::= TIMESTAMP */
  {  158,   -4 }, /* (70) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  158,   -2 }, /* (71) type_name ::= TINYINT UNSIGNED */
  {  158,   -2 }, /* (72) type_name ::= SMALLINT UNSIGNED */
  {  158,   -2 }, /* (73) type_name ::= INT UNSIGNED */
  {  158,   -2 }, /* (74) type_name ::= BIGINT UNSIGNED */
  {  158,   -1 }, /* (75) type_name ::= JSON */
  {  158,   -4 }, /* (76) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  158,   -1 }, /* (77) type_name ::= MEDIUMBLOB */
  {  158,   -1 }, /* (78) type_name ::= BLOB */
  {  158,   -4 }, /* (79) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  158,   -1 }, /* (80) type_name ::= DECIMAL */
  {  158,   -4 }, /* (81) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  158,   -6 }, /* (82) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  145,    0 }, /* (83) tags_def_opt ::= */
  {  145,   -1 }, /* (84) tags_def_opt ::= tags_def */
  {  148,   -4 }, /* (85) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  146,    0 }, /* (86) table_options ::= */
  {  146,   -3 }, /* (87) table_options ::= table_options COMMENT NK_STRING */
  {  146,   -3 }, /* (88) table_options ::= table_options KEEP NK_INTEGER */
  {  146,   -3 }, /* (89) table_options ::= table_options TTL NK_INTEGER */
  {  146,   -5 }, /* (90) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  154,   -1 }, /* (91) col_name_list ::= col_name */
  {  154,   -3 }, /* (92) col_name_list ::= col_name_list NK_COMMA col_name */
  {  159,   -1 }, /* (93) col_name ::= column_name */
  {  135,   -2 }, /* (94) cmd ::= SHOW VGROUPS */
  {  135,   -4 }, /* (95) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  135,   -2 }, /* (96) cmd ::= SHOW MNODES */
  {  135,   -1 }, /* (97) cmd ::= query_expression */
  {  161,   -1 }, /* (98) literal ::= NK_INTEGER */
  {  161,   -1 }, /* (99) literal ::= NK_FLOAT */
  {  161,   -1 }, /* (100) literal ::= NK_STRING */
  {  161,   -1 }, /* (101) literal ::= NK_BOOL */
  {  161,   -2 }, /* (102) literal ::= TIMESTAMP NK_STRING */
  {  161,   -1 }, /* (103) literal ::= duration_literal */
  {  162,   -1 }, /* (104) duration_literal ::= NK_VARIABLE */
  {  152,   -1 }, /* (105) literal_list ::= literal */
  {  152,   -3 }, /* (106) literal_list ::= literal_list NK_COMMA literal */
  {  140,   -1 }, /* (107) db_name ::= NK_ID */
  {  155,   -1 }, /* (108) table_name ::= NK_ID */
  {  157,   -1 }, /* (109) column_name ::= NK_ID */
  {  163,   -1 }, /* (110) function_name ::= NK_ID */
  {  164,   -1 }, /* (111) table_alias ::= NK_ID */
  {  165,   -1 }, /* (112) column_alias ::= NK_ID */
  {  136,   -1 }, /* (113) user_name ::= NK_ID */
  {  166,   -1 }, /* (114) expression ::= literal */
  {  166,   -1 }, /* (115) expression ::= column_reference */
  {  166,   -4 }, /* (116) expression ::= function_name NK_LP expression_list NK_RP */
  {  166,   -4 }, /* (117) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  166,   -1 }, /* (118) expression ::= subquery */
  {  166,   -3 }, /* (119) expression ::= NK_LP expression NK_RP */
  {  166,   -2 }, /* (120) expression ::= NK_PLUS expression */
  {  166,   -2 }, /* (121) expression ::= NK_MINUS expression */
  {  166,   -3 }, /* (122) expression ::= expression NK_PLUS expression */
  {  166,   -3 }, /* (123) expression ::= expression NK_MINUS expression */
  {  166,   -3 }, /* (124) expression ::= expression NK_STAR expression */
  {  166,   -3 }, /* (125) expression ::= expression NK_SLASH expression */
  {  166,   -3 }, /* (126) expression ::= expression NK_REM expression */
  {  168,   -1 }, /* (127) expression_list ::= expression */
  {  168,   -3 }, /* (128) expression_list ::= expression_list NK_COMMA expression */
  {  167,   -1 }, /* (129) column_reference ::= column_name */
  {  167,   -3 }, /* (130) column_reference ::= table_name NK_DOT column_name */
  {  170,   -3 }, /* (131) predicate ::= expression compare_op expression */
  {  170,   -5 }, /* (132) predicate ::= expression BETWEEN expression AND expression */
  {  170,   -6 }, /* (133) predicate ::= expression NOT BETWEEN expression AND expression */
  {  170,   -3 }, /* (134) predicate ::= expression IS NULL */
  {  170,   -4 }, /* (135) predicate ::= expression IS NOT NULL */
  {  170,   -3 }, /* (136) predicate ::= expression in_op in_predicate_value */
  {  171,   -1 }, /* (137) compare_op ::= NK_LT */
  {  171,   -1 }, /* (138) compare_op ::= NK_GT */
  {  171,   -1 }, /* (139) compare_op ::= NK_LE */
  {  171,   -1 }, /* (140) compare_op ::= NK_GE */
  {  171,   -1 }, /* (141) compare_op ::= NK_NE */
  {  171,   -1 }, /* (142) compare_op ::= NK_EQ */
  {  171,   -1 }, /* (143) compare_op ::= LIKE */
  {  171,   -2 }, /* (144) compare_op ::= NOT LIKE */
  {  171,   -1 }, /* (145) compare_op ::= MATCH */
  {  171,   -1 }, /* (146) compare_op ::= NMATCH */
  {  172,   -1 }, /* (147) in_op ::= IN */
  {  172,   -2 }, /* (148) in_op ::= NOT IN */
  {  173,   -3 }, /* (149) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  174,   -1 }, /* (150) boolean_value_expression ::= boolean_primary */
  {  174,   -2 }, /* (151) boolean_value_expression ::= NOT boolean_primary */
  {  174,   -3 }, /* (152) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  174,   -3 }, /* (153) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  175,   -1 }, /* (154) boolean_primary ::= predicate */
  {  175,   -3 }, /* (155) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  176,   -1 }, /* (156) common_expression ::= expression */
  {  176,   -1 }, /* (157) common_expression ::= boolean_value_expression */
  {  177,   -2 }, /* (158) from_clause ::= FROM table_reference_list */
  {  178,   -1 }, /* (159) table_reference_list ::= table_reference */
  {  178,   -3 }, /* (160) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  179,   -1 }, /* (161) table_reference ::= table_primary */
  {  179,   -1 }, /* (162) table_reference ::= joined_table */
  {  180,   -2 }, /* (163) table_primary ::= table_name alias_opt */
  {  180,   -4 }, /* (164) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  180,   -2 }, /* (165) table_primary ::= subquery alias_opt */
  {  180,   -1 }, /* (166) table_primary ::= parenthesized_joined_table */
  {  182,    0 }, /* (167) alias_opt ::= */
  {  182,   -1 }, /* (168) alias_opt ::= table_alias */
  {  182,   -2 }, /* (169) alias_opt ::= AS table_alias */
  {  183,   -3 }, /* (170) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  183,   -3 }, /* (171) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  181,   -6 }, /* (172) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  184,    0 }, /* (173) join_type ::= */
  {  184,   -1 }, /* (174) join_type ::= INNER */
  {  186,   -9 }, /* (175) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  187,    0 }, /* (176) set_quantifier_opt ::= */
  {  187,   -1 }, /* (177) set_quantifier_opt ::= DISTINCT */
  {  187,   -1 }, /* (178) set_quantifier_opt ::= ALL */
  {  188,   -1 }, /* (179) select_list ::= NK_STAR */
  {  188,   -1 }, /* (180) select_list ::= select_sublist */
  {  194,   -1 }, /* (181) select_sublist ::= select_item */
  {  194,   -3 }, /* (182) select_sublist ::= select_sublist NK_COMMA select_item */
  {  195,   -1 }, /* (183) select_item ::= common_expression */
  {  195,   -2 }, /* (184) select_item ::= common_expression column_alias */
  {  195,   -3 }, /* (185) select_item ::= common_expression AS column_alias */
  {  195,   -3 }, /* (186) select_item ::= table_name NK_DOT NK_STAR */
  {  189,    0 }, /* (187) where_clause_opt ::= */
  {  189,   -2 }, /* (188) where_clause_opt ::= WHERE search_condition */
  {  190,    0 }, /* (189) partition_by_clause_opt ::= */
  {  190,   -3 }, /* (190) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  191,    0 }, /* (191) twindow_clause_opt ::= */
  {  191,   -6 }, /* (192) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  191,   -4 }, /* (193) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  191,   -6 }, /* (194) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  191,   -8 }, /* (195) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  196,    0 }, /* (196) sliding_opt ::= */
  {  196,   -4 }, /* (197) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  197,    0 }, /* (198) fill_opt ::= */
  {  197,   -4 }, /* (199) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  197,   -6 }, /* (200) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  198,   -1 }, /* (201) fill_mode ::= NONE */
  {  198,   -1 }, /* (202) fill_mode ::= PREV */
  {  198,   -1 }, /* (203) fill_mode ::= NULL */
  {  198,   -1 }, /* (204) fill_mode ::= LINEAR */
  {  198,   -1 }, /* (205) fill_mode ::= NEXT */
  {  192,    0 }, /* (206) group_by_clause_opt ::= */
  {  192,   -3 }, /* (207) group_by_clause_opt ::= GROUP BY group_by_list */
  {  199,   -1 }, /* (208) group_by_list ::= expression */
  {  199,   -3 }, /* (209) group_by_list ::= group_by_list NK_COMMA expression */
  {  193,    0 }, /* (210) having_clause_opt ::= */
  {  193,   -2 }, /* (211) having_clause_opt ::= HAVING search_condition */
  {  160,   -4 }, /* (212) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  200,   -1 }, /* (213) query_expression_body ::= query_primary */
  {  200,   -4 }, /* (214) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  204,   -1 }, /* (215) query_primary ::= query_specification */
  {  201,    0 }, /* (216) order_by_clause_opt ::= */
  {  201,   -3 }, /* (217) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  202,    0 }, /* (218) slimit_clause_opt ::= */
  {  202,   -2 }, /* (219) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  202,   -4 }, /* (220) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  202,   -4 }, /* (221) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  203,    0 }, /* (222) limit_clause_opt ::= */
  {  203,   -2 }, /* (223) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  203,   -4 }, /* (224) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  203,   -4 }, /* (225) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  169,   -3 }, /* (226) subquery ::= NK_LP query_expression NK_RP */
  {  185,   -1 }, /* (227) search_condition ::= common_expression */
  {  205,   -1 }, /* (228) sort_specification_list ::= sort_specification */
  {  205,   -3 }, /* (229) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  206,   -3 }, /* (230) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  207,    0 }, /* (231) ordering_specification_opt ::= */
  {  207,   -1 }, /* (232) ordering_specification_opt ::= ASC */
  {  207,   -1 }, /* (233) ordering_specification_opt ::= DESC */
  {  208,    0 }, /* (234) null_ordering_opt ::= */
  {  208,   -2 }, /* (235) null_ordering_opt ::= NULLS FIRST */
  {  208,   -2 }, /* (236) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0);}
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy129, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0);}
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy129, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0);}
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy129); }
        break;
      case 4: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy129, NULL);}
        break;
      case 6: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0);}
        break;
      case 7: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0);}
        break;
      case 8: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy129);}
        break;
      case 9: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 107: /* db_name ::= NK_ID */ yytestcase(yyruleno==107);
      case 108: /* table_name ::= NK_ID */ yytestcase(yyruleno==108);
      case 109: /* column_name ::= NK_ID */ yytestcase(yyruleno==109);
      case 110: /* function_name ::= NK_ID */ yytestcase(yyruleno==110);
      case 111: /* table_alias ::= NK_ID */ yytestcase(yyruleno==111);
      case 112: /* column_alias ::= NK_ID */ yytestcase(yyruleno==112);
      case 113: /* user_name ::= NK_ID */ yytestcase(yyruleno==113);
{ yylhsminor.yy129 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 13: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy185, &yymsp[-1].minor.yy129, yymsp[0].minor.yy391);}
        break;
      case 14: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy185, &yymsp[0].minor.yy129); }
        break;
      case 15: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 16: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy129);}
        break;
      case 17: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy185 = true; }
        break;
      case 18: /* not_exists_opt ::= */
      case 20: /* exists_opt ::= */ yytestcase(yyruleno==20);
      case 176: /* set_quantifier_opt ::= */ yytestcase(yyruleno==176);
{ yymsp[1].minor.yy185 = false; }
        break;
      case 19: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy185 = true; }
        break;
      case 21: /* db_options ::= */
{ yymsp[1].minor.yy391 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 22: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 23: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 24: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 25: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 26: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 27: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 28: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 29: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 30: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 31: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 32: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 33: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 34: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 35: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 36: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 37: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 38: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy391 = setDatabaseOption(pCxt, yymsp[-2].minor.yy391, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy391 = yylhsminor.yy391;
        break;
      case 39: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 41: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==41);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy185, yymsp[-5].minor.yy256, yymsp[-3].minor.yy46, yymsp[-1].minor.yy46, yymsp[0].minor.yy340);}
        break;
      case 40: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy46);}
        break;
      case 42: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy46); }
        break;
      case 43: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy185, yymsp[0].minor.yy256); }
        break;
      case 44: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 45: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 46: /* multi_create_clause ::= create_subtable_clause */
      case 49: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==49);
      case 56: /* column_def_list ::= column_def */ yytestcase(yyruleno==56);
      case 91: /* col_name_list ::= col_name */ yytestcase(yyruleno==91);
      case 181: /* select_sublist ::= select_item */ yytestcase(yyruleno==181);
      case 228: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==228);
{ yylhsminor.yy46 = createNodeList(pCxt, yymsp[0].minor.yy256); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 47: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 50: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==50);
{ yylhsminor.yy46 = addNodeToList(pCxt, yymsp[-1].minor.yy46, yymsp[0].minor.yy256); }
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 48: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy256 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy185, yymsp[-7].minor.yy256, yymsp[-5].minor.yy256, yymsp[-4].minor.yy46, yymsp[-1].minor.yy46); }
  yymsp[-8].minor.yy256 = yylhsminor.yy256;
        break;
      case 51: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy256 = createDropTableClause(pCxt, yymsp[-1].minor.yy185, yymsp[0].minor.yy256); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 52: /* specific_tags_opt ::= */
      case 83: /* tags_def_opt ::= */ yytestcase(yyruleno==83);
      case 189: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==189);
      case 206: /* group_by_clause_opt ::= */ yytestcase(yyruleno==206);
      case 216: /* order_by_clause_opt ::= */ yytestcase(yyruleno==216);
{ yymsp[1].minor.yy46 = NULL; }
        break;
      case 53: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy46 = yymsp[-1].minor.yy46; }
        break;
      case 54: /* full_table_name ::= table_name */
{ yylhsminor.yy256 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy129, NULL); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 55: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy256 = createRealTableNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129, NULL); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 57: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 92: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==92);
      case 182: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==182);
      case 229: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==229);
{ yylhsminor.yy46 = addNodeToList(pCxt, yymsp[-2].minor.yy46, yymsp[0].minor.yy256); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 58: /* column_def ::= column_name type_name */
{ yylhsminor.yy256 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy129, yymsp[0].minor.yy70, NULL); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 59: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy256 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-2].minor.yy70, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 60: /* type_name ::= BOOL */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 61: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 62: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 63: /* type_name ::= INT */
      case 64: /* type_name ::= INTEGER */ yytestcase(yyruleno==64);
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 65: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 66: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 67: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 68: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy70 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 69: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 70: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy70 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 71: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy70 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 72: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy70 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 73: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy70 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 74: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy70 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 75: /* type_name ::= JSON */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 76: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy70 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 77: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 78: /* type_name ::= BLOB */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 79: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy70 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 80: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy70 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 81: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy70 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 82: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy70 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 84: /* tags_def_opt ::= tags_def */
      case 180: /* select_list ::= select_sublist */ yytestcase(yyruleno==180);
{ yylhsminor.yy46 = yymsp[0].minor.yy46; }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 85: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy46 = yymsp[-1].minor.yy46; }
        break;
      case 86: /* table_options ::= */
{ yymsp[1].minor.yy340 = createDefaultTableOptions(pCxt);}
        break;
      case 87: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy340 = setTableOption(pCxt, yymsp[-2].minor.yy340, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy340 = yylhsminor.yy340;
        break;
      case 88: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy340 = setTableOption(pCxt, yymsp[-2].minor.yy340, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy340 = yylhsminor.yy340;
        break;
      case 89: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy340 = setTableOption(pCxt, yymsp[-2].minor.yy340, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy340 = yylhsminor.yy340;
        break;
      case 90: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy340 = setTableSmaOption(pCxt, yymsp[-4].minor.yy340, yymsp[-1].minor.yy46); }
  yymsp[-4].minor.yy340 = yylhsminor.yy340;
        break;
      case 93: /* col_name ::= column_name */
{ yylhsminor.yy256 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy129); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 94: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 95: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy129); }
        break;
      case 96: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 97: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy256; }
        break;
      case 98: /* literal ::= NK_INTEGER */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 99: /* literal ::= NK_FLOAT */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 100: /* literal ::= NK_STRING */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 101: /* literal ::= NK_BOOL */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 102: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 103: /* literal ::= duration_literal */
      case 114: /* expression ::= literal */ yytestcase(yyruleno==114);
      case 115: /* expression ::= column_reference */ yytestcase(yyruleno==115);
      case 118: /* expression ::= subquery */ yytestcase(yyruleno==118);
      case 150: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==150);
      case 154: /* boolean_primary ::= predicate */ yytestcase(yyruleno==154);
      case 156: /* common_expression ::= expression */ yytestcase(yyruleno==156);
      case 157: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==157);
      case 159: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==159);
      case 161: /* table_reference ::= table_primary */ yytestcase(yyruleno==161);
      case 162: /* table_reference ::= joined_table */ yytestcase(yyruleno==162);
      case 166: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==166);
      case 213: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==213);
      case 215: /* query_primary ::= query_specification */ yytestcase(yyruleno==215);
{ yylhsminor.yy256 = yymsp[0].minor.yy256; }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 104: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 105: /* literal_list ::= literal */
      case 127: /* expression_list ::= expression */ yytestcase(yyruleno==127);
{ yylhsminor.yy46 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy256)); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 106: /* literal_list ::= literal_list NK_COMMA literal */
      case 128: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==128);
{ yylhsminor.yy46 = addNodeToList(pCxt, yymsp[-2].minor.yy46, releaseRawExprNode(pCxt, yymsp[0].minor.yy256)); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 116: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-1].minor.yy46)); }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 117: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 119: /* expression ::= NK_LP expression NK_RP */
      case 155: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==155);
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy256)); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 120: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy256));
                                                                                  }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 121: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy256), NULL));
                                                                                  }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 122: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); 
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 123: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); 
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 124: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); 
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 125: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); 
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 126: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); 
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 129: /* column_reference ::= column_name */
{ yylhsminor.yy256 = createRawExprNode(pCxt, &yymsp[0].minor.yy129, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy129)); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 130: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129, createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129)); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 131: /* predicate ::= expression compare_op expression */
      case 136: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==136);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy326, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256)));
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 132: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy256), releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256)));
                                                                                  }
  yymsp[-4].minor.yy256 = yylhsminor.yy256;
        break;
      case 133: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[-5].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256)));
                                                                                  }
  yymsp[-5].minor.yy256 = yylhsminor.yy256;
        break;
      case 134: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), NULL));
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 135: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy256), NULL));
                                                                                  }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 137: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy326 = OP_TYPE_LOWER_THAN; }
        break;
      case 138: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy326 = OP_TYPE_GREATER_THAN; }
        break;
      case 139: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy326 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 140: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy326 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 141: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy326 = OP_TYPE_NOT_EQUAL; }
        break;
      case 142: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy326 = OP_TYPE_EQUAL; }
        break;
      case 143: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy326 = OP_TYPE_LIKE; }
        break;
      case 144: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy326 = OP_TYPE_NOT_LIKE; }
        break;
      case 145: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy326 = OP_TYPE_MATCH; }
        break;
      case 146: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy326 = OP_TYPE_NMATCH; }
        break;
      case 147: /* in_op ::= IN */
{ yymsp[0].minor.yy326 = OP_TYPE_IN; }
        break;
      case 148: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy326 = OP_TYPE_NOT_IN; }
        break;
      case 149: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy46)); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 151: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy256), NULL));
                                                                                  }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 152: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256)));
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 153: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy256);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), releaseRawExprNode(pCxt, yymsp[0].minor.yy256)));
                                                                                  }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 158: /* from_clause ::= FROM table_reference_list */
      case 188: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==188);
      case 211: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==211);
{ yymsp[-1].minor.yy256 = yymsp[0].minor.yy256; }
        break;
      case 160: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy256 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy256, yymsp[0].minor.yy256, NULL); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 163: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy256 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 164: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy256 = createRealTableNode(pCxt, &yymsp[-3].minor.yy129, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 165: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy256 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy256), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 167: /* alias_opt ::= */
{ yymsp[1].minor.yy129 = nil_token;  }
        break;
      case 168: /* alias_opt ::= table_alias */
{ yylhsminor.yy129 = yymsp[0].minor.yy129; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 169: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy129 = yymsp[0].minor.yy129; }
        break;
      case 170: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 171: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==171);
{ yymsp[-2].minor.yy256 = yymsp[-1].minor.yy256; }
        break;
      case 172: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy256 = createJoinTableNode(pCxt, yymsp[-4].minor.yy266, yymsp[-5].minor.yy256, yymsp[-2].minor.yy256, yymsp[0].minor.yy256); }
  yymsp[-5].minor.yy256 = yylhsminor.yy256;
        break;
      case 173: /* join_type ::= */
{ yymsp[1].minor.yy266 = JOIN_TYPE_INNER; }
        break;
      case 174: /* join_type ::= INNER */
{ yymsp[0].minor.yy266 = JOIN_TYPE_INNER; }
        break;
      case 175: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy256 = createSelectStmt(pCxt, yymsp[-7].minor.yy185, yymsp[-6].minor.yy46, yymsp[-5].minor.yy256);
                                                                                    yymsp[-8].minor.yy256 = addWhereClause(pCxt, yymsp[-8].minor.yy256, yymsp[-4].minor.yy256);
                                                                                    yymsp[-8].minor.yy256 = addPartitionByClause(pCxt, yymsp[-8].minor.yy256, yymsp[-3].minor.yy46);
                                                                                    yymsp[-8].minor.yy256 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy256, yymsp[-2].minor.yy256);
                                                                                    yymsp[-8].minor.yy256 = addGroupByClause(pCxt, yymsp[-8].minor.yy256, yymsp[-1].minor.yy46);
                                                                                    yymsp[-8].minor.yy256 = addHavingClause(pCxt, yymsp[-8].minor.yy256, yymsp[0].minor.yy256);
                                                                                  }
        break;
      case 177: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy185 = true; }
        break;
      case 178: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy185 = false; }
        break;
      case 179: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy46 = NULL; }
        break;
      case 183: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy256);
                                                                                    yylhsminor.yy256 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy256), &t);
                                                                                  }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 184: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy256 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy256), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 185: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy256 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), &yymsp[0].minor.yy129); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 186: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy256 = createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 187: /* where_clause_opt ::= */
      case 191: /* twindow_clause_opt ::= */ yytestcase(yyruleno==191);
      case 196: /* sliding_opt ::= */ yytestcase(yyruleno==196);
      case 198: /* fill_opt ::= */ yytestcase(yyruleno==198);
      case 210: /* having_clause_opt ::= */ yytestcase(yyruleno==210);
      case 218: /* slimit_clause_opt ::= */ yytestcase(yyruleno==218);
      case 222: /* limit_clause_opt ::= */ yytestcase(yyruleno==222);
{ yymsp[1].minor.yy256 = NULL; }
        break;
      case 190: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 207: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==207);
      case 217: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==217);
{ yymsp[-2].minor.yy46 = yymsp[0].minor.yy46; }
        break;
      case 192: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy256 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy256), &yymsp[-1].minor.yy0); }
        break;
      case 193: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy256 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy256)); }
        break;
      case 194: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy256 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy256, NULL, yymsp[-1].minor.yy256, yymsp[0].minor.yy256); }
        break;
      case 195: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy256 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy256, yymsp[-3].minor.yy256, yymsp[-1].minor.yy256, yymsp[0].minor.yy256); }
        break;
      case 197: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy256 = yymsp[-1].minor.yy256; }
        break;
      case 199: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy256 = createFillNode(pCxt, yymsp[-1].minor.yy360, NULL); }
        break;
      case 200: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy256 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy46)); }
        break;
      case 201: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy360 = FILL_MODE_NONE; }
        break;
      case 202: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy360 = FILL_MODE_PREV; }
        break;
      case 203: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy360 = FILL_MODE_NULL; }
        break;
      case 204: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy360 = FILL_MODE_LINEAR; }
        break;
      case 205: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy360 = FILL_MODE_NEXT; }
        break;
      case 208: /* group_by_list ::= expression */
{ yylhsminor.yy46 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 209: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy46 = addNodeToList(pCxt, yymsp[-2].minor.yy46, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy256))); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 212: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy256 = addOrderByClause(pCxt, yymsp[-3].minor.yy256, yymsp[-2].minor.yy46);
                                                                                    yylhsminor.yy256 = addSlimitClause(pCxt, yylhsminor.yy256, yymsp[-1].minor.yy256);
                                                                                    yylhsminor.yy256 = addLimitClause(pCxt, yylhsminor.yy256, yymsp[0].minor.yy256);
                                                                                  }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 214: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy256 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy256, yymsp[0].minor.yy256); }
  yymsp[-3].minor.yy256 = yylhsminor.yy256;
        break;
      case 219: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 223: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==223);
{ yymsp[-1].minor.yy256 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 220: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 224: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-3].minor.yy256 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 221: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 225: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==225);
{ yymsp[-3].minor.yy256 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 226: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy256 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy256); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 227: /* search_condition ::= common_expression */
{ yylhsminor.yy256 = releaseRawExprNode(pCxt, yymsp[0].minor.yy256); }
  yymsp[0].minor.yy256 = yylhsminor.yy256;
        break;
      case 230: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy256 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy256), yymsp[-1].minor.yy202, yymsp[0].minor.yy147); }
  yymsp[-2].minor.yy256 = yylhsminor.yy256;
        break;
      case 231: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy202 = ORDER_ASC; }
        break;
      case 232: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy202 = ORDER_ASC; }
        break;
      case 233: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy202 = ORDER_DESC; }
        break;
      case 234: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy147 = NULL_ORDER_DEFAULT; }
        break;
      case 235: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy147 = NULL_ORDER_FIRST; }
        break;
      case 236: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy147 = NULL_ORDER_LAST; }
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
