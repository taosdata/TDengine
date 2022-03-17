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
#define YYNOCODE 220
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SNodeList* yy24;
  bool yy97;
  SToken yy129;
  SDataType yy224;
  ENullOrder yy257;
  EOperatorType yy260;
  EFillMode yy294;
  EJoinType yy332;
  EOrder yy378;
  SNode* yy432;
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
#define YYNSTATE             333
#define YYNRULE              255
#define YYNTOKEN             141
#define YY_MAX_SHIFT         332
#define YY_MIN_SHIFTREDUCE   501
#define YY_MAX_SHIFTREDUCE   755
#define YY_ERROR_ACTION      756
#define YY_ACCEPT_ACTION     757
#define YY_NO_ACTION         758
#define YY_MIN_REDUCE        759
#define YY_MAX_REDUCE        1013
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
#define YY_ACTTAB_COUNT (1012)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   163,  856,  240,  855,  272,  162,  272,  200,  856,  814,
 /*    10 */   854,  878,   31,   29,   27,   26,   25,   65,  880,  817,
 /*    20 */   178,  817,  256,  176,  906,   31,   29,   27,   26,   25,
 /*    30 */   779,  878,   40,  705,   55,  819,   71,   63,  880,   62,
 /*    40 */   155,  229,   66,  812,  243,  809,  906,   79,  938,  939,
 /*    50 */   626,  943,  878,  155,  175,  258,  511,  863,  259,  891,
 /*    60 */   511,  638,   58,  892,  895,  931,  512,  513,  208,  165,
 /*    70 */   927,   77,  321,  320,  319,  318,  317,  316,  315,  314,
 /*    80 */   313,  312,  311,  310,  309,  308,  307,  306,  305,  775,
 /*    90 */   959,   27,   26,   25,   31,   29,   27,   26,   25,   23,
 /*   100 */   170,  271,  656,  657,  658,  659,  660,  661,  662,  664,
 /*   110 */   665,  666,   23,  170,  271,  656,  657,  658,  659,  660,
 /*   120 */   661,  662,  664,  665,  666,   31,   29,   27,   26,   25,
 /*   130 */    10,  321,  320,  319,  318,  317,  316,  315,  314,  313,
 /*   140 */   312,  311,  310,  309,  308,  307,  306,  305,  565,  295,
 /*   150 */   294,  293,  569,  292,  571,  572,  291,  574,  288,  804,
 /*   160 */   580,  285,  582,  583,  282,  279,   24,  117,  256,  196,
 /*   170 */   906,  992,  626,  195,  624,  256,  177,  906,  194,  856,
 /*   180 */   193,  854,   83,  878,  991,   83,  258,  233,  990,  244,
 /*   190 */   891,  180,  271,   57,  892,  895,  931,   54,  182,  190,
 /*   200 */   154,  927,   51,  819,   71,  272,  192,  191,  269,  301,
 /*   210 */   819,   71,  992,  300,  805,  243,  624,  906,  751,  752,
 /*   220 */   817,   10,  234,  878,  298,   82,  258,  945,  302,  990,
 /*   230 */   891,  757,  627,   58,  892,  895,  931,  256,  186,  906,
 /*   240 */   165,  927,   77,  228,  942,  878,  201,  299,  258,  628,
 /*   250 */    30,   28,  891,  201,  113,   58,  892,  895,  931,  617,
 /*   260 */   221,  958,  165,  927, 1004,  184,  256,  615,  906,  172,
 /*   270 */   629,  235,  230,  965,  878,  304,  878,  258,   12,  272,
 /*   280 */   297,  891,  270,  881,   58,  892,  895,  931,  992,    9,
 /*   290 */     8,  165,  927, 1004,  817,  256,  272,  906,    1,  130,
 /*   300 */    83,   82,  988,  878,  181,  990,  258,  856,   40,  854,
 /*   310 */   891,  817,  112,   58,  892,  895,  931,  273,   21,  813,
 /*   320 */   165,  927, 1004,   30,   28,  698,  272,  663,  189,  183,
 /*   330 */   667,  949,  617,   99,  616,  618,  621,  114,   30,   28,
 /*   340 */   615,  817,  172,  329,  328,  803,  247,  617,  256,  674,
 /*   350 */   906,   12,  722,   83,  140,  615,  878,  172,  161,  258,
 /*   360 */   141,   72,  244,  891,   30,   28,  144,  892,  895,   49,
 /*   370 */   945,    1,  945,  617,   84,  225,  720,  721,  723,  724,
 /*   380 */   810,  615,  226,  172,  304,  992,    7,  941,    6,  940,
 /*   390 */   273,  697,   12,  864,  259,  256,  248,  906,   82,  820,
 /*   400 */    71,  136,  990,  878,  847,  273,  258,  616,  618,  621,
 /*   410 */   891,  240,    1,   59,  892,  895,  931,  256,  107,  906,
 /*   420 */   930,  927,  616,  618,  621,  878,   65,  217,  258,  950,
 /*   430 */   693,  273,  891,  251,   69,   59,  892,  895,  931,  207,
 /*   440 */   244,   83,  254,  927,  213,  961,   63,  211,  616,  618,
 /*   450 */   621,   83,   94,   30,   28,  257,  110,  938,  239,  241,
 /*   460 */   238,  249,  617,  992,  719,  617,   30,   28,  693,   93,
 /*   470 */   615,   60,  172,  615,  246,  617,   82,  104,  907,  256,
 /*   480 */   990,  906,  668,  615,  102,  172,  116,  878,  255,   32,
 /*   490 */   258,    2,  754,  755,  891,  635,  624,   73,  892,  895,
 /*   500 */   252,    7,   32,  696,  607,    9,    8,   30,   28,  185,
 /*   510 */   256,   32,  906,  632,    7,  885,  617,  625,  878,   85,
 /*   520 */   273,  258,  883,  273,  615,  891,  172,  631,   59,  892,
 /*   530 */   895,  931,  214,  273,  245, 1005,  928,  616,  618,  621,
 /*   540 */   616,  618,  621,  197,  198,  256,  199,  906,   39,  653,
 /*   550 */   616,  618,  621,  878,  202,    1,  258,  171,   90,  122,
 /*   560 */   891,  215,  209,  148,  892,  895,  120,  256,  630,  906,
 /*   570 */   264,  134,  160,  127,  273,  878,  558,   67,  258,  222,
 /*   580 */    68,  216,  891,   69,  992,  148,  892,  895,  256,  218,
 /*   590 */   906,  616,  618,  621,  629,  553,  878,   82,   95,  258,
 /*   600 */   962,  990,   60,  891,  586,  133,  147,  892,  895,  590,
 /*   610 */   256,  277,  906,   61,  802,  227,   68,  219,  878,  325,
 /*   620 */   972,  258,  132,  262,  256,  891,  906,  100,   73,  892,
 /*   630 */   895,  621,  878,  224,  971,  258,  169,  236,  256,  891,
 /*   640 */   906,  103,  148,  892,  895,   56,  878,  164,  128,  258,
 /*   650 */   173,    5,  256,  891,  906,  952,  148,  892,  895,  237,
 /*   660 */   878,   75,  106,  258,  301,  223, 1006,  891,  300,  108,
 /*   670 */   146,  892,  895,  240,  268,    4,  220,   64,  256,   96,
 /*   680 */   906,  693,  628,  302,  595,  946,  878,   33,   65,  258,
 /*   690 */   253,   69,  256,  891,  906,  109,  149,  892,  895,   70,
 /*   700 */   878,  989,  299,  258, 1007,  250,   68,  891,   63,  166,
 /*   710 */   142,  892,  895,  256,   17,  906,  115,  242,   78,  938,
 /*   720 */   939,  878,  943,  913,  258,  862,  260,  256,  891,  906,
 /*   730 */   261,  150,  892,  895,  861,  878,  265,  174,  258,  266,
 /*   740 */   124,  256,  891,  906,  267,  143,  892,  895,   48,  878,
 /*   750 */   135,  818,  258,   50,  275,  256,  891,  906,  137,  151,
 /*   760 */   892,  895,  131,  878,  872,  138,  258,  332,  152,  256,
 /*   770 */   891,  906,  153,  903,  892,  895,  139,  878,  778,  871,
 /*   780 */   258,  870,  187,  256,  891,  906,  188,  902,  892,  895,
 /*   790 */   869,  878,  808,  807,  258,  777,  774,  256,  891,  906,
 /*   800 */   768,  901,  892,  895,  763,  878,  868,  859,  258,   87,
 /*   810 */   806,  256,  891,  906,  524,  158,  892,  895,  776,  878,
 /*   820 */   773,  205,  258,  203,  204,  256,  891,  906,  767,  157,
 /*   830 */   892,  895,  766,  878,  762,  761,  258,  210,  760,  867,
 /*   840 */   891,  866,  212,  159,  892,  895,   36,  858,  256,   42,
 /*   850 */   906,   97,   98,    3,  240,   14,  878,   15,   74,  258,
 /*   860 */   101,  105,  256,  891,  906,   43,  156,  892,  895,   65,
 /*   870 */   878,   32,  231,  258,  718,  712,   92,  891,   34,   37,
 /*   880 */   145,  892,  895,  711,   76,   44,   11,  232,  690,   63,
 /*   890 */   206,  883,  689,   91,   19,   45,   20,  111,  745,   80,
 /*   900 */   938,  939,  740,  943,   35,  739,   31,   29,   27,   26,
 /*   910 */    25,   22,   81,  167,   16,  744,   41,  743,  168,   89,
 /*   920 */     8,   31,   29,   27,   26,   25,  118,  636,   13,   31,
 /*   930 */    29,   27,   26,   25,  654,   18,  263,  857,  119,  716,
 /*   940 */   121,  123,   46,  126,  125,   47,   88,  619,   51,  276,
 /*   950 */    86,  179,   38,  280,  882,  579,  587,  129,  278,  274,
 /*   960 */   584,  281,  283,  581,  284,  286,  575,  287,  289,  573,
 /*   970 */   564,  290,   52,   53,  594,  296,  593,  578,  638,  592,
 /*   980 */   303,  577,  522,  543,  576,  542,  541,  536,  540,  539,
 /*   990 */   538,  537,  535,  534,  533,  772,  532,  531,  530,  529,
 /*  1000 */   528,  527,  322,  323,  765,  764,  326,  327,  324,  759,
 /*  1010 */   330,  331,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   158,  163,  146,  165,  146,  160,  146,  149,  163,  149,
 /*    10 */   165,  169,   12,   13,   14,   15,   16,  161,  176,  161,
 /*    20 */   158,  161,  161,  150,  163,   12,   13,   14,   15,   16,
 /*    30 */     0,  169,  148,   14,  145,  162,  163,  181,  176,  155,
 /*    40 */    40,  180,  153,  159,  161,  156,  163,  191,  192,  193,
 /*    50 */    31,  195,  169,   40,  168,  172,   21,  171,  172,  176,
 /*    60 */    21,   61,  179,  180,  181,  182,   31,   32,   29,  186,
 /*    70 */   187,  188,   42,   43,   44,   45,   46,   47,   48,   49,
 /*    80 */    50,   51,   52,   53,   54,   55,   56,   57,   58,    0,
 /*    90 */   207,   14,   15,   16,   12,   13,   14,   15,   16,   99,
 /*   100 */   100,   31,  102,  103,  104,  105,  106,  107,  108,  109,
 /*   110 */   110,  111,   99,  100,   31,  102,  103,  104,  105,  106,
 /*   120 */   107,  108,  109,  110,  111,   12,   13,   14,   15,   16,
 /*   130 */    60,   42,   43,   44,   45,   46,   47,   48,   49,   50,
 /*   140 */    51,   52,   53,   54,   55,   56,   57,   58,   70,   71,
 /*   150 */    72,   73,   74,   75,   76,   77,   78,   79,   80,    0,
 /*   160 */    82,   83,   84,   85,   86,   87,  183,  184,  161,   26,
 /*   170 */   163,  198,   31,   30,   31,  161,  160,  163,   35,  163,
 /*   180 */    37,  165,  115,  169,  211,  115,  172,  180,  215,  175,
 /*   190 */   176,  150,   31,  179,  180,  181,  182,   60,  150,   56,
 /*   200 */   186,  187,   65,  162,  163,  146,   63,   64,  149,   50,
 /*   210 */   162,  163,  198,   54,    0,  161,   31,  163,  136,  137,
 /*   220 */   161,   60,   31,  169,   66,  211,  172,  177,   69,  215,
 /*   230 */   176,  141,   31,  179,  180,  181,  182,  161,   95,  163,
 /*   240 */   186,  187,  188,   92,  194,  169,   39,   88,  172,   31,
 /*   250 */    12,   13,  176,   39,  200,  179,  180,  181,  182,   21,
 /*   260 */   206,  207,  186,  187,  188,  175,  161,   29,  163,   31,
 /*   270 */    31,  120,  121,  197,  169,   39,  169,  172,   40,  146,
 /*   280 */   164,  176,  149,  176,  179,  180,  181,  182,  198,    1,
 /*   290 */     2,  186,  187,  188,  161,  161,  146,  163,   60,  149,
 /*   300 */   115,  211,  197,  169,  160,  215,  172,  163,  148,  165,
 /*   310 */   176,  161,   94,  179,  180,  181,  182,   79,   99,  159,
 /*   320 */   186,  187,  188,   12,   13,   14,  146,  108,  146,  149,
 /*   330 */   111,  197,   21,   94,   96,   97,   98,  218,   12,   13,
 /*   340 */    29,  161,   31,  143,  144,    0,    3,   21,  161,   61,
 /*   350 */   163,   40,  101,  115,   18,   29,  169,   31,   22,  172,
 /*   360 */    24,   25,  175,  176,   12,   13,  179,  180,  181,  145,
 /*   370 */   177,   60,  177,   21,   38,  124,  125,  126,  127,  128,
 /*   380 */   156,   29,  209,   31,   39,  198,   60,  194,   34,  194,
 /*   390 */    79,    4,   40,  171,  172,  161,   68,  163,  211,  162,
 /*   400 */   163,  151,  215,  169,  154,   79,  172,   96,   97,   98,
 /*   410 */   176,  146,   60,  179,  180,  181,  182,  161,  203,  163,
 /*   420 */   186,  187,   96,   97,   98,  169,  161,   61,  172,  113,
 /*   430 */   114,   79,  176,   68,   68,  179,  180,  181,  182,  143,
 /*   440 */   175,  115,  186,  187,   20,  178,  181,   23,   96,   97,
 /*   450 */    98,  115,   19,   12,   13,   14,  191,  192,  193,  196,
 /*   460 */   195,  133,   21,  198,   61,   21,   12,   13,  114,   36,
 /*   470 */    29,   68,   31,   29,  131,   21,  211,   61,  163,  161,
 /*   480 */   215,  163,   61,   29,   68,   31,  212,  169,   40,   68,
 /*   490 */   172,  199,  139,  140,  176,   61,   31,  179,  180,  181,
 /*   500 */   135,   60,   68,  116,   61,    1,    2,   12,   13,  146,
 /*   510 */   161,   68,  163,   31,   60,   60,   21,   31,  169,  148,
 /*   520 */    79,  172,   67,   79,   29,  176,   31,   31,  179,  180,
 /*   530 */   181,  182,  146,   79,  216,  217,  187,   96,   97,   98,
 /*   540 */    96,   97,   98,  174,  161,  161,  166,  163,  148,  101,
 /*   550 */    96,   97,   98,  169,  146,   60,  172,  173,  148,   61,
 /*   560 */   176,  175,  142,  179,  180,  181,   68,  161,   31,  163,
 /*   570 */    61,  146,  142,   61,   79,  169,   61,   68,  172,  173,
 /*   580 */    68,  174,  176,   68,  198,  179,  180,  181,  161,  161,
 /*   590 */   163,   96,   97,   98,   31,   61,  169,  211,  145,  172,
 /*   600 */   178,  215,   68,  176,   61,   19,  179,  180,  181,   61,
 /*   610 */   161,   68,  163,   27,    0,  123,   68,  166,  169,   33,
 /*   620 */   208,  172,   36,  122,  161,  176,  163,  170,  179,  180,
 /*   630 */   181,   98,  169,  169,  208,  172,  173,  210,  161,  176,
 /*   640 */   163,  170,  179,  180,  181,   59,  169,  169,   62,  172,
 /*   650 */   173,  130,  161,  176,  163,  205,  179,  180,  181,  129,
 /*   660 */   169,  202,  204,  172,   50,  118,  217,  176,   54,  201,
 /*   670 */   179,  180,  181,  146,   88,  117,   90,  161,  161,   93,
 /*   680 */   163,  114,   31,   69,   61,  177,  169,  112,  161,  172,
 /*   690 */   134,   68,  161,  176,  163,  189,  179,  180,  181,   61,
 /*   700 */   169,  214,   88,  172,  219,  132,   68,  176,  181,  138,
 /*   710 */   179,  180,  181,  161,   60,  163,  213,  190,  191,  192,
 /*   720 */   193,  169,  195,  185,  172,  170,  169,  161,  176,  163,
 /*   730 */   169,  179,  180,  181,  170,  169,   91,  169,  172,  167,
 /*   740 */   161,  161,  176,  163,  166,  179,  180,  181,  145,  169,
 /*   750 */   154,  161,  172,   60,  157,  161,  176,  163,  146,  179,
 /*   760 */   180,  181,  145,  169,    0,  147,  172,  142,  152,  161,
 /*   770 */   176,  163,  152,  179,  180,  181,  147,  169,    0,    0,
 /*   780 */   172,    0,   56,  161,  176,  163,   67,  179,  180,  181,
 /*   790 */     0,  169,    0,    0,  172,    0,    0,  161,  176,  163,
 /*   800 */     0,  179,  180,  181,    0,  169,    0,    0,  172,   34,
 /*   810 */     0,  161,  176,  163,   41,  179,  180,  181,    0,  169,
 /*   820 */     0,   34,  172,   29,   27,  161,  176,  163,    0,  179,
 /*   830 */   180,  181,    0,  169,    0,    0,  172,   21,    0,    0,
 /*   840 */   176,    0,   21,  179,  180,  181,   94,    0,  161,   60,
 /*   850 */   163,   34,   89,   68,  146,  119,  169,  119,   60,  172,
 /*   860 */    61,   60,  161,  176,  163,   60,  179,  180,  181,  161,
 /*   870 */   169,   68,   29,  172,   61,   61,   19,  176,  113,   68,
 /*   880 */   179,  180,  181,   61,   27,   60,  119,   68,   61,  181,
 /*   890 */    33,   67,   61,   36,   68,    4,    2,   67,   61,  191,
 /*   900 */   192,  193,   29,  195,   68,   29,   12,   13,   14,   15,
 /*   910 */    16,    2,   67,   29,   68,   29,   59,   29,   29,   62,
 /*   920 */     2,   12,   13,   14,   15,   16,   67,   61,   60,   12,
 /*   930 */    13,   14,   15,   16,  101,   60,   92,    0,   61,   61,
 /*   940 */    60,   60,   60,   89,   34,   60,   89,   21,   65,   29,
 /*   950 */    93,   29,   60,   29,   67,   81,   61,   67,   60,   66,
 /*   960 */    61,   60,   29,   61,   60,   29,   61,   60,   29,   61,
 /*   970 */    21,   60,   60,   60,   29,   69,   29,   81,   61,   21,
 /*   980 */    40,   81,   41,   29,   81,   29,   29,   21,   29,   29,
 /*   990 */    29,   29,   29,   29,   29,    0,   29,   29,   29,   29,
 /*  1000 */    29,   29,   29,   27,    0,    0,   29,   28,   34,    0,
 /*  1010 */    21,   20,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1020 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1030 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1040 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1050 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1060 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1070 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1080 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1090 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1100 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1110 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1120 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1130 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1140 */   220,  220,  220,  220,  220,  220,  220,  220,  220,  220,
 /*  1150 */   220,  220,  220,
};
#define YY_SHIFT_COUNT    (332)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1009)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   336,  238,  311,  352,  352,  352,  352,  326,  352,  352,
 /*    10 */    70,  454,  495,  441,  454,  454,  454,  454,  454,  454,
 /*    20 */   454,  454,  454,  454,  454,  454,  454,  454,  454,  454,
 /*    30 */   454,  454,  454,  161,  161,  161,  185,  444,  444,   83,
 /*    40 */    83,  207,  141,  191,  191,   67,  201,  141,   83,   83,
 /*    50 */   141,   83,  141,  141,  141,   83,  236,    0,   13,   13,
 /*    60 */   444,   35,  214,  218,  218,  218,  345,  201,  141,  141,
 /*    70 */   158,   78,  143,   82,  251,  151,   39,  239,  316,  354,
 /*    80 */   316,   19,  343,  387,  465,  482,  207,  486,  496,  207,
 /*    90 */   465,  207,  537,  465,  537,  482,  236,  486,  496,  563,
 /*   100 */   492,  501,  533,  492,  501,  533,  521,  530,  547,  558,
 /*   110 */   567,  486,  651,  575,  571,  556,  573,  654,  141,  501,
 /*   120 */   533,  533,  501,  533,  645,  486,  496,  158,  236,  486,
 /*   130 */   693,  465,  236,  537, 1012, 1012, 1012, 1012,   30,   89,
 /*   140 */   586,  857,  894,  909,  917,  113,  113,  113,  113,  113,
 /*   150 */   113,  113,  159,  614,  288,  219,   77,   77,   77,   77,
 /*   160 */   424,  433,  366,  403,  416,  504,  353,  328,  365,  421,
 /*   170 */   448,  434,  455,  443,  498,  509,  512,  515,  534,  543,
 /*   180 */   548,  623,  638,  137,  764,  778,  779,  781,  726,  719,
 /*   190 */   790,  792,  793,  795,  796,  800,  804,  806,  807,  775,
 /*   200 */   810,  773,  818,  820,  794,  797,  787,  828,  832,  834,
 /*   210 */   835,  816,  838,  821,  839,  841,  752,  847,  789,  817,
 /*   220 */   763,  785,  803,  736,  799,  811,  813,  798,  801,  814,
 /*   230 */   805,  822,  843,  819,  824,  825,  826,  738,  827,  831,
 /*   240 */   830,  765,  836,  845,  837,  846,  767,  891,  873,  876,
 /*   250 */   884,  886,  888,  889,  918,  833,  859,  866,  868,  875,
 /*   260 */   877,  878,  880,  881,  844,  882,  937,  910,  854,  885,
 /*   270 */   883,  887,  890,  926,  892,  893,  895,  920,  922,  898,
 /*   280 */   899,  924,  901,  902,  933,  904,  905,  936,  907,  908,
 /*   290 */   939,  911,  874,  896,  900,  903,  949,  906,  912,  913,
 /*   300 */   945,  947,  958,  941,  940,  954,  956,  957,  959,  960,
 /*   310 */   961,  962,  966,  963,  964,  965,  967,  968,  969,  970,
 /*   320 */   971,  972,  995,  973,  976,  974, 1004,  977,  979, 1005,
 /*   330 */  1009,  989,  991,
};
#define YY_REDUCE_COUNT (137)
#define YY_REDUCE_MIN   (-162)
#define YY_REDUCE_MAX   (708)
static const short yy_reduce_ofst[] = {
 /*     0 */    90,   14,   54, -117,   76,  105,  134,  187,  234,  256,
 /*    10 */   265,  318,  349,  384,  406,  427,  449,  463,  477,  491,
 /*    20 */   517,  531,  552,  566,  580,  594,  608,  622,  636,  650,
 /*    30 */   664,  687,  701,  527, -144,  708,  386, -158, -138, -142,
 /*    40 */  -140, -116, -155, -139,    7,  -27, -114, -127,   59,  133,
 /*    50 */    16,  150,   41,  144,   48,  180, -111,  -17,  -17,  -17,
 /*    60 */   107,  200,  160,   50,  193,  195,  224,  222,  237, -162,
 /*    70 */   250,  116,  182,  119,  173,  215,  296,  267,  263,  263,
 /*    80 */   263,  315,  274,  292,  363,  369,  371,  383,  380,  400,
 /*    90 */   408,  410,  420,  425,  430,  407,  453,  428,  451,  422,
 /*   100 */   412,  457,  464,  426,  471,  478,  450,  458,  459,  468,
 /*   110 */   263,  516,  508,  506,  485,  487,  503,  538,  315,  555,
 /*   120 */   557,  561,  564,  568,  572,  579,  578,  596,  603,  590,
 /*   130 */   597,  612,  617,  625,  618,  616,  620,  629,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*    10 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*    20 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*    30 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*    40 */   756,  783,  756,  756,  756,  756,  756,  756,  756,  756,
 /*    50 */   756,  756,  756,  756,  756,  756,  781,  756,  933,  756,
 /*    60 */   756,  756,  783,  944,  944,  944,  781,  756,  756,  756,
 /*    70 */   846,  756,  756, 1008,  756,  968,  756,  960,  936,  950,
 /*    80 */   937,  756,  993,  953,  756,  756,  783,  756,  756,  783,
 /*    90 */   756,  783,  756,  756,  756,  756,  781,  756,  756,  756,
 /*   100 */   975,  973,  756,  975,  973,  756,  987,  983,  966,  964,
 /*   110 */   950,  756,  756,  756, 1011,  999,  995,  756,  756,  973,
 /*   120 */   756,  756,  973,  756,  860,  756,  756,  756,  781,  756,
 /*   130 */   815,  756,  781,  756,  784,  849,  849,  784,  756,  756,
 /*   140 */   756,  756,  756,  756,  756,  905,  986,  985,  904,  910,
 /*   150 */   909,  908,  756,  756,  756,  756,  899,  900,  898,  897,
 /*   160 */   756,  756,  756,  756,  756,  934,  756,  996, 1000,  756,
 /*   170 */   756,  756,  884,  756,  756,  756,  756,  756,  756,  756,
 /*   180 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   190 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   200 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   210 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   220 */   756,  957,  967,  756,  756,  756,  756,  756,  756,  756,
 /*   230 */   756,  756,  756,  756,  884,  756,  984,  756,  943,  939,
 /*   240 */   756,  756,  935,  756,  756,  994,  756,  756,  756,  756,
 /*   250 */   756,  756,  756,  756,  929,  756,  756,  756,  756,  756,
 /*   260 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   270 */   756,  883,  756,  756,  756,  756,  756,  756,  756,  843,
 /*   280 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   290 */   756,  756,  828,  826,  825,  824,  756,  821,  756,  756,
 /*   300 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   310 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   320 */   756,  756,  756,  756,  756,  756,  756,  756,  756,  756,
 /*   330 */   756,  756,  756,
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
  /*   33 */ "QNODE",
  /*   34 */ "ON",
  /*   35 */ "QNODES",
  /*   36 */ "DATABASE",
  /*   37 */ "DATABASES",
  /*   38 */ "USE",
  /*   39 */ "IF",
  /*   40 */ "NOT",
  /*   41 */ "EXISTS",
  /*   42 */ "BLOCKS",
  /*   43 */ "CACHE",
  /*   44 */ "CACHELAST",
  /*   45 */ "COMP",
  /*   46 */ "DAYS",
  /*   47 */ "FSYNC",
  /*   48 */ "MAXROWS",
  /*   49 */ "MINROWS",
  /*   50 */ "KEEP",
  /*   51 */ "PRECISION",
  /*   52 */ "QUORUM",
  /*   53 */ "REPLICA",
  /*   54 */ "TTL",
  /*   55 */ "WAL",
  /*   56 */ "VGROUPS",
  /*   57 */ "SINGLE_STABLE",
  /*   58 */ "STREAM_MODE",
  /*   59 */ "TABLE",
  /*   60 */ "NK_LP",
  /*   61 */ "NK_RP",
  /*   62 */ "STABLE",
  /*   63 */ "TABLES",
  /*   64 */ "STABLES",
  /*   65 */ "USING",
  /*   66 */ "TAGS",
  /*   67 */ "NK_DOT",
  /*   68 */ "NK_COMMA",
  /*   69 */ "COMMENT",
  /*   70 */ "BOOL",
  /*   71 */ "TINYINT",
  /*   72 */ "SMALLINT",
  /*   73 */ "INT",
  /*   74 */ "INTEGER",
  /*   75 */ "BIGINT",
  /*   76 */ "FLOAT",
  /*   77 */ "DOUBLE",
  /*   78 */ "BINARY",
  /*   79 */ "TIMESTAMP",
  /*   80 */ "NCHAR",
  /*   81 */ "UNSIGNED",
  /*   82 */ "JSON",
  /*   83 */ "VARCHAR",
  /*   84 */ "MEDIUMBLOB",
  /*   85 */ "BLOB",
  /*   86 */ "VARBINARY",
  /*   87 */ "DECIMAL",
  /*   88 */ "SMA",
  /*   89 */ "INDEX",
  /*   90 */ "FULLTEXT",
  /*   91 */ "FUNCTION",
  /*   92 */ "INTERVAL",
  /*   93 */ "TOPIC",
  /*   94 */ "AS",
  /*   95 */ "MNODES",
  /*   96 */ "NK_FLOAT",
  /*   97 */ "NK_BOOL",
  /*   98 */ "NK_VARIABLE",
  /*   99 */ "BETWEEN",
  /*  100 */ "IS",
  /*  101 */ "NULL",
  /*  102 */ "NK_LT",
  /*  103 */ "NK_GT",
  /*  104 */ "NK_LE",
  /*  105 */ "NK_GE",
  /*  106 */ "NK_NE",
  /*  107 */ "NK_EQ",
  /*  108 */ "LIKE",
  /*  109 */ "MATCH",
  /*  110 */ "NMATCH",
  /*  111 */ "IN",
  /*  112 */ "FROM",
  /*  113 */ "JOIN",
  /*  114 */ "INNER",
  /*  115 */ "SELECT",
  /*  116 */ "DISTINCT",
  /*  117 */ "WHERE",
  /*  118 */ "PARTITION",
  /*  119 */ "BY",
  /*  120 */ "SESSION",
  /*  121 */ "STATE_WINDOW",
  /*  122 */ "SLIDING",
  /*  123 */ "FILL",
  /*  124 */ "VALUE",
  /*  125 */ "NONE",
  /*  126 */ "PREV",
  /*  127 */ "LINEAR",
  /*  128 */ "NEXT",
  /*  129 */ "GROUP",
  /*  130 */ "HAVING",
  /*  131 */ "ORDER",
  /*  132 */ "SLIMIT",
  /*  133 */ "SOFFSET",
  /*  134 */ "LIMIT",
  /*  135 */ "OFFSET",
  /*  136 */ "ASC",
  /*  137 */ "DESC",
  /*  138 */ "NULLS",
  /*  139 */ "FIRST",
  /*  140 */ "LAST",
  /*  141 */ "cmd",
  /*  142 */ "user_name",
  /*  143 */ "dnode_endpoint",
  /*  144 */ "dnode_host_name",
  /*  145 */ "not_exists_opt",
  /*  146 */ "db_name",
  /*  147 */ "db_options",
  /*  148 */ "exists_opt",
  /*  149 */ "full_table_name",
  /*  150 */ "column_def_list",
  /*  151 */ "tags_def_opt",
  /*  152 */ "table_options",
  /*  153 */ "multi_create_clause",
  /*  154 */ "tags_def",
  /*  155 */ "multi_drop_clause",
  /*  156 */ "create_subtable_clause",
  /*  157 */ "specific_tags_opt",
  /*  158 */ "literal_list",
  /*  159 */ "drop_table_clause",
  /*  160 */ "col_name_list",
  /*  161 */ "table_name",
  /*  162 */ "column_def",
  /*  163 */ "column_name",
  /*  164 */ "type_name",
  /*  165 */ "col_name",
  /*  166 */ "index_name",
  /*  167 */ "index_options",
  /*  168 */ "func_list",
  /*  169 */ "duration_literal",
  /*  170 */ "sliding_opt",
  /*  171 */ "func",
  /*  172 */ "function_name",
  /*  173 */ "expression_list",
  /*  174 */ "topic_name",
  /*  175 */ "query_expression",
  /*  176 */ "literal",
  /*  177 */ "table_alias",
  /*  178 */ "column_alias",
  /*  179 */ "expression",
  /*  180 */ "column_reference",
  /*  181 */ "subquery",
  /*  182 */ "predicate",
  /*  183 */ "compare_op",
  /*  184 */ "in_op",
  /*  185 */ "in_predicate_value",
  /*  186 */ "boolean_value_expression",
  /*  187 */ "boolean_primary",
  /*  188 */ "common_expression",
  /*  189 */ "from_clause",
  /*  190 */ "table_reference_list",
  /*  191 */ "table_reference",
  /*  192 */ "table_primary",
  /*  193 */ "joined_table",
  /*  194 */ "alias_opt",
  /*  195 */ "parenthesized_joined_table",
  /*  196 */ "join_type",
  /*  197 */ "search_condition",
  /*  198 */ "query_specification",
  /*  199 */ "set_quantifier_opt",
  /*  200 */ "select_list",
  /*  201 */ "where_clause_opt",
  /*  202 */ "partition_by_clause_opt",
  /*  203 */ "twindow_clause_opt",
  /*  204 */ "group_by_clause_opt",
  /*  205 */ "having_clause_opt",
  /*  206 */ "select_sublist",
  /*  207 */ "select_item",
  /*  208 */ "fill_opt",
  /*  209 */ "fill_mode",
  /*  210 */ "group_by_list",
  /*  211 */ "query_expression_body",
  /*  212 */ "order_by_clause_opt",
  /*  213 */ "slimit_clause_opt",
  /*  214 */ "limit_clause_opt",
  /*  215 */ "query_primary",
  /*  216 */ "sort_specification_list",
  /*  217 */ "sort_specification",
  /*  218 */ "ordering_specification_opt",
  /*  219 */ "null_ordering_opt",
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
 /*  13 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  14 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  15 */ "cmd ::= SHOW QNODES",
 /*  16 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  17 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  18 */ "cmd ::= SHOW DATABASES",
 /*  19 */ "cmd ::= USE db_name",
 /*  20 */ "cmd ::= ALTER DATABASE db_name db_options",
 /*  21 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  22 */ "not_exists_opt ::=",
 /*  23 */ "exists_opt ::= IF EXISTS",
 /*  24 */ "exists_opt ::=",
 /*  25 */ "db_options ::=",
 /*  26 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  27 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  28 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  29 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  30 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  31 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  32 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  33 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  34 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  35 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  36 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  37 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  38 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  39 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  40 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  41 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  42 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  43 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  44 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  45 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  46 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  47 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  48 */ "cmd ::= SHOW TABLES",
 /*  49 */ "cmd ::= SHOW STABLES",
 /*  50 */ "multi_create_clause ::= create_subtable_clause",
 /*  51 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  52 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  53 */ "multi_drop_clause ::= drop_table_clause",
 /*  54 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  55 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  56 */ "specific_tags_opt ::=",
 /*  57 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  58 */ "full_table_name ::= table_name",
 /*  59 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  60 */ "column_def_list ::= column_def",
 /*  61 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  62 */ "column_def ::= column_name type_name",
 /*  63 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  64 */ "type_name ::= BOOL",
 /*  65 */ "type_name ::= TINYINT",
 /*  66 */ "type_name ::= SMALLINT",
 /*  67 */ "type_name ::= INT",
 /*  68 */ "type_name ::= INTEGER",
 /*  69 */ "type_name ::= BIGINT",
 /*  70 */ "type_name ::= FLOAT",
 /*  71 */ "type_name ::= DOUBLE",
 /*  72 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  73 */ "type_name ::= TIMESTAMP",
 /*  74 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  75 */ "type_name ::= TINYINT UNSIGNED",
 /*  76 */ "type_name ::= SMALLINT UNSIGNED",
 /*  77 */ "type_name ::= INT UNSIGNED",
 /*  78 */ "type_name ::= BIGINT UNSIGNED",
 /*  79 */ "type_name ::= JSON",
 /*  80 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  81 */ "type_name ::= MEDIUMBLOB",
 /*  82 */ "type_name ::= BLOB",
 /*  83 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  84 */ "type_name ::= DECIMAL",
 /*  85 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  86 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  87 */ "tags_def_opt ::=",
 /*  88 */ "tags_def_opt ::= tags_def",
 /*  89 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  90 */ "table_options ::=",
 /*  91 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  92 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  93 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  94 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  95 */ "col_name_list ::= col_name",
 /*  96 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  97 */ "col_name ::= column_name",
 /*  98 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /*  99 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /* 100 */ "cmd ::= DROP INDEX index_name ON table_name",
 /* 101 */ "index_options ::=",
 /* 102 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 103 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 104 */ "func_list ::= func",
 /* 105 */ "func_list ::= func_list NK_COMMA func",
 /* 106 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 107 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression",
 /* 108 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name",
 /* 109 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 110 */ "cmd ::= SHOW VGROUPS",
 /* 111 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 112 */ "cmd ::= SHOW MNODES",
 /* 113 */ "cmd ::= query_expression",
 /* 114 */ "literal ::= NK_INTEGER",
 /* 115 */ "literal ::= NK_FLOAT",
 /* 116 */ "literal ::= NK_STRING",
 /* 117 */ "literal ::= NK_BOOL",
 /* 118 */ "literal ::= TIMESTAMP NK_STRING",
 /* 119 */ "literal ::= duration_literal",
 /* 120 */ "duration_literal ::= NK_VARIABLE",
 /* 121 */ "literal_list ::= literal",
 /* 122 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 123 */ "db_name ::= NK_ID",
 /* 124 */ "table_name ::= NK_ID",
 /* 125 */ "column_name ::= NK_ID",
 /* 126 */ "function_name ::= NK_ID",
 /* 127 */ "table_alias ::= NK_ID",
 /* 128 */ "column_alias ::= NK_ID",
 /* 129 */ "user_name ::= NK_ID",
 /* 130 */ "index_name ::= NK_ID",
 /* 131 */ "topic_name ::= NK_ID",
 /* 132 */ "expression ::= literal",
 /* 133 */ "expression ::= column_reference",
 /* 134 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 135 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 136 */ "expression ::= subquery",
 /* 137 */ "expression ::= NK_LP expression NK_RP",
 /* 138 */ "expression ::= NK_PLUS expression",
 /* 139 */ "expression ::= NK_MINUS expression",
 /* 140 */ "expression ::= expression NK_PLUS expression",
 /* 141 */ "expression ::= expression NK_MINUS expression",
 /* 142 */ "expression ::= expression NK_STAR expression",
 /* 143 */ "expression ::= expression NK_SLASH expression",
 /* 144 */ "expression ::= expression NK_REM expression",
 /* 145 */ "expression_list ::= expression",
 /* 146 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 147 */ "column_reference ::= column_name",
 /* 148 */ "column_reference ::= table_name NK_DOT column_name",
 /* 149 */ "predicate ::= expression compare_op expression",
 /* 150 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 151 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 152 */ "predicate ::= expression IS NULL",
 /* 153 */ "predicate ::= expression IS NOT NULL",
 /* 154 */ "predicate ::= expression in_op in_predicate_value",
 /* 155 */ "compare_op ::= NK_LT",
 /* 156 */ "compare_op ::= NK_GT",
 /* 157 */ "compare_op ::= NK_LE",
 /* 158 */ "compare_op ::= NK_GE",
 /* 159 */ "compare_op ::= NK_NE",
 /* 160 */ "compare_op ::= NK_EQ",
 /* 161 */ "compare_op ::= LIKE",
 /* 162 */ "compare_op ::= NOT LIKE",
 /* 163 */ "compare_op ::= MATCH",
 /* 164 */ "compare_op ::= NMATCH",
 /* 165 */ "in_op ::= IN",
 /* 166 */ "in_op ::= NOT IN",
 /* 167 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 168 */ "boolean_value_expression ::= boolean_primary",
 /* 169 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 170 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 171 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 172 */ "boolean_primary ::= predicate",
 /* 173 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 174 */ "common_expression ::= expression",
 /* 175 */ "common_expression ::= boolean_value_expression",
 /* 176 */ "from_clause ::= FROM table_reference_list",
 /* 177 */ "table_reference_list ::= table_reference",
 /* 178 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 179 */ "table_reference ::= table_primary",
 /* 180 */ "table_reference ::= joined_table",
 /* 181 */ "table_primary ::= table_name alias_opt",
 /* 182 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 183 */ "table_primary ::= subquery alias_opt",
 /* 184 */ "table_primary ::= parenthesized_joined_table",
 /* 185 */ "alias_opt ::=",
 /* 186 */ "alias_opt ::= table_alias",
 /* 187 */ "alias_opt ::= AS table_alias",
 /* 188 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 189 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 190 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 191 */ "join_type ::=",
 /* 192 */ "join_type ::= INNER",
 /* 193 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 194 */ "set_quantifier_opt ::=",
 /* 195 */ "set_quantifier_opt ::= DISTINCT",
 /* 196 */ "set_quantifier_opt ::= ALL",
 /* 197 */ "select_list ::= NK_STAR",
 /* 198 */ "select_list ::= select_sublist",
 /* 199 */ "select_sublist ::= select_item",
 /* 200 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 201 */ "select_item ::= common_expression",
 /* 202 */ "select_item ::= common_expression column_alias",
 /* 203 */ "select_item ::= common_expression AS column_alias",
 /* 204 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 205 */ "where_clause_opt ::=",
 /* 206 */ "where_clause_opt ::= WHERE search_condition",
 /* 207 */ "partition_by_clause_opt ::=",
 /* 208 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 209 */ "twindow_clause_opt ::=",
 /* 210 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 211 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 212 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 213 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 214 */ "sliding_opt ::=",
 /* 215 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 216 */ "fill_opt ::=",
 /* 217 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 218 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 219 */ "fill_mode ::= NONE",
 /* 220 */ "fill_mode ::= PREV",
 /* 221 */ "fill_mode ::= NULL",
 /* 222 */ "fill_mode ::= LINEAR",
 /* 223 */ "fill_mode ::= NEXT",
 /* 224 */ "group_by_clause_opt ::=",
 /* 225 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 226 */ "group_by_list ::= expression",
 /* 227 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 228 */ "having_clause_opt ::=",
 /* 229 */ "having_clause_opt ::= HAVING search_condition",
 /* 230 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 231 */ "query_expression_body ::= query_primary",
 /* 232 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 233 */ "query_primary ::= query_specification",
 /* 234 */ "order_by_clause_opt ::=",
 /* 235 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 236 */ "slimit_clause_opt ::=",
 /* 237 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 238 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 239 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 240 */ "limit_clause_opt ::=",
 /* 241 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 242 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 243 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 244 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 245 */ "search_condition ::= common_expression",
 /* 246 */ "sort_specification_list ::= sort_specification",
 /* 247 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 248 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 249 */ "ordering_specification_opt ::=",
 /* 250 */ "ordering_specification_opt ::= ASC",
 /* 251 */ "ordering_specification_opt ::= DESC",
 /* 252 */ "null_ordering_opt ::=",
 /* 253 */ "null_ordering_opt ::= NULLS FIRST",
 /* 254 */ "null_ordering_opt ::= NULLS LAST",
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
    case 141: /* cmd */
    case 147: /* db_options */
    case 149: /* full_table_name */
    case 152: /* table_options */
    case 156: /* create_subtable_clause */
    case 159: /* drop_table_clause */
    case 162: /* column_def */
    case 165: /* col_name */
    case 167: /* index_options */
    case 169: /* duration_literal */
    case 170: /* sliding_opt */
    case 171: /* func */
    case 175: /* query_expression */
    case 176: /* literal */
    case 179: /* expression */
    case 180: /* column_reference */
    case 181: /* subquery */
    case 182: /* predicate */
    case 185: /* in_predicate_value */
    case 186: /* boolean_value_expression */
    case 187: /* boolean_primary */
    case 188: /* common_expression */
    case 189: /* from_clause */
    case 190: /* table_reference_list */
    case 191: /* table_reference */
    case 192: /* table_primary */
    case 193: /* joined_table */
    case 195: /* parenthesized_joined_table */
    case 197: /* search_condition */
    case 198: /* query_specification */
    case 201: /* where_clause_opt */
    case 203: /* twindow_clause_opt */
    case 205: /* having_clause_opt */
    case 207: /* select_item */
    case 208: /* fill_opt */
    case 211: /* query_expression_body */
    case 213: /* slimit_clause_opt */
    case 214: /* limit_clause_opt */
    case 215: /* query_primary */
    case 217: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy432)); 
}
      break;
    case 142: /* user_name */
    case 143: /* dnode_endpoint */
    case 144: /* dnode_host_name */
    case 146: /* db_name */
    case 161: /* table_name */
    case 163: /* column_name */
    case 166: /* index_name */
    case 172: /* function_name */
    case 174: /* topic_name */
    case 177: /* table_alias */
    case 178: /* column_alias */
    case 194: /* alias_opt */
{
 
}
      break;
    case 145: /* not_exists_opt */
    case 148: /* exists_opt */
    case 199: /* set_quantifier_opt */
{
 
}
      break;
    case 150: /* column_def_list */
    case 151: /* tags_def_opt */
    case 153: /* multi_create_clause */
    case 154: /* tags_def */
    case 155: /* multi_drop_clause */
    case 157: /* specific_tags_opt */
    case 158: /* literal_list */
    case 160: /* col_name_list */
    case 168: /* func_list */
    case 173: /* expression_list */
    case 200: /* select_list */
    case 202: /* partition_by_clause_opt */
    case 204: /* group_by_clause_opt */
    case 206: /* select_sublist */
    case 210: /* group_by_list */
    case 212: /* order_by_clause_opt */
    case 216: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy24)); 
}
      break;
    case 164: /* type_name */
{
 
}
      break;
    case 183: /* compare_op */
    case 184: /* in_op */
{
 
}
      break;
    case 196: /* join_type */
{
 
}
      break;
    case 209: /* fill_mode */
{
 
}
      break;
    case 218: /* ordering_specification_opt */
{
 
}
      break;
    case 219: /* null_ordering_opt */
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
  {  141,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  141,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  141,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  141,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  141,   -2 }, /* (4) cmd ::= SHOW USERS */
  {  141,   -3 }, /* (5) cmd ::= CREATE DNODE dnode_endpoint */
  {  141,   -5 }, /* (6) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  141,   -3 }, /* (7) cmd ::= DROP DNODE NK_INTEGER */
  {  141,   -3 }, /* (8) cmd ::= DROP DNODE dnode_endpoint */
  {  141,   -2 }, /* (9) cmd ::= SHOW DNODES */
  {  143,   -1 }, /* (10) dnode_endpoint ::= NK_STRING */
  {  144,   -1 }, /* (11) dnode_host_name ::= NK_ID */
  {  144,   -1 }, /* (12) dnode_host_name ::= NK_IPTOKEN */
  {  141,   -5 }, /* (13) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  141,   -5 }, /* (14) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  141,   -2 }, /* (15) cmd ::= SHOW QNODES */
  {  141,   -5 }, /* (16) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  141,   -4 }, /* (17) cmd ::= DROP DATABASE exists_opt db_name */
  {  141,   -2 }, /* (18) cmd ::= SHOW DATABASES */
  {  141,   -2 }, /* (19) cmd ::= USE db_name */
  {  141,   -4 }, /* (20) cmd ::= ALTER DATABASE db_name db_options */
  {  145,   -3 }, /* (21) not_exists_opt ::= IF NOT EXISTS */
  {  145,    0 }, /* (22) not_exists_opt ::= */
  {  148,   -2 }, /* (23) exists_opt ::= IF EXISTS */
  {  148,    0 }, /* (24) exists_opt ::= */
  {  147,    0 }, /* (25) db_options ::= */
  {  147,   -3 }, /* (26) db_options ::= db_options BLOCKS NK_INTEGER */
  {  147,   -3 }, /* (27) db_options ::= db_options CACHE NK_INTEGER */
  {  147,   -3 }, /* (28) db_options ::= db_options CACHELAST NK_INTEGER */
  {  147,   -3 }, /* (29) db_options ::= db_options COMP NK_INTEGER */
  {  147,   -3 }, /* (30) db_options ::= db_options DAYS NK_INTEGER */
  {  147,   -3 }, /* (31) db_options ::= db_options FSYNC NK_INTEGER */
  {  147,   -3 }, /* (32) db_options ::= db_options MAXROWS NK_INTEGER */
  {  147,   -3 }, /* (33) db_options ::= db_options MINROWS NK_INTEGER */
  {  147,   -3 }, /* (34) db_options ::= db_options KEEP NK_INTEGER */
  {  147,   -3 }, /* (35) db_options ::= db_options PRECISION NK_STRING */
  {  147,   -3 }, /* (36) db_options ::= db_options QUORUM NK_INTEGER */
  {  147,   -3 }, /* (37) db_options ::= db_options REPLICA NK_INTEGER */
  {  147,   -3 }, /* (38) db_options ::= db_options TTL NK_INTEGER */
  {  147,   -3 }, /* (39) db_options ::= db_options WAL NK_INTEGER */
  {  147,   -3 }, /* (40) db_options ::= db_options VGROUPS NK_INTEGER */
  {  147,   -3 }, /* (41) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  147,   -3 }, /* (42) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  141,   -9 }, /* (43) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  141,   -3 }, /* (44) cmd ::= CREATE TABLE multi_create_clause */
  {  141,   -9 }, /* (45) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  141,   -3 }, /* (46) cmd ::= DROP TABLE multi_drop_clause */
  {  141,   -4 }, /* (47) cmd ::= DROP STABLE exists_opt full_table_name */
  {  141,   -2 }, /* (48) cmd ::= SHOW TABLES */
  {  141,   -2 }, /* (49) cmd ::= SHOW STABLES */
  {  153,   -1 }, /* (50) multi_create_clause ::= create_subtable_clause */
  {  153,   -2 }, /* (51) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  156,   -9 }, /* (52) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  155,   -1 }, /* (53) multi_drop_clause ::= drop_table_clause */
  {  155,   -2 }, /* (54) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  159,   -2 }, /* (55) drop_table_clause ::= exists_opt full_table_name */
  {  157,    0 }, /* (56) specific_tags_opt ::= */
  {  157,   -3 }, /* (57) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  149,   -1 }, /* (58) full_table_name ::= table_name */
  {  149,   -3 }, /* (59) full_table_name ::= db_name NK_DOT table_name */
  {  150,   -1 }, /* (60) column_def_list ::= column_def */
  {  150,   -3 }, /* (61) column_def_list ::= column_def_list NK_COMMA column_def */
  {  162,   -2 }, /* (62) column_def ::= column_name type_name */
  {  162,   -4 }, /* (63) column_def ::= column_name type_name COMMENT NK_STRING */
  {  164,   -1 }, /* (64) type_name ::= BOOL */
  {  164,   -1 }, /* (65) type_name ::= TINYINT */
  {  164,   -1 }, /* (66) type_name ::= SMALLINT */
  {  164,   -1 }, /* (67) type_name ::= INT */
  {  164,   -1 }, /* (68) type_name ::= INTEGER */
  {  164,   -1 }, /* (69) type_name ::= BIGINT */
  {  164,   -1 }, /* (70) type_name ::= FLOAT */
  {  164,   -1 }, /* (71) type_name ::= DOUBLE */
  {  164,   -4 }, /* (72) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  164,   -1 }, /* (73) type_name ::= TIMESTAMP */
  {  164,   -4 }, /* (74) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  164,   -2 }, /* (75) type_name ::= TINYINT UNSIGNED */
  {  164,   -2 }, /* (76) type_name ::= SMALLINT UNSIGNED */
  {  164,   -2 }, /* (77) type_name ::= INT UNSIGNED */
  {  164,   -2 }, /* (78) type_name ::= BIGINT UNSIGNED */
  {  164,   -1 }, /* (79) type_name ::= JSON */
  {  164,   -4 }, /* (80) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  164,   -1 }, /* (81) type_name ::= MEDIUMBLOB */
  {  164,   -1 }, /* (82) type_name ::= BLOB */
  {  164,   -4 }, /* (83) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  164,   -1 }, /* (84) type_name ::= DECIMAL */
  {  164,   -4 }, /* (85) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  164,   -6 }, /* (86) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  151,    0 }, /* (87) tags_def_opt ::= */
  {  151,   -1 }, /* (88) tags_def_opt ::= tags_def */
  {  154,   -4 }, /* (89) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  152,    0 }, /* (90) table_options ::= */
  {  152,   -3 }, /* (91) table_options ::= table_options COMMENT NK_STRING */
  {  152,   -3 }, /* (92) table_options ::= table_options KEEP NK_INTEGER */
  {  152,   -3 }, /* (93) table_options ::= table_options TTL NK_INTEGER */
  {  152,   -5 }, /* (94) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  160,   -1 }, /* (95) col_name_list ::= col_name */
  {  160,   -3 }, /* (96) col_name_list ::= col_name_list NK_COMMA col_name */
  {  165,   -1 }, /* (97) col_name ::= column_name */
  {  141,   -7 }, /* (98) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  141,   -9 }, /* (99) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  141,   -5 }, /* (100) cmd ::= DROP INDEX index_name ON table_name */
  {  167,    0 }, /* (101) index_options ::= */
  {  167,   -9 }, /* (102) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  167,  -11 }, /* (103) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  168,   -1 }, /* (104) func_list ::= func */
  {  168,   -3 }, /* (105) func_list ::= func_list NK_COMMA func */
  {  171,   -4 }, /* (106) func ::= function_name NK_LP expression_list NK_RP */
  {  141,   -6 }, /* (107) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
  {  141,   -6 }, /* (108) cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
  {  141,   -4 }, /* (109) cmd ::= DROP TOPIC exists_opt topic_name */
  {  141,   -2 }, /* (110) cmd ::= SHOW VGROUPS */
  {  141,   -4 }, /* (111) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  141,   -2 }, /* (112) cmd ::= SHOW MNODES */
  {  141,   -1 }, /* (113) cmd ::= query_expression */
  {  176,   -1 }, /* (114) literal ::= NK_INTEGER */
  {  176,   -1 }, /* (115) literal ::= NK_FLOAT */
  {  176,   -1 }, /* (116) literal ::= NK_STRING */
  {  176,   -1 }, /* (117) literal ::= NK_BOOL */
  {  176,   -2 }, /* (118) literal ::= TIMESTAMP NK_STRING */
  {  176,   -1 }, /* (119) literal ::= duration_literal */
  {  169,   -1 }, /* (120) duration_literal ::= NK_VARIABLE */
  {  158,   -1 }, /* (121) literal_list ::= literal */
  {  158,   -3 }, /* (122) literal_list ::= literal_list NK_COMMA literal */
  {  146,   -1 }, /* (123) db_name ::= NK_ID */
  {  161,   -1 }, /* (124) table_name ::= NK_ID */
  {  163,   -1 }, /* (125) column_name ::= NK_ID */
  {  172,   -1 }, /* (126) function_name ::= NK_ID */
  {  177,   -1 }, /* (127) table_alias ::= NK_ID */
  {  178,   -1 }, /* (128) column_alias ::= NK_ID */
  {  142,   -1 }, /* (129) user_name ::= NK_ID */
  {  166,   -1 }, /* (130) index_name ::= NK_ID */
  {  174,   -1 }, /* (131) topic_name ::= NK_ID */
  {  179,   -1 }, /* (132) expression ::= literal */
  {  179,   -1 }, /* (133) expression ::= column_reference */
  {  179,   -4 }, /* (134) expression ::= function_name NK_LP expression_list NK_RP */
  {  179,   -4 }, /* (135) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  179,   -1 }, /* (136) expression ::= subquery */
  {  179,   -3 }, /* (137) expression ::= NK_LP expression NK_RP */
  {  179,   -2 }, /* (138) expression ::= NK_PLUS expression */
  {  179,   -2 }, /* (139) expression ::= NK_MINUS expression */
  {  179,   -3 }, /* (140) expression ::= expression NK_PLUS expression */
  {  179,   -3 }, /* (141) expression ::= expression NK_MINUS expression */
  {  179,   -3 }, /* (142) expression ::= expression NK_STAR expression */
  {  179,   -3 }, /* (143) expression ::= expression NK_SLASH expression */
  {  179,   -3 }, /* (144) expression ::= expression NK_REM expression */
  {  173,   -1 }, /* (145) expression_list ::= expression */
  {  173,   -3 }, /* (146) expression_list ::= expression_list NK_COMMA expression */
  {  180,   -1 }, /* (147) column_reference ::= column_name */
  {  180,   -3 }, /* (148) column_reference ::= table_name NK_DOT column_name */
  {  182,   -3 }, /* (149) predicate ::= expression compare_op expression */
  {  182,   -5 }, /* (150) predicate ::= expression BETWEEN expression AND expression */
  {  182,   -6 }, /* (151) predicate ::= expression NOT BETWEEN expression AND expression */
  {  182,   -3 }, /* (152) predicate ::= expression IS NULL */
  {  182,   -4 }, /* (153) predicate ::= expression IS NOT NULL */
  {  182,   -3 }, /* (154) predicate ::= expression in_op in_predicate_value */
  {  183,   -1 }, /* (155) compare_op ::= NK_LT */
  {  183,   -1 }, /* (156) compare_op ::= NK_GT */
  {  183,   -1 }, /* (157) compare_op ::= NK_LE */
  {  183,   -1 }, /* (158) compare_op ::= NK_GE */
  {  183,   -1 }, /* (159) compare_op ::= NK_NE */
  {  183,   -1 }, /* (160) compare_op ::= NK_EQ */
  {  183,   -1 }, /* (161) compare_op ::= LIKE */
  {  183,   -2 }, /* (162) compare_op ::= NOT LIKE */
  {  183,   -1 }, /* (163) compare_op ::= MATCH */
  {  183,   -1 }, /* (164) compare_op ::= NMATCH */
  {  184,   -1 }, /* (165) in_op ::= IN */
  {  184,   -2 }, /* (166) in_op ::= NOT IN */
  {  185,   -3 }, /* (167) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  186,   -1 }, /* (168) boolean_value_expression ::= boolean_primary */
  {  186,   -2 }, /* (169) boolean_value_expression ::= NOT boolean_primary */
  {  186,   -3 }, /* (170) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  186,   -3 }, /* (171) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  187,   -1 }, /* (172) boolean_primary ::= predicate */
  {  187,   -3 }, /* (173) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  188,   -1 }, /* (174) common_expression ::= expression */
  {  188,   -1 }, /* (175) common_expression ::= boolean_value_expression */
  {  189,   -2 }, /* (176) from_clause ::= FROM table_reference_list */
  {  190,   -1 }, /* (177) table_reference_list ::= table_reference */
  {  190,   -3 }, /* (178) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  191,   -1 }, /* (179) table_reference ::= table_primary */
  {  191,   -1 }, /* (180) table_reference ::= joined_table */
  {  192,   -2 }, /* (181) table_primary ::= table_name alias_opt */
  {  192,   -4 }, /* (182) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  192,   -2 }, /* (183) table_primary ::= subquery alias_opt */
  {  192,   -1 }, /* (184) table_primary ::= parenthesized_joined_table */
  {  194,    0 }, /* (185) alias_opt ::= */
  {  194,   -1 }, /* (186) alias_opt ::= table_alias */
  {  194,   -2 }, /* (187) alias_opt ::= AS table_alias */
  {  195,   -3 }, /* (188) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  195,   -3 }, /* (189) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  193,   -6 }, /* (190) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  196,    0 }, /* (191) join_type ::= */
  {  196,   -1 }, /* (192) join_type ::= INNER */
  {  198,   -9 }, /* (193) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  199,    0 }, /* (194) set_quantifier_opt ::= */
  {  199,   -1 }, /* (195) set_quantifier_opt ::= DISTINCT */
  {  199,   -1 }, /* (196) set_quantifier_opt ::= ALL */
  {  200,   -1 }, /* (197) select_list ::= NK_STAR */
  {  200,   -1 }, /* (198) select_list ::= select_sublist */
  {  206,   -1 }, /* (199) select_sublist ::= select_item */
  {  206,   -3 }, /* (200) select_sublist ::= select_sublist NK_COMMA select_item */
  {  207,   -1 }, /* (201) select_item ::= common_expression */
  {  207,   -2 }, /* (202) select_item ::= common_expression column_alias */
  {  207,   -3 }, /* (203) select_item ::= common_expression AS column_alias */
  {  207,   -3 }, /* (204) select_item ::= table_name NK_DOT NK_STAR */
  {  201,    0 }, /* (205) where_clause_opt ::= */
  {  201,   -2 }, /* (206) where_clause_opt ::= WHERE search_condition */
  {  202,    0 }, /* (207) partition_by_clause_opt ::= */
  {  202,   -3 }, /* (208) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  203,    0 }, /* (209) twindow_clause_opt ::= */
  {  203,   -6 }, /* (210) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  203,   -4 }, /* (211) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  203,   -6 }, /* (212) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  203,   -8 }, /* (213) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  170,    0 }, /* (214) sliding_opt ::= */
  {  170,   -4 }, /* (215) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  208,    0 }, /* (216) fill_opt ::= */
  {  208,   -4 }, /* (217) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  208,   -6 }, /* (218) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  209,   -1 }, /* (219) fill_mode ::= NONE */
  {  209,   -1 }, /* (220) fill_mode ::= PREV */
  {  209,   -1 }, /* (221) fill_mode ::= NULL */
  {  209,   -1 }, /* (222) fill_mode ::= LINEAR */
  {  209,   -1 }, /* (223) fill_mode ::= NEXT */
  {  204,    0 }, /* (224) group_by_clause_opt ::= */
  {  204,   -3 }, /* (225) group_by_clause_opt ::= GROUP BY group_by_list */
  {  210,   -1 }, /* (226) group_by_list ::= expression */
  {  210,   -3 }, /* (227) group_by_list ::= group_by_list NK_COMMA expression */
  {  205,    0 }, /* (228) having_clause_opt ::= */
  {  205,   -2 }, /* (229) having_clause_opt ::= HAVING search_condition */
  {  175,   -4 }, /* (230) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  211,   -1 }, /* (231) query_expression_body ::= query_primary */
  {  211,   -4 }, /* (232) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  215,   -1 }, /* (233) query_primary ::= query_specification */
  {  212,    0 }, /* (234) order_by_clause_opt ::= */
  {  212,   -3 }, /* (235) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  213,    0 }, /* (236) slimit_clause_opt ::= */
  {  213,   -2 }, /* (237) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  213,   -4 }, /* (238) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  213,   -4 }, /* (239) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  214,    0 }, /* (240) limit_clause_opt ::= */
  {  214,   -2 }, /* (241) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  214,   -4 }, /* (242) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  214,   -4 }, /* (243) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  181,   -3 }, /* (244) subquery ::= NK_LP query_expression NK_RP */
  {  197,   -1 }, /* (245) search_condition ::= common_expression */
  {  216,   -1 }, /* (246) sort_specification_list ::= sort_specification */
  {  216,   -3 }, /* (247) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  217,   -3 }, /* (248) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  218,    0 }, /* (249) ordering_specification_opt ::= */
  {  218,   -1 }, /* (250) ordering_specification_opt ::= ASC */
  {  218,   -1 }, /* (251) ordering_specification_opt ::= DESC */
  {  219,    0 }, /* (252) null_ordering_opt ::= */
  {  219,   -2 }, /* (253) null_ordering_opt ::= NULLS FIRST */
  {  219,   -2 }, /* (254) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0); }
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy129, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy129, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0); }
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy129); }
        break;
      case 4: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy129, NULL); }
        break;
      case 6: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0); }
        break;
      case 7: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 8: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy129); }
        break;
      case 9: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 123: /* db_name ::= NK_ID */ yytestcase(yyruleno==123);
      case 124: /* table_name ::= NK_ID */ yytestcase(yyruleno==124);
      case 125: /* column_name ::= NK_ID */ yytestcase(yyruleno==125);
      case 126: /* function_name ::= NK_ID */ yytestcase(yyruleno==126);
      case 127: /* table_alias ::= NK_ID */ yytestcase(yyruleno==127);
      case 128: /* column_alias ::= NK_ID */ yytestcase(yyruleno==128);
      case 129: /* user_name ::= NK_ID */ yytestcase(yyruleno==129);
      case 130: /* index_name ::= NK_ID */ yytestcase(yyruleno==130);
      case 131: /* topic_name ::= NK_ID */ yytestcase(yyruleno==131);
{ yylhsminor.yy129 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 13: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 14: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 15: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL); }
        break;
      case 16: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy97, &yymsp[-1].minor.yy129, yymsp[0].minor.yy432); }
        break;
      case 17: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy97, &yymsp[0].minor.yy129); }
        break;
      case 18: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 19: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy129); }
        break;
      case 20: /* cmd ::= ALTER DATABASE db_name db_options */
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy129, yymsp[0].minor.yy432); }
        break;
      case 21: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy97 = true; }
        break;
      case 22: /* not_exists_opt ::= */
      case 24: /* exists_opt ::= */ yytestcase(yyruleno==24);
      case 194: /* set_quantifier_opt ::= */ yytestcase(yyruleno==194);
{ yymsp[1].minor.yy97 = false; }
        break;
      case 23: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy97 = true; }
        break;
      case 25: /* db_options ::= */
{ yymsp[1].minor.yy432 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 26: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 27: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 28: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 29: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 30: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 31: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 32: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 33: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 34: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 35: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 36: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 37: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 38: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 39: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 40: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 41: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 42: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy432 = setDatabaseOption(pCxt, yymsp[-2].minor.yy432, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 43: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 45: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==45);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy97, yymsp[-5].minor.yy432, yymsp[-3].minor.yy24, yymsp[-1].minor.yy24, yymsp[0].minor.yy432);}
        break;
      case 44: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy24);}
        break;
      case 46: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy24); }
        break;
      case 47: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy97, yymsp[0].minor.yy432); }
        break;
      case 48: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 49: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 50: /* multi_create_clause ::= create_subtable_clause */
      case 53: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==53);
      case 60: /* column_def_list ::= column_def */ yytestcase(yyruleno==60);
      case 95: /* col_name_list ::= col_name */ yytestcase(yyruleno==95);
      case 104: /* func_list ::= func */ yytestcase(yyruleno==104);
      case 199: /* select_sublist ::= select_item */ yytestcase(yyruleno==199);
      case 246: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==246);
{ yylhsminor.yy24 = createNodeList(pCxt, yymsp[0].minor.yy432); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 51: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 54: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==54);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-1].minor.yy24, yymsp[0].minor.yy432); }
  yymsp[-1].minor.yy24 = yylhsminor.yy24;
        break;
      case 52: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy432 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy97, yymsp[-7].minor.yy432, yymsp[-5].minor.yy432, yymsp[-4].minor.yy24, yymsp[-1].minor.yy24); }
  yymsp[-8].minor.yy432 = yylhsminor.yy432;
        break;
      case 55: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy432 = createDropTableClause(pCxt, yymsp[-1].minor.yy97, yymsp[0].minor.yy432); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 56: /* specific_tags_opt ::= */
      case 87: /* tags_def_opt ::= */ yytestcase(yyruleno==87);
      case 207: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==207);
      case 224: /* group_by_clause_opt ::= */ yytestcase(yyruleno==224);
      case 234: /* order_by_clause_opt ::= */ yytestcase(yyruleno==234);
{ yymsp[1].minor.yy24 = NULL; }
        break;
      case 57: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy24 = yymsp[-1].minor.yy24; }
        break;
      case 58: /* full_table_name ::= table_name */
{ yylhsminor.yy432 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy129, NULL); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 59: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy432 = createRealTableNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129, NULL); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 61: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 96: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==96);
      case 105: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==105);
      case 200: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==200);
      case 247: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==247);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, yymsp[0].minor.yy432); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 62: /* column_def ::= column_name type_name */
{ yylhsminor.yy432 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy129, yymsp[0].minor.yy224, NULL); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 63: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy432 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-2].minor.yy224, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 64: /* type_name ::= BOOL */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 65: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 66: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 67: /* type_name ::= INT */
      case 68: /* type_name ::= INTEGER */ yytestcase(yyruleno==68);
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 69: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 70: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 71: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 72: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy224 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 73: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 74: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy224 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 75: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy224 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 76: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy224 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 77: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy224 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 78: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy224 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 79: /* type_name ::= JSON */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 80: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy224 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 81: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 82: /* type_name ::= BLOB */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 83: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy224 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 84: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy224 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 85: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy224 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 86: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy224 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 88: /* tags_def_opt ::= tags_def */
      case 198: /* select_list ::= select_sublist */ yytestcase(yyruleno==198);
{ yylhsminor.yy24 = yymsp[0].minor.yy24; }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 89: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy24 = yymsp[-1].minor.yy24; }
        break;
      case 90: /* table_options ::= */
{ yymsp[1].minor.yy432 = createDefaultTableOptions(pCxt);}
        break;
      case 91: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy432 = setTableOption(pCxt, yymsp[-2].minor.yy432, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 92: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy432 = setTableOption(pCxt, yymsp[-2].minor.yy432, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 93: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy432 = setTableOption(pCxt, yymsp[-2].minor.yy432, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 94: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy432 = setTableSmaOption(pCxt, yymsp[-4].minor.yy432, yymsp[-1].minor.yy24); }
  yymsp[-4].minor.yy432 = yylhsminor.yy432;
        break;
      case 97: /* col_name ::= column_name */
{ yylhsminor.yy432 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy129); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 98: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy129, &yymsp[-1].minor.yy129, NULL, yymsp[0].minor.yy432); }
        break;
      case 99: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy129, &yymsp[-3].minor.yy129, yymsp[-1].minor.yy24, NULL); }
        break;
      case 100: /* cmd ::= DROP INDEX index_name ON table_name */
{ pCxt->pRootNode = createDropIndexStmt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129); }
        break;
      case 101: /* index_options ::= */
      case 205: /* where_clause_opt ::= */ yytestcase(yyruleno==205);
      case 209: /* twindow_clause_opt ::= */ yytestcase(yyruleno==209);
      case 214: /* sliding_opt ::= */ yytestcase(yyruleno==214);
      case 216: /* fill_opt ::= */ yytestcase(yyruleno==216);
      case 228: /* having_clause_opt ::= */ yytestcase(yyruleno==228);
      case 236: /* slimit_clause_opt ::= */ yytestcase(yyruleno==236);
      case 240: /* limit_clause_opt ::= */ yytestcase(yyruleno==240);
{ yymsp[1].minor.yy432 = NULL; }
        break;
      case 102: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy432 = createIndexOption(pCxt, yymsp[-6].minor.yy24, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), NULL, yymsp[0].minor.yy432); }
        break;
      case 103: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy432 = createIndexOption(pCxt, yymsp[-8].minor.yy24, releaseRawExprNode(pCxt, yymsp[-4].minor.yy432), releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), yymsp[0].minor.yy432); }
        break;
      case 106: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy432 = createFunctionNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-1].minor.yy24); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 107: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy97, &yymsp[-2].minor.yy129, yymsp[0].minor.yy432, NULL); }
        break;
      case 108: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy97, &yymsp[-2].minor.yy129, NULL, &yymsp[0].minor.yy129); }
        break;
      case 109: /* cmd ::= DROP TOPIC exists_opt topic_name */
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy97, &yymsp[0].minor.yy129); }
        break;
      case 110: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 111: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy129); }
        break;
      case 112: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 113: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy432; }
        break;
      case 114: /* literal ::= NK_INTEGER */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 115: /* literal ::= NK_FLOAT */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 116: /* literal ::= NK_STRING */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 117: /* literal ::= NK_BOOL */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 118: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 119: /* literal ::= duration_literal */
      case 132: /* expression ::= literal */ yytestcase(yyruleno==132);
      case 133: /* expression ::= column_reference */ yytestcase(yyruleno==133);
      case 136: /* expression ::= subquery */ yytestcase(yyruleno==136);
      case 168: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==168);
      case 172: /* boolean_primary ::= predicate */ yytestcase(yyruleno==172);
      case 174: /* common_expression ::= expression */ yytestcase(yyruleno==174);
      case 175: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==175);
      case 177: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==177);
      case 179: /* table_reference ::= table_primary */ yytestcase(yyruleno==179);
      case 180: /* table_reference ::= joined_table */ yytestcase(yyruleno==180);
      case 184: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==184);
      case 231: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==231);
      case 233: /* query_primary ::= query_specification */ yytestcase(yyruleno==233);
{ yylhsminor.yy432 = yymsp[0].minor.yy432; }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 120: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 121: /* literal_list ::= literal */
      case 145: /* expression_list ::= expression */ yytestcase(yyruleno==145);
{ yylhsminor.yy24 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy432)); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 122: /* literal_list ::= literal_list NK_COMMA literal */
      case 146: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==146);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, releaseRawExprNode(pCxt, yymsp[0].minor.yy432)); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 134: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-1].minor.yy24)); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 135: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 137: /* expression ::= NK_LP expression NK_RP */
      case 173: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==173);
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy432)); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 138: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy432));
                                                                                  }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 139: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy432), NULL));
                                                                                  }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 140: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); 
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 141: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); 
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 142: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); 
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 143: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); 
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 144: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); 
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 147: /* column_reference ::= column_name */
{ yylhsminor.yy432 = createRawExprNode(pCxt, &yymsp[0].minor.yy129, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy129)); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 148: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129, createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129)); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 149: /* predicate ::= expression compare_op expression */
      case 154: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==154);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy260, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432)));
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 150: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy432), releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432)));
                                                                                  }
  yymsp[-4].minor.yy432 = yylhsminor.yy432;
        break;
      case 151: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[-5].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432)));
                                                                                  }
  yymsp[-5].minor.yy432 = yylhsminor.yy432;
        break;
      case 152: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), NULL));
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 153: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy432), NULL));
                                                                                  }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 155: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy260 = OP_TYPE_LOWER_THAN; }
        break;
      case 156: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy260 = OP_TYPE_GREATER_THAN; }
        break;
      case 157: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy260 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 158: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy260 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 159: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy260 = OP_TYPE_NOT_EQUAL; }
        break;
      case 160: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy260 = OP_TYPE_EQUAL; }
        break;
      case 161: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy260 = OP_TYPE_LIKE; }
        break;
      case 162: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy260 = OP_TYPE_NOT_LIKE; }
        break;
      case 163: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy260 = OP_TYPE_MATCH; }
        break;
      case 164: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy260 = OP_TYPE_NMATCH; }
        break;
      case 165: /* in_op ::= IN */
{ yymsp[0].minor.yy260 = OP_TYPE_IN; }
        break;
      case 166: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy260 = OP_TYPE_NOT_IN; }
        break;
      case 167: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy24)); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 169: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy432), NULL));
                                                                                  }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 170: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432)));
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 171: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy432);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), releaseRawExprNode(pCxt, yymsp[0].minor.yy432)));
                                                                                  }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 176: /* from_clause ::= FROM table_reference_list */
      case 206: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==206);
      case 229: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==229);
{ yymsp[-1].minor.yy432 = yymsp[0].minor.yy432; }
        break;
      case 178: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy432 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy432, yymsp[0].minor.yy432, NULL); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 181: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy432 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 182: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy432 = createRealTableNode(pCxt, &yymsp[-3].minor.yy129, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 183: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy432 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy432), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 185: /* alias_opt ::= */
{ yymsp[1].minor.yy129 = nil_token;  }
        break;
      case 186: /* alias_opt ::= table_alias */
{ yylhsminor.yy129 = yymsp[0].minor.yy129; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 187: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy129 = yymsp[0].minor.yy129; }
        break;
      case 188: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 189: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==189);
{ yymsp[-2].minor.yy432 = yymsp[-1].minor.yy432; }
        break;
      case 190: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy432 = createJoinTableNode(pCxt, yymsp[-4].minor.yy332, yymsp[-5].minor.yy432, yymsp[-2].minor.yy432, yymsp[0].minor.yy432); }
  yymsp[-5].minor.yy432 = yylhsminor.yy432;
        break;
      case 191: /* join_type ::= */
{ yymsp[1].minor.yy332 = JOIN_TYPE_INNER; }
        break;
      case 192: /* join_type ::= INNER */
{ yymsp[0].minor.yy332 = JOIN_TYPE_INNER; }
        break;
      case 193: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy432 = createSelectStmt(pCxt, yymsp[-7].minor.yy97, yymsp[-6].minor.yy24, yymsp[-5].minor.yy432);
                                                                                    yymsp[-8].minor.yy432 = addWhereClause(pCxt, yymsp[-8].minor.yy432, yymsp[-4].minor.yy432);
                                                                                    yymsp[-8].minor.yy432 = addPartitionByClause(pCxt, yymsp[-8].minor.yy432, yymsp[-3].minor.yy24);
                                                                                    yymsp[-8].minor.yy432 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy432, yymsp[-2].minor.yy432);
                                                                                    yymsp[-8].minor.yy432 = addGroupByClause(pCxt, yymsp[-8].minor.yy432, yymsp[-1].minor.yy24);
                                                                                    yymsp[-8].minor.yy432 = addHavingClause(pCxt, yymsp[-8].minor.yy432, yymsp[0].minor.yy432);
                                                                                  }
        break;
      case 195: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy97 = true; }
        break;
      case 196: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy97 = false; }
        break;
      case 197: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy24 = NULL; }
        break;
      case 201: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy432);
                                                                                    yylhsminor.yy432 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy432), &t);
                                                                                  }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 202: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy432 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy432), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy432 = yylhsminor.yy432;
        break;
      case 203: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy432 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), &yymsp[0].minor.yy129); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 204: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy432 = createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 208: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 225: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==225);
      case 235: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==235);
{ yymsp[-2].minor.yy24 = yymsp[0].minor.yy24; }
        break;
      case 210: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy432 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy432), &yymsp[-1].minor.yy0); }
        break;
      case 211: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy432 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy432)); }
        break;
      case 212: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy432 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy432), NULL, yymsp[-1].minor.yy432, yymsp[0].minor.yy432); }
        break;
      case 213: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy432 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy432), releaseRawExprNode(pCxt, yymsp[-3].minor.yy432), yymsp[-1].minor.yy432, yymsp[0].minor.yy432); }
        break;
      case 215: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy432 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy432); }
        break;
      case 217: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy432 = createFillNode(pCxt, yymsp[-1].minor.yy294, NULL); }
        break;
      case 218: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy432 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy24)); }
        break;
      case 219: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy294 = FILL_MODE_NONE; }
        break;
      case 220: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy294 = FILL_MODE_PREV; }
        break;
      case 221: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy294 = FILL_MODE_NULL; }
        break;
      case 222: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy294 = FILL_MODE_LINEAR; }
        break;
      case 223: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy294 = FILL_MODE_NEXT; }
        break;
      case 226: /* group_by_list ::= expression */
{ yylhsminor.yy24 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 227: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy432))); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 230: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy432 = addOrderByClause(pCxt, yymsp[-3].minor.yy432, yymsp[-2].minor.yy24);
                                                                                    yylhsminor.yy432 = addSlimitClause(pCxt, yylhsminor.yy432, yymsp[-1].minor.yy432);
                                                                                    yylhsminor.yy432 = addLimitClause(pCxt, yylhsminor.yy432, yymsp[0].minor.yy432);
                                                                                  }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 232: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy432 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy432, yymsp[0].minor.yy432); }
  yymsp[-3].minor.yy432 = yylhsminor.yy432;
        break;
      case 237: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 241: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==241);
{ yymsp[-1].minor.yy432 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 238: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 242: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==242);
{ yymsp[-3].minor.yy432 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 239: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 243: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==243);
{ yymsp[-3].minor.yy432 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 244: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy432 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy432); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 245: /* search_condition ::= common_expression */
{ yylhsminor.yy432 = releaseRawExprNode(pCxt, yymsp[0].minor.yy432); }
  yymsp[0].minor.yy432 = yylhsminor.yy432;
        break;
      case 248: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy432 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy432), yymsp[-1].minor.yy378, yymsp[0].minor.yy257); }
  yymsp[-2].minor.yy432 = yylhsminor.yy432;
        break;
      case 249: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy378 = ORDER_ASC; }
        break;
      case 250: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy378 = ORDER_ASC; }
        break;
      case 251: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy378 = ORDER_DESC; }
        break;
      case 252: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy257 = NULL_ORDER_DEFAULT; }
        break;
      case 253: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy257 = NULL_ORDER_FIRST; }
        break;
      case 254: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy257 = NULL_ORDER_LAST; }
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
