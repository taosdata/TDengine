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
#define YYNOCODE 216
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  EOrder yy2;
  EJoinType yy36;
  SToken yy209;
  ENullOrder yy217;
  EOperatorType yy236;
  SNodeList* yy280;
  bool yy281;
  SDataType yy304;
  EFillMode yy342;
  SNode* yy344;
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
#define YYNSTATE             308
#define YYNRULE              246
#define YYNTOKEN             138
#define YY_MAX_SHIFT         307
#define YY_MIN_SHIFTREDUCE   475
#define YY_MAX_SHIFTREDUCE   720
#define YY_ERROR_ACTION      721
#define YY_ACCEPT_ACTION     722
#define YY_NO_ACTION         723
#define YY_MIN_REDUCE        724
#define YY_MAX_REDUCE        969
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
#define YY_ACTTAB_COUNT (1037)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   152,  235,  276,  862,  835,  251,  251,  167,  185,  775,
 /*    10 */   838,  835,   31,   29,   27,   26,   25,  837,  835,  208,
 /*    20 */   778,  778,  165,  178,  837,   31,   29,   27,   26,   25,
 /*    30 */   737,   39,    9,    8,  780,   70,  151,  145,   61,  817,
 /*    40 */   250,  815,  773,  235,  251,  862,  235,  248,  862,  164,
 /*    50 */   145,  835,  823,  238,  237,   24,  109,  847,  603,  778,
 /*    60 */    58,  848,  851,  887,  212,  662,   10,  886,  883,  300,
 /*    70 */   299,  298,  297,  296,  295,  294,  293,  292,  291,  290,
 /*    80 */   289,  288,  287,  286,  285,  284,  304,  303,  250,  639,
 /*    90 */    31,   29,   27,   26,   25,   23,  159,  592,  621,  622,
 /*   100 */   623,  624,  625,  626,  627,  629,  630,  631,   23,  159,
 /*   110 */   186,  621,  622,  623,  624,  625,  626,  627,  629,  630,
 /*   120 */   631,   31,   29,   27,   26,   25,  535,  274,  273,  272,
 /*   130 */   539,  271,  541,  542,  270,  544,  267,   20,  550,  264,
 /*   140 */   552,  553,  261,  258,  235,   87,  862,   31,   29,   27,
 /*   150 */    26,   25,  835,   75,  766,  237,  213,  223,  847,   86,
 /*   160 */    82,   56,  848,  851,  887,   54,  948,  603,  144,  883,
 /*   170 */   250,  130,  219,   65,  661,  195,  770,  134,   71,  947,
 /*   180 */   948,  593,   40,  946,  222,   84,  862,   64,   83,  764,
 /*   190 */   186,  583,  835,   81,   39,  237,   10,  946,  847,  581,
 /*   200 */   223,   57,  848,  851,  887,  774,   62,  207,  154,  883,
 /*   210 */    76,  716,  717,   27,   26,   25,  102,  894,  218,  485,
 /*   220 */   217,  166,  105,  948,  817,  283,  815,  189,  200,  914,
 /*   230 */   222,  125,  862,  283,  214,  209,   81,  594,  835,   60,
 /*   240 */   946,  237,  485,  277,  847,  124,  252,   57,  848,  851,
 /*   250 */   887,   82,  486,  487,  154,  883,   76,  251,  226,  235,
 /*   260 */   249,  862,  582,  584,  587,   82,  251,  835,   55,  122,
 /*   270 */   237,  120,  778,  847,  106,  915,   57,  848,  851,  887,
 /*   280 */   169,  778,  205,  154,  883,  960,   99,  235,  251,  862,
 /*   290 */   722,  172,  780,   70,  921,  835,  171,  247,  237,  127,
 /*   300 */   199,  847,  808,  778,   57,  848,  851,  887,  780,   70,
 /*   310 */   595,  154,  883,  960,   48,  104,  235,  188,  862,   21,
 /*   320 */    30,   28,  944,  173,  835,  771,  220,  237,  628,  583,
 /*   330 */   847,  632,  230,   57,  848,  851,  887,  581,  765,  161,
 /*   340 */   154,  883,  960,    6,  901,   12,  948,   30,   28,  663,
 /*   350 */   170,  905,  901,  817,  863,  815,  583,  917,  901,   81,
 /*   360 */   235,  898,  862,  946,  581,    1,  161,  658,  835,  897,
 /*   370 */   234,  237,   12,  223,  847,  896,  670,  133,  848,  851,
 /*   380 */   227,  824,  238,  225,  252,  280,   30,   28,   91,  279,
 /*   390 */   781,   70,    1,  592,  108,  583,  948,   30,   28,  231,
 /*   400 */   582,  584,  587,  581,  281,  161,  583,  174,  817,   81,
 /*   410 */   816,  252,  194,  946,  581,  192,  161,  906,  658,  196,
 /*   420 */    82,    2,   12,  278,  590,  684,   68,  582,  584,  587,
 /*   430 */   618,    7,   59,   96,  633,  235,  763,  862,    9,    8,
 /*   440 */    94,   32,    1,  835,  600,  228,  237,  719,  720,  847,
 /*   450 */   252,   32,   58,  848,  851,  887,   30,   28,  236,  233,
 /*   460 */   883,  252,  842,   30,   28,  583,  582,  584,  587,  840,
 /*   470 */    38,   53,  583,  581,  576,  161,   50,  582,  584,  587,
 /*   480 */   581,   32,  161,  280,  187,   85,   82,  279,   30,   28,
 /*   490 */   190,  184,  235,  596,  862,  183,  590,  583,  150,  182,
 /*   500 */   835,    7,  281,  237,  197,  581,  847,  161,    7,   72,
 /*   510 */   848,  851,  114,  591,  206,  219,  243,  119,  179,  112,
 /*   520 */   252,  278,  528,   66,   67,  181,  180,  252,  523,   68,
 /*   530 */    64,  597,  556,    1,  198,   59,  582,  584,  587,  256,
 /*   540 */   595,  560,  918,  582,  584,  587,  224,  961,   67,   62,
 /*   550 */   565,  928,  252,  241,   92,   69,  175,   68,  221,   77,
 /*   560 */   894,  895,   67,  899,  235,  587,  862,  203,  582,  584,
 /*   570 */   587,   95,  835,  927,  153,  237,    5,   98,  847,  908,
 /*   580 */    74,   58,  848,  851,  887,  100,  235,  687,  862,  884,
 /*   590 */   202,    4,  216,   63,  835,  658,  594,  237,  160,  902,
 /*   600 */   847,  945,   33,  138,  848,  851,  101,  155,  235,  232,
 /*   610 */   862,  204,  685,  686,  688,  689,  835,  963,  229,  237,
 /*   620 */   201,  107,  847,   17,  869,  138,  848,  851,  822,  239,
 /*   630 */   244,  235,  126,  862,  240,   49,  821,   47,  123,  835,
 /*   640 */   307,  254,  237,  245,  116,  847,  163,  128,  137,  848,
 /*   650 */   851,  235,  142,  862,  246,  129,  779,  143,  829,  835,
 /*   660 */   176,  177,  237,  740,  235,  847,  862,  828,   72,  848,
 /*   670 */   851,  827,  835,  494,  738,  237,  158,  191,  847,  215,
 /*   680 */   826,  138,  848,  851,  769,  768,  235,  739,  862,   31,
 /*   690 */    29,   27,   26,   25,  835,   88,  193,  237,  162,  733,
 /*   700 */   847,  728,  767,  138,  848,  851,  962,  732,  731,  727,
 /*   710 */   726,  235,  219,  862,  725,  819,   41,   89,   90,  835,
 /*   720 */     3,   14,  237,   93,  683,  847,   32,   64,  136,  848,
 /*   730 */   851,  235,  210,  862,   36,   73,   97,  211,   42,  835,
 /*   740 */   677,  676,  237,   43,   15,  847,   62,   34,  139,  848,
 /*   750 */   851,  655,  840,  235,   19,  862,   78,  894,  895,   35,
 /*   760 */   899,  835,  654,   16,  237,   11,  705,  847,   44,  103,
 /*   770 */   131,  848,  851,  704,  710,  235,   80,  862,  156,  709,
 /*   780 */   708,  157,    8,  835,  601,  110,  237,  619,   13,  847,
 /*   790 */    18,  113,  140,  848,  851,  818,  111,  681,  235,  115,
 /*   800 */   862,  242,   45,   46,  117,  118,  835,  839,  585,  237,
 /*   810 */    50,  121,  847,   37,  255,  132,  848,  851,  235,  557,
 /*   820 */   862,  253,  168,  257,  534,  554,  835,  259,  260,  237,
 /*   830 */   551,  235,  847,  862,  262,  141,  848,  851,  263,  835,
 /*   840 */   545,  265,  237,  266,  268,  847,  543,  269,  859,  848,
 /*   850 */   851,  549,  548,  235,  547,  862,  546,  275,   51,   52,
 /*   860 */   564,  835,  563,  562,  237,  506,  492,  847,  513,  512,
 /*   870 */   858,  848,  851,  282,  511,  510,  509,  508,  235,  507,
 /*   880 */   862,  505,  504,  503,  502,  501,  835,  500,  499,  237,
 /*   890 */   498,  497,  847,  730,  301,  857,  848,  851,  235,  302,
 /*   900 */   862,  729,  724,  305,  306,  723,  835,  723,  723,  237,
 /*   910 */   723,  723,  847,  723,  723,  148,  848,  851,  723,  723,
 /*   920 */   235,  723,  862,  723,  723,  723,  723,  723,  835,  723,
 /*   930 */   723,  237,  723,  723,  847,  723,  723,  147,  848,  851,
 /*   940 */   723,  723,  235,  723,  862,  723,  723,  723,  723,  723,
 /*   950 */   835,  723,  723,  237,  723,  723,  847,  723,  723,  149,
 /*   960 */   848,  851,  723,  723,  723,  235,  723,  862,  723,  723,
 /*   970 */   723,  723,  723,  835,  723,  219,  237,  723,  723,  847,
 /*   980 */   723,  723,  146,  848,  851,  235,  723,  862,  723,  723,
 /*   990 */    64,  723,  723,  835,  723,  723,  237,  723,  723,  847,
 /*  1000 */   723,  723,  135,  848,  851,  723,  723,  723,  723,   62,
 /*  1010 */   723,  723,  723,  723,  723,  723,  723,  723,  723,   79,
 /*  1020 */   894,  895,   22,  899,  723,  723,  723,  723,  723,  723,
 /*  1030 */   723,  723,   31,   29,   27,   26,   25,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   155,  158,  161,  160,  166,  143,  143,  155,  146,  146,
 /*    10 */   172,  166,   12,   13,   14,   15,   16,  172,  166,  176,
 /*    20 */   158,  158,  147,  143,  172,   12,   13,   14,   15,   16,
 /*    30 */     0,  145,    1,    2,  159,  160,  157,   37,  152,  160,
 /*    40 */    31,  162,  156,  158,  143,  160,  158,  146,  160,  165,
 /*    50 */    37,  166,  168,  169,  169,  179,  180,  172,   58,  158,
 /*    60 */   175,  176,  177,  178,  176,    4,   57,  182,  183,   39,
 /*    70 */    40,   41,   42,   43,   44,   45,   46,   47,   48,   49,
 /*    80 */    50,   51,   52,   53,   54,   55,  140,  141,   31,   58,
 /*    90 */    12,   13,   14,   15,   16,   95,   96,   31,   98,   99,
 /*   100 */   100,  101,  102,  103,  104,  105,  106,  107,   95,   96,
 /*   110 */    36,   98,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   120 */   107,   12,   13,   14,   15,   16,   67,   68,   69,   70,
 /*   130 */    71,   72,   73,   74,   75,   76,   77,    2,   79,   80,
 /*   140 */    81,   82,   83,   84,  158,   19,  160,   12,   13,   14,
 /*   150 */    15,   16,  166,   27,    0,  169,   31,  171,  172,   33,
 /*   160 */   112,  175,  176,  177,  178,  142,  194,   58,  182,  183,
 /*   170 */    31,   18,  143,  150,  113,   22,  153,   24,   25,  207,
 /*   180 */   194,   31,   56,  211,  158,   59,  160,  158,   35,    0,
 /*   190 */    36,   21,  166,  207,  145,  169,   57,  211,  172,   29,
 /*   200 */   171,  175,  176,  177,  178,  156,  177,   90,  182,  183,
 /*   210 */   184,  133,  134,   14,   15,   16,  187,  188,  189,   21,
 /*   220 */   191,  157,  196,  194,  160,   36,  162,   29,  202,  203,
 /*   230 */   158,   19,  160,   36,  117,  118,  207,   31,  166,   27,
 /*   240 */   211,  169,   21,   63,  172,   33,   76,  175,  176,  177,
 /*   250 */   178,  112,   31,   32,  182,  183,  184,  143,    3,  158,
 /*   260 */   146,  160,   92,   93,   94,  112,  143,  166,   56,  146,
 /*   270 */   169,   59,  158,  172,  214,  203,  175,  176,  177,  178,
 /*   280 */   147,  158,  205,  182,  183,  184,  199,  158,  143,  160,
 /*   290 */   138,  146,  159,  160,  193,  166,  147,   85,  169,  148,
 /*   300 */    88,  172,  151,  158,  175,  176,  177,  178,  159,  160,
 /*   310 */    31,  182,  183,  184,  142,  109,  158,  140,  160,   95,
 /*   320 */    12,   13,  193,  171,  166,  153,  192,  169,  104,   21,
 /*   330 */   172,  107,   65,  175,  176,  177,  178,   29,    0,   31,
 /*   340 */   182,  183,  184,   87,  173,   37,  194,   12,   13,   14,
 /*   350 */   157,  193,  173,  160,  160,  162,   21,  174,  173,  207,
 /*   360 */   158,  190,  160,  211,   29,   57,   31,  111,  166,  190,
 /*   370 */    37,  169,   37,  171,  172,  190,   14,  175,  176,  177,
 /*   380 */    65,  168,  169,  128,   76,   47,   12,   13,  109,   51,
 /*   390 */   159,  160,   57,   31,  208,   21,  194,   12,   13,  132,
 /*   400 */    92,   93,   94,   29,   66,   31,   21,  143,  160,  207,
 /*   410 */   162,   76,   20,  211,   29,   23,   31,  110,  111,   58,
 /*   420 */   112,  195,   37,   85,   31,   58,   65,   92,   93,   94,
 /*   430 */    97,   57,   65,   58,   58,  158,    0,  160,    1,    2,
 /*   440 */    65,   65,   57,  166,   58,  130,  169,  136,  137,  172,
 /*   450 */    76,   65,  175,  176,  177,  178,   12,   13,   14,  182,
 /*   460 */   183,   76,   57,   12,   13,   21,   92,   93,   94,   64,
 /*   470 */   145,   57,   21,   29,   58,   31,   62,   92,   93,   94,
 /*   480 */    29,   65,   31,   47,  143,  145,  112,   51,   12,   13,
 /*   490 */   139,   26,  158,   31,  160,   30,   31,   21,  139,   34,
 /*   500 */   166,   57,   66,  169,  158,   29,  172,   31,   57,  175,
 /*   510 */   176,  177,   58,   31,  120,  143,   58,   58,   53,   65,
 /*   520 */    76,   85,   58,   65,   65,   60,   61,   76,   58,   65,
 /*   530 */   158,   31,   58,   57,  163,   65,   92,   93,   94,   65,
 /*   540 */    31,   58,  174,   92,   93,   94,  212,  213,   65,  177,
 /*   550 */    58,  204,   76,  119,  167,   58,   91,   65,  186,  187,
 /*   560 */   188,  189,   65,  191,  158,   94,  160,  166,   92,   93,
 /*   570 */    94,  167,  166,  204,  166,  169,  127,  200,  172,  201,
 /*   580 */   198,  175,  176,  177,  178,  197,  158,   97,  160,  183,
 /*   590 */   115,  114,  126,  158,  166,  111,   31,  169,  170,  173,
 /*   600 */   172,  210,  108,  175,  176,  177,  185,  135,  158,  131,
 /*   610 */   160,  121,  122,  123,  124,  125,  166,  215,  129,  169,
 /*   620 */   170,  209,  172,   57,  181,  175,  176,  177,  167,  166,
 /*   630 */    89,  158,  151,  160,  166,   57,  167,  142,  142,  166,
 /*   640 */   139,  154,  169,  164,  158,  172,  166,  143,  175,  176,
 /*   650 */   177,  158,  149,  160,  163,  144,  158,  149,    0,  166,
 /*   660 */    53,   64,  169,    0,  158,  172,  160,    0,  175,  176,
 /*   670 */   177,    0,  166,   38,    0,  169,  170,   21,  172,  206,
 /*   680 */     0,  175,  176,  177,    0,    0,  158,    0,  160,   12,
 /*   690 */    13,   14,   15,   16,  166,   19,   21,  169,  170,    0,
 /*   700 */   172,    0,    0,  175,  176,  177,  213,    0,    0,    0,
 /*   710 */     0,  158,  143,  160,    0,    0,   57,   87,   86,  166,
 /*   720 */    65,  116,  169,   58,   58,  172,   65,  158,  175,  176,
 /*   730 */   177,  158,   29,  160,   65,   57,   57,   65,   57,  166,
 /*   740 */    58,   58,  169,   57,  116,  172,  177,  110,  175,  176,
 /*   750 */   177,   58,   64,  158,   65,  160,  187,  188,  189,   65,
 /*   760 */   191,  166,   58,   65,  169,  116,   29,  172,    4,   64,
 /*   770 */   175,  176,  177,   29,   58,  158,   64,  160,   29,   29,
 /*   780 */    29,   29,    2,  166,   58,   64,  169,   97,   57,  172,
 /*   790 */    57,   57,  175,  176,  177,    0,   58,   58,  158,   57,
 /*   800 */   160,   90,   57,   57,   87,   86,  166,   64,   21,  169,
 /*   810 */    62,   64,  172,   57,   29,  175,  176,  177,  158,   58,
 /*   820 */   160,   63,   29,   57,   21,   58,  166,   29,   57,  169,
 /*   830 */    58,  158,  172,  160,   29,  175,  176,  177,   57,  166,
 /*   840 */    58,   29,  169,   57,   29,  172,   58,   57,  175,  176,
 /*   850 */   177,   78,   78,  158,   78,  160,   78,   66,   57,   57,
 /*   860 */    29,  166,   29,   21,  169,   21,   38,  172,   29,   29,
 /*   870 */   175,  176,  177,   37,   29,   29,   29,   29,  158,   29,
 /*   880 */   160,   29,   29,   29,   29,   29,  166,   29,   29,  169,
 /*   890 */    29,   29,  172,    0,   29,  175,  176,  177,  158,   28,
 /*   900 */   160,    0,    0,   21,   20,  216,  166,  216,  216,  169,
 /*   910 */   216,  216,  172,  216,  216,  175,  176,  177,  216,  216,
 /*   920 */   158,  216,  160,  216,  216,  216,  216,  216,  166,  216,
 /*   930 */   216,  169,  216,  216,  172,  216,  216,  175,  176,  177,
 /*   940 */   216,  216,  158,  216,  160,  216,  216,  216,  216,  216,
 /*   950 */   166,  216,  216,  169,  216,  216,  172,  216,  216,  175,
 /*   960 */   176,  177,  216,  216,  216,  158,  216,  160,  216,  216,
 /*   970 */   216,  216,  216,  166,  216,  143,  169,  216,  216,  172,
 /*   980 */   216,  216,  175,  176,  177,  158,  216,  160,  216,  216,
 /*   990 */   158,  216,  216,  166,  216,  216,  169,  216,  216,  172,
 /*  1000 */   216,  216,  175,  176,  177,  216,  216,  216,  216,  177,
 /*  1010 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  187,
 /*  1020 */   188,  189,    2,  191,  216,  216,  216,  216,  216,  216,
 /*  1030 */   216,  216,   12,   13,   14,   15,   16,  216,  216,  216,
 /*  1040 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1050 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1060 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1070 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1080 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1090 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1100 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1110 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1120 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1130 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1140 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1150 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*  1160 */   216,
};
#define YY_SHIFT_COUNT    (307)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1020)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   153,  308,  335,  385,  385,  385,  385,  374,  385,  385,
 /*    10 */   139,  451,  476,  444,  451,  451,  451,  451,  451,  451,
 /*    20 */   451,  451,  451,  451,  451,  451,  451,  451,  451,  451,
 /*    30 */   451,  451,  451,    9,    9,    9,  170,  170,   57,   57,
 /*    40 */    74,   66,  125,  125,   48,  150,   66,   57,   57,   66,
 /*    50 */    57,   66,   66,   66,   57,  197,    0,   13,   13,  170,
 /*    60 */   221,  154,  206,  206,  206,  189,  150,   66,   66,  180,
 /*    70 */    59,  465,   78,  490,  117,  198,  279,  307,  256,  307,
 /*    80 */   362,  255,   61,  393,   74,  393,   74,  462,  462,  482,
 /*    90 */   500,  509,  394,  434,  471,  394,  434,  471,  449,  466,
 /*   100 */   475,  477,  484,  482,  565,  494,  472,  478,  489,  566,
 /*   110 */    66,  434,  471,  471,  434,  471,  541,  482,  500,  180,
 /*   120 */   197,  482,  578,  393,  197,  462, 1037, 1037, 1037,   30,
 /*   130 */   212,  135, 1020,  109,  126,  677,  677,  677,  677,  677,
 /*   140 */   677,  677,  338,  436,   31,  224,  199,  199,  199,  199,
 /*   150 */   392,  361,  367,  375,  437,  311,  315,  267,  376,  333,
 /*   160 */   386,  405,  416,  454,  458,  459,  464,  470,  474,  483,
 /*   170 */   492,  497,  414,  658,  663,  667,  671,  607,  597,  680,
 /*   180 */   684,  685,  687,  699,  701,  702,  635,  674,  707,  708,
 /*   190 */   709,  710,  656,  714,  675,  676,  715,  659,  630,  632,
 /*   200 */   655,  661,  605,  665,  669,  666,  678,  679,  682,  681,
 /*   210 */   683,  703,  672,  688,  686,  689,  628,  693,  704,  705,
 /*   220 */   637,  694,  712,  716,  698,  649,  764,  737,  744,  749,
 /*   230 */   750,  751,  752,  780,  690,  721,  726,  731,  733,  738,
 /*   240 */   739,  734,  742,  711,  745,  795,  717,  719,  746,  748,
 /*   250 */   743,  747,  787,  756,  758,  761,  785,  793,  766,  767,
 /*   260 */   798,  771,  772,  805,  781,  782,  812,  786,  788,  815,
 /*   270 */   790,  773,  774,  776,  778,  803,  791,  801,  802,  831,
 /*   280 */   833,  842,  828,  836,  839,  840,  845,  846,  847,  848,
 /*   290 */   850,  844,  852,  853,  854,  855,  856,  858,  859,  861,
 /*   300 */   862,  893,  865,  871,  901,  902,  882,  884,
};
#define YY_REDUCE_COUNT (128)
#define YY_REDUCE_MIN   (-162)
#define YY_REDUCE_MAX   (832)
static const short yy_reduce_ofst[] = {
 /*     0 */   152,  -14,   26,   72,  101,  129,  158,  202, -115,  277,
 /*    10 */    29,  334,  406,  428,  450,  473,  493,  506,  528,  553,
 /*    20 */   573,  595,  617,  640,  660,  673,  695,  720,  740,  762,
 /*    30 */   784,  807,  827,  372,  569,  832, -155, -148, -138, -137,
 /*    40 */  -114, -121, -157, -112,  -28, -116, -125,  -99,  114,   64,
 /*    50 */   123,  133,  193,  149,  145,   23, -124, -124, -124, -162,
 /*    60 */   -54,   49,  171,  179,  185,  172,  213,  231,  248,  151,
 /*    70 */  -159, -120,   60,   77,   87,  177,  183,  134,  134,  134,
 /*    80 */   194,  186,  226,  264,  325,  341,  340,  351,  359,  346,
 /*    90 */   371,  368,  347,  387,  401,  369,  404,  408,  378,  377,
 /*   100 */   382,  388,  134,  435,  426,  421,  402,  391,  412,  443,
 /*   110 */   194,  461,  463,  468,  469,  480,  479,  486,  491,  481,
 /*   120 */   495,  498,  487,  504,  496,  501,  503,  508,  511,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*    10 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*    20 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*    30 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*    40 */   744,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*    50 */   721,  721,  721,  721,  721,  742,  721,  889,  721,  721,
 /*    60 */   721,  744,  900,  900,  900,  742,  721,  721,  721,  807,
 /*    70 */   721,  721,  964,  721,  924,  721,  916,  892,  906,  893,
 /*    80 */   721,  949,  909,  721,  744,  721,  744,  721,  721,  721,
 /*    90 */   721,  721,  931,  929,  721,  931,  929,  721,  943,  939,
 /*   100 */   922,  920,  906,  721,  721,  721,  967,  955,  951,  721,
 /*   110 */   721,  929,  721,  721,  929,  721,  820,  721,  721,  721,
 /*   120 */   742,  721,  776,  721,  742,  721,  810,  810,  745,  721,
 /*   130 */   721,  721,  721,  721,  721,  861,  942,  941,  860,  866,
 /*   140 */   865,  864,  721,  721,  721,  721,  855,  856,  854,  853,
 /*   150 */   721,  721,  721,  721,  890,  721,  952,  956,  721,  721,
 /*   160 */   721,  841,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   170 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   180 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   190 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   200 */   913,  923,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   210 */   721,  721,  721,  841,  721,  940,  721,  899,  895,  721,
 /*   220 */   721,  891,  721,  721,  950,  721,  721,  721,  721,  721,
 /*   230 */   721,  721,  721,  885,  721,  721,  721,  721,  721,  721,
 /*   240 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   250 */   840,  721,  721,  721,  721,  721,  721,  721,  804,  721,
 /*   260 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   270 */   721,  789,  787,  786,  785,  721,  782,  721,  721,  721,
 /*   280 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   290 */   721,  721,  721,  721,  721,  721,  721,  721,  721,  721,
 /*   300 */   721,  721,  721,  721,  721,  721,  721,  721,
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
  /*   86 */ "INDEX",
  /*   87 */ "ON",
  /*   88 */ "FULLTEXT",
  /*   89 */ "FUNCTION",
  /*   90 */ "INTERVAL",
  /*   91 */ "MNODES",
  /*   92 */ "NK_FLOAT",
  /*   93 */ "NK_BOOL",
  /*   94 */ "NK_VARIABLE",
  /*   95 */ "BETWEEN",
  /*   96 */ "IS",
  /*   97 */ "NULL",
  /*   98 */ "NK_LT",
  /*   99 */ "NK_GT",
  /*  100 */ "NK_LE",
  /*  101 */ "NK_GE",
  /*  102 */ "NK_NE",
  /*  103 */ "NK_EQ",
  /*  104 */ "LIKE",
  /*  105 */ "MATCH",
  /*  106 */ "NMATCH",
  /*  107 */ "IN",
  /*  108 */ "FROM",
  /*  109 */ "AS",
  /*  110 */ "JOIN",
  /*  111 */ "INNER",
  /*  112 */ "SELECT",
  /*  113 */ "DISTINCT",
  /*  114 */ "WHERE",
  /*  115 */ "PARTITION",
  /*  116 */ "BY",
  /*  117 */ "SESSION",
  /*  118 */ "STATE_WINDOW",
  /*  119 */ "SLIDING",
  /*  120 */ "FILL",
  /*  121 */ "VALUE",
  /*  122 */ "NONE",
  /*  123 */ "PREV",
  /*  124 */ "LINEAR",
  /*  125 */ "NEXT",
  /*  126 */ "GROUP",
  /*  127 */ "HAVING",
  /*  128 */ "ORDER",
  /*  129 */ "SLIMIT",
  /*  130 */ "SOFFSET",
  /*  131 */ "LIMIT",
  /*  132 */ "OFFSET",
  /*  133 */ "ASC",
  /*  134 */ "DESC",
  /*  135 */ "NULLS",
  /*  136 */ "FIRST",
  /*  137 */ "LAST",
  /*  138 */ "cmd",
  /*  139 */ "user_name",
  /*  140 */ "dnode_endpoint",
  /*  141 */ "dnode_host_name",
  /*  142 */ "not_exists_opt",
  /*  143 */ "db_name",
  /*  144 */ "db_options",
  /*  145 */ "exists_opt",
  /*  146 */ "full_table_name",
  /*  147 */ "column_def_list",
  /*  148 */ "tags_def_opt",
  /*  149 */ "table_options",
  /*  150 */ "multi_create_clause",
  /*  151 */ "tags_def",
  /*  152 */ "multi_drop_clause",
  /*  153 */ "create_subtable_clause",
  /*  154 */ "specific_tags_opt",
  /*  155 */ "literal_list",
  /*  156 */ "drop_table_clause",
  /*  157 */ "col_name_list",
  /*  158 */ "table_name",
  /*  159 */ "column_def",
  /*  160 */ "column_name",
  /*  161 */ "type_name",
  /*  162 */ "col_name",
  /*  163 */ "index_name",
  /*  164 */ "index_options",
  /*  165 */ "func_list",
  /*  166 */ "duration_literal",
  /*  167 */ "sliding_opt",
  /*  168 */ "func",
  /*  169 */ "function_name",
  /*  170 */ "expression_list",
  /*  171 */ "query_expression",
  /*  172 */ "literal",
  /*  173 */ "table_alias",
  /*  174 */ "column_alias",
  /*  175 */ "expression",
  /*  176 */ "column_reference",
  /*  177 */ "subquery",
  /*  178 */ "predicate",
  /*  179 */ "compare_op",
  /*  180 */ "in_op",
  /*  181 */ "in_predicate_value",
  /*  182 */ "boolean_value_expression",
  /*  183 */ "boolean_primary",
  /*  184 */ "common_expression",
  /*  185 */ "from_clause",
  /*  186 */ "table_reference_list",
  /*  187 */ "table_reference",
  /*  188 */ "table_primary",
  /*  189 */ "joined_table",
  /*  190 */ "alias_opt",
  /*  191 */ "parenthesized_joined_table",
  /*  192 */ "join_type",
  /*  193 */ "search_condition",
  /*  194 */ "query_specification",
  /*  195 */ "set_quantifier_opt",
  /*  196 */ "select_list",
  /*  197 */ "where_clause_opt",
  /*  198 */ "partition_by_clause_opt",
  /*  199 */ "twindow_clause_opt",
  /*  200 */ "group_by_clause_opt",
  /*  201 */ "having_clause_opt",
  /*  202 */ "select_sublist",
  /*  203 */ "select_item",
  /*  204 */ "fill_opt",
  /*  205 */ "fill_mode",
  /*  206 */ "group_by_list",
  /*  207 */ "query_expression_body",
  /*  208 */ "order_by_clause_opt",
  /*  209 */ "slimit_clause_opt",
  /*  210 */ "limit_clause_opt",
  /*  211 */ "query_primary",
  /*  212 */ "sort_specification_list",
  /*  213 */ "sort_specification",
  /*  214 */ "ordering_specification_opt",
  /*  215 */ "null_ordering_opt",
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
 /*  94 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /*  95 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /*  96 */ "index_options ::=",
 /*  97 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /*  98 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /*  99 */ "func_list ::= func",
 /* 100 */ "func_list ::= func_list NK_COMMA func",
 /* 101 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 102 */ "cmd ::= SHOW VGROUPS",
 /* 103 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 104 */ "cmd ::= SHOW MNODES",
 /* 105 */ "cmd ::= query_expression",
 /* 106 */ "literal ::= NK_INTEGER",
 /* 107 */ "literal ::= NK_FLOAT",
 /* 108 */ "literal ::= NK_STRING",
 /* 109 */ "literal ::= NK_BOOL",
 /* 110 */ "literal ::= TIMESTAMP NK_STRING",
 /* 111 */ "literal ::= duration_literal",
 /* 112 */ "duration_literal ::= NK_VARIABLE",
 /* 113 */ "literal_list ::= literal",
 /* 114 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 115 */ "db_name ::= NK_ID",
 /* 116 */ "table_name ::= NK_ID",
 /* 117 */ "column_name ::= NK_ID",
 /* 118 */ "function_name ::= NK_ID",
 /* 119 */ "table_alias ::= NK_ID",
 /* 120 */ "column_alias ::= NK_ID",
 /* 121 */ "user_name ::= NK_ID",
 /* 122 */ "index_name ::= NK_ID",
 /* 123 */ "expression ::= literal",
 /* 124 */ "expression ::= column_reference",
 /* 125 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 126 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 127 */ "expression ::= subquery",
 /* 128 */ "expression ::= NK_LP expression NK_RP",
 /* 129 */ "expression ::= NK_PLUS expression",
 /* 130 */ "expression ::= NK_MINUS expression",
 /* 131 */ "expression ::= expression NK_PLUS expression",
 /* 132 */ "expression ::= expression NK_MINUS expression",
 /* 133 */ "expression ::= expression NK_STAR expression",
 /* 134 */ "expression ::= expression NK_SLASH expression",
 /* 135 */ "expression ::= expression NK_REM expression",
 /* 136 */ "expression_list ::= expression",
 /* 137 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 138 */ "column_reference ::= column_name",
 /* 139 */ "column_reference ::= table_name NK_DOT column_name",
 /* 140 */ "predicate ::= expression compare_op expression",
 /* 141 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 142 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 143 */ "predicate ::= expression IS NULL",
 /* 144 */ "predicate ::= expression IS NOT NULL",
 /* 145 */ "predicate ::= expression in_op in_predicate_value",
 /* 146 */ "compare_op ::= NK_LT",
 /* 147 */ "compare_op ::= NK_GT",
 /* 148 */ "compare_op ::= NK_LE",
 /* 149 */ "compare_op ::= NK_GE",
 /* 150 */ "compare_op ::= NK_NE",
 /* 151 */ "compare_op ::= NK_EQ",
 /* 152 */ "compare_op ::= LIKE",
 /* 153 */ "compare_op ::= NOT LIKE",
 /* 154 */ "compare_op ::= MATCH",
 /* 155 */ "compare_op ::= NMATCH",
 /* 156 */ "in_op ::= IN",
 /* 157 */ "in_op ::= NOT IN",
 /* 158 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 159 */ "boolean_value_expression ::= boolean_primary",
 /* 160 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 161 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 162 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 163 */ "boolean_primary ::= predicate",
 /* 164 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 165 */ "common_expression ::= expression",
 /* 166 */ "common_expression ::= boolean_value_expression",
 /* 167 */ "from_clause ::= FROM table_reference_list",
 /* 168 */ "table_reference_list ::= table_reference",
 /* 169 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 170 */ "table_reference ::= table_primary",
 /* 171 */ "table_reference ::= joined_table",
 /* 172 */ "table_primary ::= table_name alias_opt",
 /* 173 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 174 */ "table_primary ::= subquery alias_opt",
 /* 175 */ "table_primary ::= parenthesized_joined_table",
 /* 176 */ "alias_opt ::=",
 /* 177 */ "alias_opt ::= table_alias",
 /* 178 */ "alias_opt ::= AS table_alias",
 /* 179 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 180 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 181 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 182 */ "join_type ::=",
 /* 183 */ "join_type ::= INNER",
 /* 184 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 185 */ "set_quantifier_opt ::=",
 /* 186 */ "set_quantifier_opt ::= DISTINCT",
 /* 187 */ "set_quantifier_opt ::= ALL",
 /* 188 */ "select_list ::= NK_STAR",
 /* 189 */ "select_list ::= select_sublist",
 /* 190 */ "select_sublist ::= select_item",
 /* 191 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 192 */ "select_item ::= common_expression",
 /* 193 */ "select_item ::= common_expression column_alias",
 /* 194 */ "select_item ::= common_expression AS column_alias",
 /* 195 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 196 */ "where_clause_opt ::=",
 /* 197 */ "where_clause_opt ::= WHERE search_condition",
 /* 198 */ "partition_by_clause_opt ::=",
 /* 199 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 200 */ "twindow_clause_opt ::=",
 /* 201 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 202 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 203 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 204 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 205 */ "sliding_opt ::=",
 /* 206 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 207 */ "fill_opt ::=",
 /* 208 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 209 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 210 */ "fill_mode ::= NONE",
 /* 211 */ "fill_mode ::= PREV",
 /* 212 */ "fill_mode ::= NULL",
 /* 213 */ "fill_mode ::= LINEAR",
 /* 214 */ "fill_mode ::= NEXT",
 /* 215 */ "group_by_clause_opt ::=",
 /* 216 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 217 */ "group_by_list ::= expression",
 /* 218 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 219 */ "having_clause_opt ::=",
 /* 220 */ "having_clause_opt ::= HAVING search_condition",
 /* 221 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 222 */ "query_expression_body ::= query_primary",
 /* 223 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 224 */ "query_primary ::= query_specification",
 /* 225 */ "order_by_clause_opt ::=",
 /* 226 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 227 */ "slimit_clause_opt ::=",
 /* 228 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 229 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 230 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 231 */ "limit_clause_opt ::=",
 /* 232 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 233 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 234 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 235 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 236 */ "search_condition ::= common_expression",
 /* 237 */ "sort_specification_list ::= sort_specification",
 /* 238 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 239 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 240 */ "ordering_specification_opt ::=",
 /* 241 */ "ordering_specification_opt ::= ASC",
 /* 242 */ "ordering_specification_opt ::= DESC",
 /* 243 */ "null_ordering_opt ::=",
 /* 244 */ "null_ordering_opt ::= NULLS FIRST",
 /* 245 */ "null_ordering_opt ::= NULLS LAST",
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
    case 138: /* cmd */
    case 144: /* db_options */
    case 146: /* full_table_name */
    case 149: /* table_options */
    case 153: /* create_subtable_clause */
    case 156: /* drop_table_clause */
    case 159: /* column_def */
    case 162: /* col_name */
    case 164: /* index_options */
    case 166: /* duration_literal */
    case 167: /* sliding_opt */
    case 168: /* func */
    case 171: /* query_expression */
    case 172: /* literal */
    case 175: /* expression */
    case 176: /* column_reference */
    case 177: /* subquery */
    case 178: /* predicate */
    case 181: /* in_predicate_value */
    case 182: /* boolean_value_expression */
    case 183: /* boolean_primary */
    case 184: /* common_expression */
    case 185: /* from_clause */
    case 186: /* table_reference_list */
    case 187: /* table_reference */
    case 188: /* table_primary */
    case 189: /* joined_table */
    case 191: /* parenthesized_joined_table */
    case 193: /* search_condition */
    case 194: /* query_specification */
    case 197: /* where_clause_opt */
    case 199: /* twindow_clause_opt */
    case 201: /* having_clause_opt */
    case 203: /* select_item */
    case 204: /* fill_opt */
    case 207: /* query_expression_body */
    case 209: /* slimit_clause_opt */
    case 210: /* limit_clause_opt */
    case 211: /* query_primary */
    case 213: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy344)); 
}
      break;
    case 139: /* user_name */
    case 140: /* dnode_endpoint */
    case 141: /* dnode_host_name */
    case 143: /* db_name */
    case 158: /* table_name */
    case 160: /* column_name */
    case 163: /* index_name */
    case 169: /* function_name */
    case 173: /* table_alias */
    case 174: /* column_alias */
    case 190: /* alias_opt */
{
 
}
      break;
    case 142: /* not_exists_opt */
    case 145: /* exists_opt */
    case 195: /* set_quantifier_opt */
{
 
}
      break;
    case 147: /* column_def_list */
    case 148: /* tags_def_opt */
    case 150: /* multi_create_clause */
    case 151: /* tags_def */
    case 152: /* multi_drop_clause */
    case 154: /* specific_tags_opt */
    case 155: /* literal_list */
    case 157: /* col_name_list */
    case 165: /* func_list */
    case 170: /* expression_list */
    case 196: /* select_list */
    case 198: /* partition_by_clause_opt */
    case 200: /* group_by_clause_opt */
    case 202: /* select_sublist */
    case 206: /* group_by_list */
    case 208: /* order_by_clause_opt */
    case 212: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy280)); 
}
      break;
    case 161: /* type_name */
{
 
}
      break;
    case 179: /* compare_op */
    case 180: /* in_op */
{
 
}
      break;
    case 192: /* join_type */
{
 
}
      break;
    case 205: /* fill_mode */
{
 
}
      break;
    case 214: /* ordering_specification_opt */
{
 
}
      break;
    case 215: /* null_ordering_opt */
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
  {  138,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  138,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  138,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  138,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  138,   -2 }, /* (4) cmd ::= SHOW USERS */
  {  138,   -3 }, /* (5) cmd ::= CREATE DNODE dnode_endpoint */
  {  138,   -5 }, /* (6) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  138,   -3 }, /* (7) cmd ::= DROP DNODE NK_INTEGER */
  {  138,   -3 }, /* (8) cmd ::= DROP DNODE dnode_endpoint */
  {  138,   -2 }, /* (9) cmd ::= SHOW DNODES */
  {  140,   -1 }, /* (10) dnode_endpoint ::= NK_STRING */
  {  141,   -1 }, /* (11) dnode_host_name ::= NK_ID */
  {  141,   -1 }, /* (12) dnode_host_name ::= NK_IPTOKEN */
  {  138,   -5 }, /* (13) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  138,   -4 }, /* (14) cmd ::= DROP DATABASE exists_opt db_name */
  {  138,   -2 }, /* (15) cmd ::= SHOW DATABASES */
  {  138,   -2 }, /* (16) cmd ::= USE db_name */
  {  142,   -3 }, /* (17) not_exists_opt ::= IF NOT EXISTS */
  {  142,    0 }, /* (18) not_exists_opt ::= */
  {  145,   -2 }, /* (19) exists_opt ::= IF EXISTS */
  {  145,    0 }, /* (20) exists_opt ::= */
  {  144,    0 }, /* (21) db_options ::= */
  {  144,   -3 }, /* (22) db_options ::= db_options BLOCKS NK_INTEGER */
  {  144,   -3 }, /* (23) db_options ::= db_options CACHE NK_INTEGER */
  {  144,   -3 }, /* (24) db_options ::= db_options CACHELAST NK_INTEGER */
  {  144,   -3 }, /* (25) db_options ::= db_options COMP NK_INTEGER */
  {  144,   -3 }, /* (26) db_options ::= db_options DAYS NK_INTEGER */
  {  144,   -3 }, /* (27) db_options ::= db_options FSYNC NK_INTEGER */
  {  144,   -3 }, /* (28) db_options ::= db_options MAXROWS NK_INTEGER */
  {  144,   -3 }, /* (29) db_options ::= db_options MINROWS NK_INTEGER */
  {  144,   -3 }, /* (30) db_options ::= db_options KEEP NK_INTEGER */
  {  144,   -3 }, /* (31) db_options ::= db_options PRECISION NK_STRING */
  {  144,   -3 }, /* (32) db_options ::= db_options QUORUM NK_INTEGER */
  {  144,   -3 }, /* (33) db_options ::= db_options REPLICA NK_INTEGER */
  {  144,   -3 }, /* (34) db_options ::= db_options TTL NK_INTEGER */
  {  144,   -3 }, /* (35) db_options ::= db_options WAL NK_INTEGER */
  {  144,   -3 }, /* (36) db_options ::= db_options VGROUPS NK_INTEGER */
  {  144,   -3 }, /* (37) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  144,   -3 }, /* (38) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  138,   -9 }, /* (39) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  138,   -3 }, /* (40) cmd ::= CREATE TABLE multi_create_clause */
  {  138,   -9 }, /* (41) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  138,   -3 }, /* (42) cmd ::= DROP TABLE multi_drop_clause */
  {  138,   -4 }, /* (43) cmd ::= DROP STABLE exists_opt full_table_name */
  {  138,   -2 }, /* (44) cmd ::= SHOW TABLES */
  {  138,   -2 }, /* (45) cmd ::= SHOW STABLES */
  {  150,   -1 }, /* (46) multi_create_clause ::= create_subtable_clause */
  {  150,   -2 }, /* (47) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  153,   -9 }, /* (48) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  152,   -1 }, /* (49) multi_drop_clause ::= drop_table_clause */
  {  152,   -2 }, /* (50) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  156,   -2 }, /* (51) drop_table_clause ::= exists_opt full_table_name */
  {  154,    0 }, /* (52) specific_tags_opt ::= */
  {  154,   -3 }, /* (53) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  146,   -1 }, /* (54) full_table_name ::= table_name */
  {  146,   -3 }, /* (55) full_table_name ::= db_name NK_DOT table_name */
  {  147,   -1 }, /* (56) column_def_list ::= column_def */
  {  147,   -3 }, /* (57) column_def_list ::= column_def_list NK_COMMA column_def */
  {  159,   -2 }, /* (58) column_def ::= column_name type_name */
  {  159,   -4 }, /* (59) column_def ::= column_name type_name COMMENT NK_STRING */
  {  161,   -1 }, /* (60) type_name ::= BOOL */
  {  161,   -1 }, /* (61) type_name ::= TINYINT */
  {  161,   -1 }, /* (62) type_name ::= SMALLINT */
  {  161,   -1 }, /* (63) type_name ::= INT */
  {  161,   -1 }, /* (64) type_name ::= INTEGER */
  {  161,   -1 }, /* (65) type_name ::= BIGINT */
  {  161,   -1 }, /* (66) type_name ::= FLOAT */
  {  161,   -1 }, /* (67) type_name ::= DOUBLE */
  {  161,   -4 }, /* (68) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  161,   -1 }, /* (69) type_name ::= TIMESTAMP */
  {  161,   -4 }, /* (70) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  161,   -2 }, /* (71) type_name ::= TINYINT UNSIGNED */
  {  161,   -2 }, /* (72) type_name ::= SMALLINT UNSIGNED */
  {  161,   -2 }, /* (73) type_name ::= INT UNSIGNED */
  {  161,   -2 }, /* (74) type_name ::= BIGINT UNSIGNED */
  {  161,   -1 }, /* (75) type_name ::= JSON */
  {  161,   -4 }, /* (76) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  161,   -1 }, /* (77) type_name ::= MEDIUMBLOB */
  {  161,   -1 }, /* (78) type_name ::= BLOB */
  {  161,   -4 }, /* (79) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  161,   -1 }, /* (80) type_name ::= DECIMAL */
  {  161,   -4 }, /* (81) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  161,   -6 }, /* (82) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  148,    0 }, /* (83) tags_def_opt ::= */
  {  148,   -1 }, /* (84) tags_def_opt ::= tags_def */
  {  151,   -4 }, /* (85) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  149,    0 }, /* (86) table_options ::= */
  {  149,   -3 }, /* (87) table_options ::= table_options COMMENT NK_STRING */
  {  149,   -3 }, /* (88) table_options ::= table_options KEEP NK_INTEGER */
  {  149,   -3 }, /* (89) table_options ::= table_options TTL NK_INTEGER */
  {  149,   -5 }, /* (90) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  157,   -1 }, /* (91) col_name_list ::= col_name */
  {  157,   -3 }, /* (92) col_name_list ::= col_name_list NK_COMMA col_name */
  {  162,   -1 }, /* (93) col_name ::= column_name */
  {  138,   -7 }, /* (94) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  138,   -9 }, /* (95) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  164,    0 }, /* (96) index_options ::= */
  {  164,   -9 }, /* (97) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  164,  -11 }, /* (98) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  165,   -1 }, /* (99) func_list ::= func */
  {  165,   -3 }, /* (100) func_list ::= func_list NK_COMMA func */
  {  168,   -4 }, /* (101) func ::= function_name NK_LP expression_list NK_RP */
  {  138,   -2 }, /* (102) cmd ::= SHOW VGROUPS */
  {  138,   -4 }, /* (103) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  138,   -2 }, /* (104) cmd ::= SHOW MNODES */
  {  138,   -1 }, /* (105) cmd ::= query_expression */
  {  172,   -1 }, /* (106) literal ::= NK_INTEGER */
  {  172,   -1 }, /* (107) literal ::= NK_FLOAT */
  {  172,   -1 }, /* (108) literal ::= NK_STRING */
  {  172,   -1 }, /* (109) literal ::= NK_BOOL */
  {  172,   -2 }, /* (110) literal ::= TIMESTAMP NK_STRING */
  {  172,   -1 }, /* (111) literal ::= duration_literal */
  {  166,   -1 }, /* (112) duration_literal ::= NK_VARIABLE */
  {  155,   -1 }, /* (113) literal_list ::= literal */
  {  155,   -3 }, /* (114) literal_list ::= literal_list NK_COMMA literal */
  {  143,   -1 }, /* (115) db_name ::= NK_ID */
  {  158,   -1 }, /* (116) table_name ::= NK_ID */
  {  160,   -1 }, /* (117) column_name ::= NK_ID */
  {  169,   -1 }, /* (118) function_name ::= NK_ID */
  {  173,   -1 }, /* (119) table_alias ::= NK_ID */
  {  174,   -1 }, /* (120) column_alias ::= NK_ID */
  {  139,   -1 }, /* (121) user_name ::= NK_ID */
  {  163,   -1 }, /* (122) index_name ::= NK_ID */
  {  175,   -1 }, /* (123) expression ::= literal */
  {  175,   -1 }, /* (124) expression ::= column_reference */
  {  175,   -4 }, /* (125) expression ::= function_name NK_LP expression_list NK_RP */
  {  175,   -4 }, /* (126) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  175,   -1 }, /* (127) expression ::= subquery */
  {  175,   -3 }, /* (128) expression ::= NK_LP expression NK_RP */
  {  175,   -2 }, /* (129) expression ::= NK_PLUS expression */
  {  175,   -2 }, /* (130) expression ::= NK_MINUS expression */
  {  175,   -3 }, /* (131) expression ::= expression NK_PLUS expression */
  {  175,   -3 }, /* (132) expression ::= expression NK_MINUS expression */
  {  175,   -3 }, /* (133) expression ::= expression NK_STAR expression */
  {  175,   -3 }, /* (134) expression ::= expression NK_SLASH expression */
  {  175,   -3 }, /* (135) expression ::= expression NK_REM expression */
  {  170,   -1 }, /* (136) expression_list ::= expression */
  {  170,   -3 }, /* (137) expression_list ::= expression_list NK_COMMA expression */
  {  176,   -1 }, /* (138) column_reference ::= column_name */
  {  176,   -3 }, /* (139) column_reference ::= table_name NK_DOT column_name */
  {  178,   -3 }, /* (140) predicate ::= expression compare_op expression */
  {  178,   -5 }, /* (141) predicate ::= expression BETWEEN expression AND expression */
  {  178,   -6 }, /* (142) predicate ::= expression NOT BETWEEN expression AND expression */
  {  178,   -3 }, /* (143) predicate ::= expression IS NULL */
  {  178,   -4 }, /* (144) predicate ::= expression IS NOT NULL */
  {  178,   -3 }, /* (145) predicate ::= expression in_op in_predicate_value */
  {  179,   -1 }, /* (146) compare_op ::= NK_LT */
  {  179,   -1 }, /* (147) compare_op ::= NK_GT */
  {  179,   -1 }, /* (148) compare_op ::= NK_LE */
  {  179,   -1 }, /* (149) compare_op ::= NK_GE */
  {  179,   -1 }, /* (150) compare_op ::= NK_NE */
  {  179,   -1 }, /* (151) compare_op ::= NK_EQ */
  {  179,   -1 }, /* (152) compare_op ::= LIKE */
  {  179,   -2 }, /* (153) compare_op ::= NOT LIKE */
  {  179,   -1 }, /* (154) compare_op ::= MATCH */
  {  179,   -1 }, /* (155) compare_op ::= NMATCH */
  {  180,   -1 }, /* (156) in_op ::= IN */
  {  180,   -2 }, /* (157) in_op ::= NOT IN */
  {  181,   -3 }, /* (158) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  182,   -1 }, /* (159) boolean_value_expression ::= boolean_primary */
  {  182,   -2 }, /* (160) boolean_value_expression ::= NOT boolean_primary */
  {  182,   -3 }, /* (161) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  182,   -3 }, /* (162) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  183,   -1 }, /* (163) boolean_primary ::= predicate */
  {  183,   -3 }, /* (164) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  184,   -1 }, /* (165) common_expression ::= expression */
  {  184,   -1 }, /* (166) common_expression ::= boolean_value_expression */
  {  185,   -2 }, /* (167) from_clause ::= FROM table_reference_list */
  {  186,   -1 }, /* (168) table_reference_list ::= table_reference */
  {  186,   -3 }, /* (169) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  187,   -1 }, /* (170) table_reference ::= table_primary */
  {  187,   -1 }, /* (171) table_reference ::= joined_table */
  {  188,   -2 }, /* (172) table_primary ::= table_name alias_opt */
  {  188,   -4 }, /* (173) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  188,   -2 }, /* (174) table_primary ::= subquery alias_opt */
  {  188,   -1 }, /* (175) table_primary ::= parenthesized_joined_table */
  {  190,    0 }, /* (176) alias_opt ::= */
  {  190,   -1 }, /* (177) alias_opt ::= table_alias */
  {  190,   -2 }, /* (178) alias_opt ::= AS table_alias */
  {  191,   -3 }, /* (179) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  191,   -3 }, /* (180) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  189,   -6 }, /* (181) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  192,    0 }, /* (182) join_type ::= */
  {  192,   -1 }, /* (183) join_type ::= INNER */
  {  194,   -9 }, /* (184) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  195,    0 }, /* (185) set_quantifier_opt ::= */
  {  195,   -1 }, /* (186) set_quantifier_opt ::= DISTINCT */
  {  195,   -1 }, /* (187) set_quantifier_opt ::= ALL */
  {  196,   -1 }, /* (188) select_list ::= NK_STAR */
  {  196,   -1 }, /* (189) select_list ::= select_sublist */
  {  202,   -1 }, /* (190) select_sublist ::= select_item */
  {  202,   -3 }, /* (191) select_sublist ::= select_sublist NK_COMMA select_item */
  {  203,   -1 }, /* (192) select_item ::= common_expression */
  {  203,   -2 }, /* (193) select_item ::= common_expression column_alias */
  {  203,   -3 }, /* (194) select_item ::= common_expression AS column_alias */
  {  203,   -3 }, /* (195) select_item ::= table_name NK_DOT NK_STAR */
  {  197,    0 }, /* (196) where_clause_opt ::= */
  {  197,   -2 }, /* (197) where_clause_opt ::= WHERE search_condition */
  {  198,    0 }, /* (198) partition_by_clause_opt ::= */
  {  198,   -3 }, /* (199) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  199,    0 }, /* (200) twindow_clause_opt ::= */
  {  199,   -6 }, /* (201) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  199,   -4 }, /* (202) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  199,   -6 }, /* (203) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  199,   -8 }, /* (204) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  167,    0 }, /* (205) sliding_opt ::= */
  {  167,   -4 }, /* (206) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  204,    0 }, /* (207) fill_opt ::= */
  {  204,   -4 }, /* (208) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  204,   -6 }, /* (209) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  205,   -1 }, /* (210) fill_mode ::= NONE */
  {  205,   -1 }, /* (211) fill_mode ::= PREV */
  {  205,   -1 }, /* (212) fill_mode ::= NULL */
  {  205,   -1 }, /* (213) fill_mode ::= LINEAR */
  {  205,   -1 }, /* (214) fill_mode ::= NEXT */
  {  200,    0 }, /* (215) group_by_clause_opt ::= */
  {  200,   -3 }, /* (216) group_by_clause_opt ::= GROUP BY group_by_list */
  {  206,   -1 }, /* (217) group_by_list ::= expression */
  {  206,   -3 }, /* (218) group_by_list ::= group_by_list NK_COMMA expression */
  {  201,    0 }, /* (219) having_clause_opt ::= */
  {  201,   -2 }, /* (220) having_clause_opt ::= HAVING search_condition */
  {  171,   -4 }, /* (221) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  207,   -1 }, /* (222) query_expression_body ::= query_primary */
  {  207,   -4 }, /* (223) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  211,   -1 }, /* (224) query_primary ::= query_specification */
  {  208,    0 }, /* (225) order_by_clause_opt ::= */
  {  208,   -3 }, /* (226) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  209,    0 }, /* (227) slimit_clause_opt ::= */
  {  209,   -2 }, /* (228) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  209,   -4 }, /* (229) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  209,   -4 }, /* (230) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  210,    0 }, /* (231) limit_clause_opt ::= */
  {  210,   -2 }, /* (232) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  210,   -4 }, /* (233) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  210,   -4 }, /* (234) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  177,   -3 }, /* (235) subquery ::= NK_LP query_expression NK_RP */
  {  193,   -1 }, /* (236) search_condition ::= common_expression */
  {  212,   -1 }, /* (237) sort_specification_list ::= sort_specification */
  {  212,   -3 }, /* (238) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  213,   -3 }, /* (239) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  214,    0 }, /* (240) ordering_specification_opt ::= */
  {  214,   -1 }, /* (241) ordering_specification_opt ::= ASC */
  {  214,   -1 }, /* (242) ordering_specification_opt ::= DESC */
  {  215,    0 }, /* (243) null_ordering_opt ::= */
  {  215,   -2 }, /* (244) null_ordering_opt ::= NULLS FIRST */
  {  215,   -2 }, /* (245) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy0);}
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy209, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0);}
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy209, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0);}
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy209); }
        break;
      case 4: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy209, NULL);}
        break;
      case 6: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy0);}
        break;
      case 7: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0);}
        break;
      case 8: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy209);}
        break;
      case 9: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 115: /* db_name ::= NK_ID */ yytestcase(yyruleno==115);
      case 116: /* table_name ::= NK_ID */ yytestcase(yyruleno==116);
      case 117: /* column_name ::= NK_ID */ yytestcase(yyruleno==117);
      case 118: /* function_name ::= NK_ID */ yytestcase(yyruleno==118);
      case 119: /* table_alias ::= NK_ID */ yytestcase(yyruleno==119);
      case 120: /* column_alias ::= NK_ID */ yytestcase(yyruleno==120);
      case 121: /* user_name ::= NK_ID */ yytestcase(yyruleno==121);
      case 122: /* index_name ::= NK_ID */ yytestcase(yyruleno==122);
{ yylhsminor.yy209 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 13: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy281, &yymsp[-1].minor.yy209, yymsp[0].minor.yy344);}
        break;
      case 14: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy281, &yymsp[0].minor.yy209); }
        break;
      case 15: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 16: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy209);}
        break;
      case 17: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy281 = true; }
        break;
      case 18: /* not_exists_opt ::= */
      case 20: /* exists_opt ::= */ yytestcase(yyruleno==20);
      case 185: /* set_quantifier_opt ::= */ yytestcase(yyruleno==185);
{ yymsp[1].minor.yy281 = false; }
        break;
      case 19: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy281 = true; }
        break;
      case 21: /* db_options ::= */
{ yymsp[1].minor.yy344 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 22: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 23: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 24: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 25: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 26: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 27: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 28: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 29: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 30: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 31: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 32: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 33: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 34: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 35: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 36: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 37: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 38: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy344 = setDatabaseOption(pCxt, yymsp[-2].minor.yy344, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 39: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 41: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==41);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy281, yymsp[-5].minor.yy344, yymsp[-3].minor.yy280, yymsp[-1].minor.yy280, yymsp[0].minor.yy344);}
        break;
      case 40: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy280);}
        break;
      case 42: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy280); }
        break;
      case 43: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy281, yymsp[0].minor.yy344); }
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
      case 99: /* func_list ::= func */ yytestcase(yyruleno==99);
      case 190: /* select_sublist ::= select_item */ yytestcase(yyruleno==190);
      case 237: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==237);
{ yylhsminor.yy280 = createNodeList(pCxt, yymsp[0].minor.yy344); }
  yymsp[0].minor.yy280 = yylhsminor.yy280;
        break;
      case 47: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 50: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==50);
{ yylhsminor.yy280 = addNodeToList(pCxt, yymsp[-1].minor.yy280, yymsp[0].minor.yy344); }
  yymsp[-1].minor.yy280 = yylhsminor.yy280;
        break;
      case 48: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy344 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy281, yymsp[-7].minor.yy344, yymsp[-5].minor.yy344, yymsp[-4].minor.yy280, yymsp[-1].minor.yy280); }
  yymsp[-8].minor.yy344 = yylhsminor.yy344;
        break;
      case 51: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy344 = createDropTableClause(pCxt, yymsp[-1].minor.yy281, yymsp[0].minor.yy344); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 52: /* specific_tags_opt ::= */
      case 83: /* tags_def_opt ::= */ yytestcase(yyruleno==83);
      case 198: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==198);
      case 215: /* group_by_clause_opt ::= */ yytestcase(yyruleno==215);
      case 225: /* order_by_clause_opt ::= */ yytestcase(yyruleno==225);
{ yymsp[1].minor.yy280 = NULL; }
        break;
      case 53: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy280 = yymsp[-1].minor.yy280; }
        break;
      case 54: /* full_table_name ::= table_name */
{ yylhsminor.yy344 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy209, NULL); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 55: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy344 = createRealTableNode(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy209, NULL); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 57: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 92: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==92);
      case 100: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==100);
      case 191: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==191);
      case 238: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==238);
{ yylhsminor.yy280 = addNodeToList(pCxt, yymsp[-2].minor.yy280, yymsp[0].minor.yy344); }
  yymsp[-2].minor.yy280 = yylhsminor.yy280;
        break;
      case 58: /* column_def ::= column_name type_name */
{ yylhsminor.yy344 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy209, yymsp[0].minor.yy304, NULL); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 59: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy344 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy209, yymsp[-2].minor.yy304, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 60: /* type_name ::= BOOL */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 61: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 62: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 63: /* type_name ::= INT */
      case 64: /* type_name ::= INTEGER */ yytestcase(yyruleno==64);
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 65: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 66: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 67: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 68: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 69: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 70: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 71: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 72: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 73: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 74: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 75: /* type_name ::= JSON */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 76: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 77: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 78: /* type_name ::= BLOB */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 79: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 80: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 81: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 82: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 84: /* tags_def_opt ::= tags_def */
      case 189: /* select_list ::= select_sublist */ yytestcase(yyruleno==189);
{ yylhsminor.yy280 = yymsp[0].minor.yy280; }
  yymsp[0].minor.yy280 = yylhsminor.yy280;
        break;
      case 85: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy280 = yymsp[-1].minor.yy280; }
        break;
      case 86: /* table_options ::= */
{ yymsp[1].minor.yy344 = createDefaultTableOptions(pCxt);}
        break;
      case 87: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy344 = setTableOption(pCxt, yymsp[-2].minor.yy344, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 88: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy344 = setTableOption(pCxt, yymsp[-2].minor.yy344, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 89: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy344 = setTableOption(pCxt, yymsp[-2].minor.yy344, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 90: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy344 = setTableSmaOption(pCxt, yymsp[-4].minor.yy344, yymsp[-1].minor.yy280); }
  yymsp[-4].minor.yy344 = yylhsminor.yy344;
        break;
      case 93: /* col_name ::= column_name */
{ yylhsminor.yy344 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy209); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 94: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy209, &yymsp[-1].minor.yy209, NULL, yymsp[0].minor.yy344); }
        break;
      case 95: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy209, &yymsp[-3].minor.yy209, yymsp[-1].minor.yy280, NULL); }
        break;
      case 96: /* index_options ::= */
      case 196: /* where_clause_opt ::= */ yytestcase(yyruleno==196);
      case 200: /* twindow_clause_opt ::= */ yytestcase(yyruleno==200);
      case 205: /* sliding_opt ::= */ yytestcase(yyruleno==205);
      case 207: /* fill_opt ::= */ yytestcase(yyruleno==207);
      case 219: /* having_clause_opt ::= */ yytestcase(yyruleno==219);
      case 227: /* slimit_clause_opt ::= */ yytestcase(yyruleno==227);
      case 231: /* limit_clause_opt ::= */ yytestcase(yyruleno==231);
{ yymsp[1].minor.yy344 = NULL; }
        break;
      case 97: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy344 = createIndexOption(pCxt, yymsp[-6].minor.yy280, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), NULL, yymsp[0].minor.yy344); }
        break;
      case 98: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy344 = createIndexOption(pCxt, yymsp[-8].minor.yy280, releaseRawExprNode(pCxt, yymsp[-4].minor.yy344), releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), yymsp[0].minor.yy344); }
        break;
      case 101: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy344 = createFunctionNode(pCxt, &yymsp[-3].minor.yy209, yymsp[-1].minor.yy280); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 102: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 103: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy209); }
        break;
      case 104: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 105: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy344; }
        break;
      case 106: /* literal ::= NK_INTEGER */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 107: /* literal ::= NK_FLOAT */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 108: /* literal ::= NK_STRING */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 109: /* literal ::= NK_BOOL */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 110: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 111: /* literal ::= duration_literal */
      case 123: /* expression ::= literal */ yytestcase(yyruleno==123);
      case 124: /* expression ::= column_reference */ yytestcase(yyruleno==124);
      case 127: /* expression ::= subquery */ yytestcase(yyruleno==127);
      case 159: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==159);
      case 163: /* boolean_primary ::= predicate */ yytestcase(yyruleno==163);
      case 165: /* common_expression ::= expression */ yytestcase(yyruleno==165);
      case 166: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==166);
      case 168: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==168);
      case 170: /* table_reference ::= table_primary */ yytestcase(yyruleno==170);
      case 171: /* table_reference ::= joined_table */ yytestcase(yyruleno==171);
      case 175: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==175);
      case 222: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==222);
      case 224: /* query_primary ::= query_specification */ yytestcase(yyruleno==224);
{ yylhsminor.yy344 = yymsp[0].minor.yy344; }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 112: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 113: /* literal_list ::= literal */
      case 136: /* expression_list ::= expression */ yytestcase(yyruleno==136);
{ yylhsminor.yy280 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy344)); }
  yymsp[0].minor.yy280 = yylhsminor.yy280;
        break;
      case 114: /* literal_list ::= literal_list NK_COMMA literal */
      case 137: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==137);
{ yylhsminor.yy280 = addNodeToList(pCxt, yymsp[-2].minor.yy280, releaseRawExprNode(pCxt, yymsp[0].minor.yy344)); }
  yymsp[-2].minor.yy280 = yylhsminor.yy280;
        break;
      case 125: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy209, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy209, yymsp[-1].minor.yy280)); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 126: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy209, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy209, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 128: /* expression ::= NK_LP expression NK_RP */
      case 164: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==164);
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy344)); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 129: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy344));
                                                                                  }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 130: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy344), NULL));
                                                                                  }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 131: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); 
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 132: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); 
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 133: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); 
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 134: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); 
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 135: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); 
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 138: /* column_reference ::= column_name */
{ yylhsminor.yy344 = createRawExprNode(pCxt, &yymsp[0].minor.yy209, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy209)); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 139: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy209, createColumnNode(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy209)); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 140: /* predicate ::= expression compare_op expression */
      case 145: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==145);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy236, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344)));
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 141: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy344), releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344)));
                                                                                  }
  yymsp[-4].minor.yy344 = yylhsminor.yy344;
        break;
      case 142: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[-5].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344)));
                                                                                  }
  yymsp[-5].minor.yy344 = yylhsminor.yy344;
        break;
      case 143: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), NULL));
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 144: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy344), NULL));
                                                                                  }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 146: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy236 = OP_TYPE_LOWER_THAN; }
        break;
      case 147: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy236 = OP_TYPE_GREATER_THAN; }
        break;
      case 148: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy236 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 149: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy236 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 150: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy236 = OP_TYPE_NOT_EQUAL; }
        break;
      case 151: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy236 = OP_TYPE_EQUAL; }
        break;
      case 152: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy236 = OP_TYPE_LIKE; }
        break;
      case 153: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy236 = OP_TYPE_NOT_LIKE; }
        break;
      case 154: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy236 = OP_TYPE_MATCH; }
        break;
      case 155: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy236 = OP_TYPE_NMATCH; }
        break;
      case 156: /* in_op ::= IN */
{ yymsp[0].minor.yy236 = OP_TYPE_IN; }
        break;
      case 157: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy236 = OP_TYPE_NOT_IN; }
        break;
      case 158: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy280)); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 160: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy344), NULL));
                                                                                  }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 161: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344)));
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 162: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy344);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), releaseRawExprNode(pCxt, yymsp[0].minor.yy344)));
                                                                                  }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 167: /* from_clause ::= FROM table_reference_list */
      case 197: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==197);
      case 220: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==220);
{ yymsp[-1].minor.yy344 = yymsp[0].minor.yy344; }
        break;
      case 169: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy344 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy344, yymsp[0].minor.yy344, NULL); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 172: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy344 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy209, &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 173: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy344 = createRealTableNode(pCxt, &yymsp[-3].minor.yy209, &yymsp[-1].minor.yy209, &yymsp[0].minor.yy209); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 174: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy344 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy344), &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 176: /* alias_opt ::= */
{ yymsp[1].minor.yy209 = nil_token;  }
        break;
      case 177: /* alias_opt ::= table_alias */
{ yylhsminor.yy209 = yymsp[0].minor.yy209; }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 178: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy209 = yymsp[0].minor.yy209; }
        break;
      case 179: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 180: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==180);
{ yymsp[-2].minor.yy344 = yymsp[-1].minor.yy344; }
        break;
      case 181: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy344 = createJoinTableNode(pCxt, yymsp[-4].minor.yy36, yymsp[-5].minor.yy344, yymsp[-2].minor.yy344, yymsp[0].minor.yy344); }
  yymsp[-5].minor.yy344 = yylhsminor.yy344;
        break;
      case 182: /* join_type ::= */
{ yymsp[1].minor.yy36 = JOIN_TYPE_INNER; }
        break;
      case 183: /* join_type ::= INNER */
{ yymsp[0].minor.yy36 = JOIN_TYPE_INNER; }
        break;
      case 184: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy344 = createSelectStmt(pCxt, yymsp[-7].minor.yy281, yymsp[-6].minor.yy280, yymsp[-5].minor.yy344);
                                                                                    yymsp[-8].minor.yy344 = addWhereClause(pCxt, yymsp[-8].minor.yy344, yymsp[-4].minor.yy344);
                                                                                    yymsp[-8].minor.yy344 = addPartitionByClause(pCxt, yymsp[-8].minor.yy344, yymsp[-3].minor.yy280);
                                                                                    yymsp[-8].minor.yy344 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy344, yymsp[-2].minor.yy344);
                                                                                    yymsp[-8].minor.yy344 = addGroupByClause(pCxt, yymsp[-8].minor.yy344, yymsp[-1].minor.yy280);
                                                                                    yymsp[-8].minor.yy344 = addHavingClause(pCxt, yymsp[-8].minor.yy344, yymsp[0].minor.yy344);
                                                                                  }
        break;
      case 186: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy281 = true; }
        break;
      case 187: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy281 = false; }
        break;
      case 188: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy280 = NULL; }
        break;
      case 192: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy344);
                                                                                    yylhsminor.yy344 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy344), &t);
                                                                                  }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 193: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy344 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy344), &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 194: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy344 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), &yymsp[0].minor.yy209); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 195: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy344 = createColumnNode(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 199: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 216: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==216);
      case 226: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==226);
{ yymsp[-2].minor.yy280 = yymsp[0].minor.yy280; }
        break;
      case 201: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy344 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy344), &yymsp[-1].minor.yy0); }
        break;
      case 202: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy344 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy344)); }
        break;
      case 203: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy344 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy344), NULL, yymsp[-1].minor.yy344, yymsp[0].minor.yy344); }
        break;
      case 204: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy344 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy344), releaseRawExprNode(pCxt, yymsp[-3].minor.yy344), yymsp[-1].minor.yy344, yymsp[0].minor.yy344); }
        break;
      case 206: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy344 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy344); }
        break;
      case 208: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy344 = createFillNode(pCxt, yymsp[-1].minor.yy342, NULL); }
        break;
      case 209: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy344 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy280)); }
        break;
      case 210: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy342 = FILL_MODE_NONE; }
        break;
      case 211: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy342 = FILL_MODE_PREV; }
        break;
      case 212: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy342 = FILL_MODE_NULL; }
        break;
      case 213: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy342 = FILL_MODE_LINEAR; }
        break;
      case 214: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy342 = FILL_MODE_NEXT; }
        break;
      case 217: /* group_by_list ::= expression */
{ yylhsminor.yy280 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); }
  yymsp[0].minor.yy280 = yylhsminor.yy280;
        break;
      case 218: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy280 = addNodeToList(pCxt, yymsp[-2].minor.yy280, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy344))); }
  yymsp[-2].minor.yy280 = yylhsminor.yy280;
        break;
      case 221: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy344 = addOrderByClause(pCxt, yymsp[-3].minor.yy344, yymsp[-2].minor.yy280);
                                                                                    yylhsminor.yy344 = addSlimitClause(pCxt, yylhsminor.yy344, yymsp[-1].minor.yy344);
                                                                                    yylhsminor.yy344 = addLimitClause(pCxt, yylhsminor.yy344, yymsp[0].minor.yy344);
                                                                                  }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 223: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy344 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy344, yymsp[0].minor.yy344); }
  yymsp[-3].minor.yy344 = yylhsminor.yy344;
        break;
      case 228: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 232: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==232);
{ yymsp[-1].minor.yy344 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 229: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 233: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==233);
{ yymsp[-3].minor.yy344 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 230: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 234: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==234);
{ yymsp[-3].minor.yy344 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 235: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy344 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy344); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 236: /* search_condition ::= common_expression */
{ yylhsminor.yy344 = releaseRawExprNode(pCxt, yymsp[0].minor.yy344); }
  yymsp[0].minor.yy344 = yylhsminor.yy344;
        break;
      case 239: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy344 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy344), yymsp[-1].minor.yy2, yymsp[0].minor.yy217); }
  yymsp[-2].minor.yy344 = yylhsminor.yy344;
        break;
      case 240: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy2 = ORDER_ASC; }
        break;
      case 241: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy2 = ORDER_ASC; }
        break;
      case 242: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy2 = ORDER_DESC; }
        break;
      case 243: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy217 = NULL_ORDER_DEFAULT; }
        break;
      case 244: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy217 = NULL_ORDER_FIRST; }
        break;
      case 245: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy217 = NULL_ORDER_LAST; }
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
