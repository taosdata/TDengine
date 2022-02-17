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

#if 0
#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
#define PARSER_COMPLETE printf("parsing complete!\n" )
#else
#define PARSER_TRACE
#define PARSER_DESTRUCTOR_TRACE
#define PARSER_COMPLETE
#endif
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
#define YYNOCODE 126
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EFillMode yy18;
  SToken yy29;
  EJoinType yy36;
  SNode* yy56;
  ENullOrder yy109;
  EOperatorType yy128;
  bool yy173;
  SNodeList* yy208;
  EOrder yy218;
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
#define YYNSTATE             148
#define YYNRULE              140
#define YYNTOKEN             72
#define YY_MAX_SHIFT         147
#define YY_MIN_SHIFTREDUCE   245
#define YY_MAX_SHIFTREDUCE   384
#define YY_ERROR_ACTION      385
#define YY_ACCEPT_ACTION     386
#define YY_NO_ACTION         387
#define YY_MIN_REDUCE        388
#define YY_MAX_REDUCE        527
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
#define YY_ACTTAB_COUNT (750)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   397,  395,   93,   23,   72,  398,  395,   73,   30,   28,
 /*    10 */    26,   25,   24,  405,  395,  142,  420,  142,  420,  143,
 /*    20 */   110,  113,   80,  406,  267,  409,   22,   88,   97,  147,
 /*    30 */   285,  286,  287,  288,  289,  290,  291,  293,  294,  295,
 /*    40 */    30,   28,   26,   25,   24,  405,  395,  142,  420,  142,
 /*    50 */   420,  143,  118,  117,   46,  406,  260,  409,   22,   88,
 /*    60 */    97,   55,  285,  286,  287,  288,  289,  290,  291,  293,
 /*    70 */   294,  295,  132,  405,  395,   70,   65,  142,  420,  143,
 /*    80 */   127,   10,   39,  406,   55,  409,  445,  119,  114,  112,
 /*    90 */    87,  441,  334,  133,  519,  247,  248,  249,  250,  144,
 /*   100 */   253,  459,  506,   55,  258,  405,  395,  475,  261,  128,
 /*   110 */   420,  143,  127,   10,   40,  406,   54,  409,  445,  456,
 /*   120 */   504,  459,   95,  441,   49,  405,  395,   56,  135,  142,
 /*   130 */   420,  143,  383,  384,   81,  406,   71,  409,   20,  455,
 /*   140 */   405,  395,  103,  472,  128,  420,  143,  125,  292,   40,
 /*   150 */   406,  296,  409,  445,   29,   27,  421,   95,  441,   49,
 /*   160 */    75,  247,  248,  249,  250,  144,  253,  120,  102,    1,
 /*   170 */   459,    9,    8,   11,   26,   25,   24,  464,  473,  322,
 /*   180 */   405,  395,  386,  145,  142,  420,  143,  134,  454,   40,
 /*   190 */   406,   55,  409,  445,  303,   29,   27,   95,  441,  518,
 /*   200 */     2,  326,  247,  248,  249,  250,  144,  253,  479,  102,
 /*   210 */     1,  405,  395,  506,   11,  142,  420,  143,    6,  322,
 /*   220 */    40,  406,  261,  409,  445,    9,    8,   54,   95,  441,
 /*   230 */   518,  504,  405,  395,  506,  139,  142,  420,  143,  502,
 /*   240 */   111,   40,  406,  325,  409,  445,  486,  136,  505,   95,
 /*   250 */   441,  518,  504,  476,   29,   27,  327,  108,  124,   45,
 /*   260 */   463,  247,  248,  249,  250,  144,  253,   43,  102,    1,
 /*   270 */    57,   42,  351,   11,  348,  253,  126,   50,  452,  453,
 /*   280 */   140,  457,  132,  405,  395,  106,  107,  142,  420,  143,
 /*   290 */   137,   59,   79,  406,   62,  409,   29,   27,  109,  349,
 /*   300 */   350,  352,  353,  247,  248,  249,  250,  144,  253,  485,
 /*   310 */   102,    7,  506,   30,   28,   26,   25,   24,   31,  405,
 /*   320 */   395,  297,   94,  142,  420,  143,   54,    5,   41,  406,
 /*   330 */   504,  409,  445,   55,   61,  466,  444,  441,  405,  395,
 /*   340 */   121,   64,  142,  420,  143,   48,  105,   41,  406,    4,
 /*   350 */   409,  445,  130,  322,  282,  129,  441,  132,   66,  257,
 /*   360 */    31,  124,   45,  264,   29,   27,  131,  402,   44,  400,
 /*   370 */    43,  247,  248,  249,  250,  144,  253,  260,  102,    7,
 /*   380 */    68,  452,  123,   32,  122,   29,   27,  506,  460,   67,
 /*   390 */    16,  427,  247,  248,  249,  250,  144,  253,  521,  102,
 /*   400 */     1,   54,   98,  405,  395,  504,  141,  142,  420,  143,
 /*   410 */   138,  503,   41,  406,  258,  409,  445,   29,   27,   74,
 /*   420 */     3,  442,   14,   31,  247,  248,  249,  250,  144,  253,
 /*   430 */    58,  102,    7,  115,  345,   60,  405,  395,   35,  347,
 /*   440 */   142,  420,  143,   47,   63,   85,  406,  101,  409,  405,
 /*   450 */   395,  116,  341,  142,  420,  143,   36,  400,   85,  406,
 /*   460 */   104,  409,  405,  395,  340,   18,  142,  420,  143,   37,
 /*   470 */    69,   85,  406,   96,  409,  405,  395,  319,   15,  142,
 /*   480 */   420,  143,  318,   33,   46,  406,   34,  409,  405,  395,
 /*   490 */     8,  399,  142,  420,  143,   53,  283,   82,  406,  265,
 /*   500 */   409,  405,  395,  374,   17,  142,  420,  143,   12,   38,
 /*   510 */    77,  406,  369,  409,  368,   99,  405,  395,  373,  372,
 /*   520 */   142,  420,  143,  100,  520,   83,  406,   76,  409,  405,
 /*   530 */   395,  251,   13,  142,  420,  143,  389,  388,   78,  406,
 /*   540 */   146,  409,  405,  395,  387,  387,  142,  420,  143,  387,
 /*   550 */   387,   84,  406,  387,  409,  405,  395,  387,  387,  142,
 /*   560 */   420,  143,  387,  387,  417,  406,  387,  409,  405,  395,
 /*   570 */   387,  387,  142,  420,  143,  387,  387,  416,  406,  387,
 /*   580 */   409,  405,  395,  387,  387,  142,  420,  143,  387,  387,
 /*   590 */   415,  406,  387,  409,  387,  405,  395,  387,  387,  142,
 /*   600 */   420,  143,  387,  387,   91,  406,  387,  409,  405,  395,
 /*   610 */   387,  387,  142,  420,  143,  387,  387,   90,  406,  387,
 /*   620 */   409,  405,  395,  387,  387,  142,  420,  143,  387,  387,
 /*   630 */    92,  406,  387,  409,  405,  395,  387,  387,  142,  420,
 /*   640 */   143,  387,  387,   89,  406,  387,  409,  405,  395,  387,
 /*   650 */   387,  142,  420,  143,  387,  387,   86,  406,  387,  409,
 /*   660 */   124,   45,  387,  387,  387,  124,   45,  387,  387,   43,
 /*   670 */   387,  387,  387,  387,   43,  387,  387,  387,  387,   51,
 /*   680 */   452,  453,  387,  457,   52,  452,  453,  387,  457,   30,
 /*   690 */    28,   26,   25,   24,   19,  387,  387,  387,  387,  387,
 /*   700 */    30,   28,   26,   25,   24,   21,  387,  387,  387,  387,
 /*   710 */   387,   30,   28,   26,   25,   24,  387,  387,  387,  387,
 /*   720 */    30,   28,   26,   25,   24,  387,  387,  387,  387,  387,
 /*   730 */   387,  387,  387,  387,  387,  387,  267,  387,  387,  387,
 /*   740 */   387,  387,  387,  387,  387,  387,  387,  387,  380,  381,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    74,   75,   76,   88,   89,   74,   75,  124,    8,    9,
 /*    10 */    10,   11,   12,   74,   75,   78,   79,   78,   79,   80,
 /*    20 */   115,   84,   83,   84,   24,   86,   26,   27,   28,   13,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     8,    9,   10,   11,   12,   74,   75,   78,   79,   78,
 /*    50 */    79,   80,   22,   84,   83,   84,   22,   86,   26,   27,
 /*    60 */    28,   45,   30,   31,   32,   33,   34,   35,   36,   37,
 /*    70 */    38,   39,   73,   74,   75,   41,  108,   78,   79,   80,
 /*    80 */    22,   23,   83,   84,   45,   86,   87,   50,   51,   52,
 /*    90 */    91,   92,   10,  122,  123,   15,   16,   17,   18,   19,
 /*   100 */    20,   81,  103,   45,   22,   74,   75,   82,   22,   78,
 /*   110 */    79,   80,   22,   23,   83,   84,  117,   86,   87,   99,
 /*   120 */   121,   81,   91,   92,   93,   74,   75,   41,    3,   78,
 /*   130 */    79,   80,   70,   71,   83,   84,  105,   86,   26,   99,
 /*   140 */    74,   75,  111,  112,   78,   79,   80,  101,   36,   83,
 /*   150 */    84,   39,   86,   87,    8,    9,   79,   91,   92,   93,
 /*   160 */   118,   15,   16,   17,   18,   19,   20,  116,   22,   23,
 /*   170 */    81,    1,    2,   27,   10,   11,   12,   42,  112,   44,
 /*   180 */    74,   75,   72,   73,   78,   79,   80,   62,   99,   83,
 /*   190 */    84,   45,   86,   87,   24,    8,    9,   91,   92,   93,
 /*   200 */   104,    4,   15,   16,   17,   18,   19,   20,  102,   22,
 /*   210 */    23,   74,   75,  103,   27,   78,   79,   80,   43,   44,
 /*   220 */    83,   84,   22,   86,   87,    1,    2,  117,   91,   92,
 /*   230 */    93,  121,   74,   75,  103,   21,   78,   79,   80,  102,
 /*   240 */    54,   83,   84,   46,   86,   87,  114,   21,  117,   91,
 /*   250 */    92,   93,  121,   82,    8,    9,   10,   53,   77,   78,
 /*   260 */   102,   15,   16,   17,   18,   19,   20,   86,   22,   23,
 /*   270 */   113,   21,   29,   27,   24,   20,   95,   96,   97,   98,
 /*   280 */    66,  100,   73,   74,   75,   75,   75,   78,   79,   80,
 /*   290 */    64,   21,   83,   84,   24,   86,    8,    9,   55,   56,
 /*   300 */    57,   58,   59,   15,   16,   17,   18,   19,   20,  114,
 /*   310 */    22,   23,  103,    8,    9,   10,   11,   12,   21,   74,
 /*   320 */    75,   24,   75,   78,   79,   80,  117,   61,   83,   84,
 /*   330 */   121,   86,   87,   45,  113,  110,   91,   92,   74,   75,
 /*   340 */    60,  109,   78,   79,   80,  107,   48,   83,   84,   47,
 /*   350 */    86,   87,   27,   44,   29,   91,   92,   73,  106,   22,
 /*   360 */    21,   77,   78,   24,    8,    9,   10,   23,   78,   25,
 /*   370 */    86,   15,   16,   17,   18,   19,   20,   22,   22,   23,
 /*   380 */    96,   97,   98,   40,  100,    8,    9,  103,   81,   94,
 /*   390 */    23,   90,   15,   16,   17,   18,   19,   20,  125,   22,
 /*   400 */    23,  117,   69,   74,   75,  121,   65,   78,   79,   80,
 /*   410 */    63,  120,   83,   84,   22,   86,   87,    8,    9,  119,
 /*   420 */    21,   92,   49,   21,   15,   16,   17,   18,   19,   20,
 /*   430 */    24,   22,   23,   15,   24,   23,   74,   75,   21,   24,
 /*   440 */    78,   79,   80,   23,   23,   83,   84,   85,   86,   74,
 /*   450 */    75,   21,   24,   78,   79,   80,   23,   25,   83,   84,
 /*   460 */    85,   86,   74,   75,   24,   21,   78,   79,   80,   23,
 /*   470 */    25,   83,   84,   85,   86,   74,   75,   24,   49,   78,
 /*   480 */    79,   80,   24,   42,   83,   84,   21,   86,   74,   75,
 /*   490 */     2,   25,   78,   79,   80,   25,   29,   83,   84,   24,
 /*   500 */    86,   74,   75,   24,   21,   78,   79,   80,   49,    4,
 /*   510 */    83,   84,   15,   86,   15,   15,   74,   75,   15,   15,
 /*   520 */    78,   79,   80,   15,  123,   83,   84,   25,   86,   74,
 /*   530 */    75,   17,   23,   78,   79,   80,    0,    0,   83,   84,
 /*   540 */    14,   86,   74,   75,  126,  126,   78,   79,   80,  126,
 /*   550 */   126,   83,   84,  126,   86,   74,   75,  126,  126,   78,
 /*   560 */    79,   80,  126,  126,   83,   84,  126,   86,   74,   75,
 /*   570 */   126,  126,   78,   79,   80,  126,  126,   83,   84,  126,
 /*   580 */    86,   74,   75,  126,  126,   78,   79,   80,  126,  126,
 /*   590 */    83,   84,  126,   86,  126,   74,   75,  126,  126,   78,
 /*   600 */    79,   80,  126,  126,   83,   84,  126,   86,   74,   75,
 /*   610 */   126,  126,   78,   79,   80,  126,  126,   83,   84,  126,
 /*   620 */    86,   74,   75,  126,  126,   78,   79,   80,  126,  126,
 /*   630 */    83,   84,  126,   86,   74,   75,  126,  126,   78,   79,
 /*   640 */    80,  126,  126,   83,   84,  126,   86,   74,   75,  126,
 /*   650 */   126,   78,   79,   80,  126,  126,   83,   84,  126,   86,
 /*   660 */    77,   78,  126,  126,  126,   77,   78,  126,  126,   86,
 /*   670 */   126,  126,  126,  126,   86,  126,  126,  126,  126,   96,
 /*   680 */    97,   98,  126,  100,   96,   97,   98,  126,  100,    8,
 /*   690 */     9,   10,   11,   12,    2,  126,  126,  126,  126,  126,
 /*   700 */     8,    9,   10,   11,   12,    2,  126,  126,  126,  126,
 /*   710 */   126,    8,    9,   10,   11,   12,  126,  126,  126,  126,
 /*   720 */     8,    9,   10,   11,   12,  126,  126,  126,  126,  126,
 /*   730 */   126,  126,  126,  126,  126,  126,   24,  126,  126,  126,
 /*   740 */   126,  126,  126,  126,  126,  126,  126,  126,   67,   68,
 /*   750 */   126,  126,  126,  126,  126,  126,  126,  126,  126,  126,
 /*   760 */   126,  126,  126,  126,  126,  126,  126,  126,  126,  126,
 /*   770 */   126,  126,  126,  126,  126,  126,  126,  126,  126,  126,
 /*   780 */   126,  126,  126,  126,  126,  126,  126,  126,  126,  126,
 /*   790 */   126,  126,  126,
};
#define YY_SHIFT_COUNT    (147)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (712)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    16,  146,  246,  187,  187,  187,  187,  288,  187,  187,
 /*    10 */    58,  377,  409,  356,  409,  409,  409,  409,  409,  409,
 /*    20 */   409,  409,  409,  409,  409,  409,  409,  409,  409,  409,
 /*    30 */   409,  409,   90,   90,   90,   80,   30,   30,   39,    0,
 /*    40 */    32,   32,   80,   34,   34,   34,  681,  243,   37,   86,
 /*    50 */   135,  175,  135,   82,  125,  197,  200,  186,  204,  255,
 /*    60 */   255,  186,  204,  255,  266,  280,  298,  302,  309,  337,
 /*    70 */   355,  343,  367,  333,  341,  347,  392,  692,  703,  712,
 /*    80 */   305,  305,  305,  305,  305,  305,  305,  170,  112,  164,
 /*    90 */   164,  164,  164,  250,  270,  224,  297,  325,   62,  226,
 /*   100 */   214,  339,  344,  399,  402,  373,  406,  410,  412,  417,
 /*   110 */   415,  420,  421,  428,  433,  440,  418,  430,  432,  446,
 /*   120 */   444,  429,  453,  458,  445,  441,  465,  466,  470,  488,
 /*   130 */   467,  475,  479,  483,  459,  505,  497,  499,  500,  503,
 /*   140 */   504,  508,  502,  509,  514,  536,  537,  526,
};
#define YY_REDUCE_COUNT (76)
#define YY_REDUCE_MIN   (-117)
#define YY_REDUCE_MAX   (588)
static const short yy_reduce_ofst[] = {
 /*     0 */   110,   -1,   31,   66,  106,  137,  158,  209,  245,  264,
 /*    10 */   284,  329,  -29,  362,  375,   51,  388,  401,  -61,  414,
 /*    20 */   427,  442,  455,  468,  481,  494,  507,  521,  534,  547,
 /*    30 */   560,  573,  181,  583,  588,  -74,  -63,  -31,  131,  -85,
 /*    40 */   -85,  -85,  -69,   20,   40,   89, -117,  -95,  -32,   25,
 /*    50 */    46,   46,   46,   77,   42,   96,  171,  132,  157,  210,
 /*    60 */   211,  195,  221,  247,  225,  232,  238,  252,   46,  290,
 /*    70 */   307,  295,  301,  273,  291,  300,   77,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   385,  385,  385,  385,  385,  385,  385,  385,  385,  385,
 /*    10 */   385,  385,  385,  385,  385,  385,  385,  385,  385,  385,
 /*    20 */   385,  385,  385,  385,  385,  385,  385,  385,  385,  385,
 /*    30 */   385,  385,  385,  385,  385,  385,  385,  385,  385,  385,
 /*    40 */   447,  385,  385,  458,  458,  458,  522,  385,  482,  474,
 /*    50 */   450,  464,  451,  385,  507,  467,  385,  489,  487,  385,
 /*    60 */   385,  489,  487,  385,  501,  497,  480,  478,  464,  385,
 /*    70 */   385,  385,  385,  525,  513,  509,  385,  385,  385,  385,
 /*    80 */   500,  499,  424,  423,  422,  418,  419,  385,  385,  413,
 /*    90 */   414,  412,  411,  385,  385,  448,  385,  385,  385,  510,
 /*   100 */   514,  385,  401,  471,  481,  385,  385,  385,  385,  385,
 /*   110 */   385,  385,  385,  385,  385,  385,  385,  385,  401,  385,
 /*   120 */   498,  385,  457,  453,  385,  385,  449,  400,  385,  443,
 /*   130 */   385,  385,  385,  508,  385,  385,  385,  385,  385,  385,
 /*   140 */   385,  385,  385,  385,  385,  385,  385,  385,
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
  /*    8 */ "NK_PLUS",
  /*    9 */ "NK_MINUS",
  /*   10 */ "NK_STAR",
  /*   11 */ "NK_SLASH",
  /*   12 */ "NK_REM",
  /*   13 */ "SHOW",
  /*   14 */ "DATABASES",
  /*   15 */ "NK_INTEGER",
  /*   16 */ "NK_FLOAT",
  /*   17 */ "NK_STRING",
  /*   18 */ "NK_BOOL",
  /*   19 */ "TIMESTAMP",
  /*   20 */ "NK_VARIABLE",
  /*   21 */ "NK_COMMA",
  /*   22 */ "NK_ID",
  /*   23 */ "NK_LP",
  /*   24 */ "NK_RP",
  /*   25 */ "NK_DOT",
  /*   26 */ "BETWEEN",
  /*   27 */ "NOT",
  /*   28 */ "IS",
  /*   29 */ "NULL",
  /*   30 */ "NK_LT",
  /*   31 */ "NK_GT",
  /*   32 */ "NK_LE",
  /*   33 */ "NK_GE",
  /*   34 */ "NK_NE",
  /*   35 */ "NK_EQ",
  /*   36 */ "LIKE",
  /*   37 */ "MATCH",
  /*   38 */ "NMATCH",
  /*   39 */ "IN",
  /*   40 */ "FROM",
  /*   41 */ "AS",
  /*   42 */ "JOIN",
  /*   43 */ "ON",
  /*   44 */ "INNER",
  /*   45 */ "SELECT",
  /*   46 */ "DISTINCT",
  /*   47 */ "WHERE",
  /*   48 */ "PARTITION",
  /*   49 */ "BY",
  /*   50 */ "SESSION",
  /*   51 */ "STATE_WINDOW",
  /*   52 */ "INTERVAL",
  /*   53 */ "SLIDING",
  /*   54 */ "FILL",
  /*   55 */ "VALUE",
  /*   56 */ "NONE",
  /*   57 */ "PREV",
  /*   58 */ "LINEAR",
  /*   59 */ "NEXT",
  /*   60 */ "GROUP",
  /*   61 */ "HAVING",
  /*   62 */ "ORDER",
  /*   63 */ "SLIMIT",
  /*   64 */ "SOFFSET",
  /*   65 */ "LIMIT",
  /*   66 */ "OFFSET",
  /*   67 */ "ASC",
  /*   68 */ "DESC",
  /*   69 */ "NULLS",
  /*   70 */ "FIRST",
  /*   71 */ "LAST",
  /*   72 */ "cmd",
  /*   73 */ "query_expression",
  /*   74 */ "literal",
  /*   75 */ "duration_literal",
  /*   76 */ "literal_list",
  /*   77 */ "db_name",
  /*   78 */ "table_name",
  /*   79 */ "column_name",
  /*   80 */ "function_name",
  /*   81 */ "table_alias",
  /*   82 */ "column_alias",
  /*   83 */ "expression",
  /*   84 */ "column_reference",
  /*   85 */ "expression_list",
  /*   86 */ "subquery",
  /*   87 */ "predicate",
  /*   88 */ "compare_op",
  /*   89 */ "in_op",
  /*   90 */ "in_predicate_value",
  /*   91 */ "boolean_value_expression",
  /*   92 */ "boolean_primary",
  /*   93 */ "common_expression",
  /*   94 */ "from_clause",
  /*   95 */ "table_reference_list",
  /*   96 */ "table_reference",
  /*   97 */ "table_primary",
  /*   98 */ "joined_table",
  /*   99 */ "alias_opt",
  /*  100 */ "parenthesized_joined_table",
  /*  101 */ "join_type",
  /*  102 */ "search_condition",
  /*  103 */ "query_specification",
  /*  104 */ "set_quantifier_opt",
  /*  105 */ "select_list",
  /*  106 */ "where_clause_opt",
  /*  107 */ "partition_by_clause_opt",
  /*  108 */ "twindow_clause_opt",
  /*  109 */ "group_by_clause_opt",
  /*  110 */ "having_clause_opt",
  /*  111 */ "select_sublist",
  /*  112 */ "select_item",
  /*  113 */ "sliding_opt",
  /*  114 */ "fill_opt",
  /*  115 */ "fill_mode",
  /*  116 */ "group_by_list",
  /*  117 */ "query_expression_body",
  /*  118 */ "order_by_clause_opt",
  /*  119 */ "slimit_clause_opt",
  /*  120 */ "limit_clause_opt",
  /*  121 */ "query_primary",
  /*  122 */ "sort_specification_list",
  /*  123 */ "sort_specification",
  /*  124 */ "ordering_specification_opt",
  /*  125 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= SHOW DATABASES",
 /*   1 */ "cmd ::= query_expression",
 /*   2 */ "literal ::= NK_INTEGER",
 /*   3 */ "literal ::= NK_FLOAT",
 /*   4 */ "literal ::= NK_STRING",
 /*   5 */ "literal ::= NK_BOOL",
 /*   6 */ "literal ::= TIMESTAMP NK_STRING",
 /*   7 */ "literal ::= duration_literal",
 /*   8 */ "duration_literal ::= NK_VARIABLE",
 /*   9 */ "literal_list ::= literal",
 /*  10 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  11 */ "db_name ::= NK_ID",
 /*  12 */ "table_name ::= NK_ID",
 /*  13 */ "column_name ::= NK_ID",
 /*  14 */ "function_name ::= NK_ID",
 /*  15 */ "table_alias ::= NK_ID",
 /*  16 */ "column_alias ::= NK_ID",
 /*  17 */ "expression ::= literal",
 /*  18 */ "expression ::= column_reference",
 /*  19 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  20 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  21 */ "expression ::= subquery",
 /*  22 */ "expression ::= NK_LP expression NK_RP",
 /*  23 */ "expression ::= NK_PLUS expression",
 /*  24 */ "expression ::= NK_MINUS expression",
 /*  25 */ "expression ::= expression NK_PLUS expression",
 /*  26 */ "expression ::= expression NK_MINUS expression",
 /*  27 */ "expression ::= expression NK_STAR expression",
 /*  28 */ "expression ::= expression NK_SLASH expression",
 /*  29 */ "expression ::= expression NK_REM expression",
 /*  30 */ "expression_list ::= expression",
 /*  31 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  32 */ "column_reference ::= column_name",
 /*  33 */ "column_reference ::= table_name NK_DOT column_name",
 /*  34 */ "predicate ::= expression compare_op expression",
 /*  35 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  36 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  37 */ "predicate ::= expression IS NULL",
 /*  38 */ "predicate ::= expression IS NOT NULL",
 /*  39 */ "predicate ::= expression in_op in_predicate_value",
 /*  40 */ "compare_op ::= NK_LT",
 /*  41 */ "compare_op ::= NK_GT",
 /*  42 */ "compare_op ::= NK_LE",
 /*  43 */ "compare_op ::= NK_GE",
 /*  44 */ "compare_op ::= NK_NE",
 /*  45 */ "compare_op ::= NK_EQ",
 /*  46 */ "compare_op ::= LIKE",
 /*  47 */ "compare_op ::= NOT LIKE",
 /*  48 */ "compare_op ::= MATCH",
 /*  49 */ "compare_op ::= NMATCH",
 /*  50 */ "in_op ::= IN",
 /*  51 */ "in_op ::= NOT IN",
 /*  52 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /*  53 */ "boolean_value_expression ::= boolean_primary",
 /*  54 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  55 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  56 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  57 */ "boolean_primary ::= predicate",
 /*  58 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  59 */ "common_expression ::= expression",
 /*  60 */ "common_expression ::= boolean_value_expression",
 /*  61 */ "from_clause ::= FROM table_reference_list",
 /*  62 */ "table_reference_list ::= table_reference",
 /*  63 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  64 */ "table_reference ::= table_primary",
 /*  65 */ "table_reference ::= joined_table",
 /*  66 */ "table_primary ::= table_name alias_opt",
 /*  67 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /*  68 */ "table_primary ::= subquery alias_opt",
 /*  69 */ "table_primary ::= parenthesized_joined_table",
 /*  70 */ "alias_opt ::=",
 /*  71 */ "alias_opt ::= table_alias",
 /*  72 */ "alias_opt ::= AS table_alias",
 /*  73 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  74 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  75 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  76 */ "join_type ::=",
 /*  77 */ "join_type ::= INNER",
 /*  78 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  79 */ "set_quantifier_opt ::=",
 /*  80 */ "set_quantifier_opt ::= DISTINCT",
 /*  81 */ "set_quantifier_opt ::= ALL",
 /*  82 */ "select_list ::= NK_STAR",
 /*  83 */ "select_list ::= select_sublist",
 /*  84 */ "select_sublist ::= select_item",
 /*  85 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  86 */ "select_item ::= common_expression",
 /*  87 */ "select_item ::= common_expression column_alias",
 /*  88 */ "select_item ::= common_expression AS column_alias",
 /*  89 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  90 */ "where_clause_opt ::=",
 /*  91 */ "where_clause_opt ::= WHERE search_condition",
 /*  92 */ "partition_by_clause_opt ::=",
 /*  93 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /*  94 */ "twindow_clause_opt ::=",
 /*  95 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /*  96 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /*  97 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /*  98 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /*  99 */ "sliding_opt ::=",
 /* 100 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 101 */ "fill_opt ::=",
 /* 102 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 103 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 104 */ "fill_mode ::= NONE",
 /* 105 */ "fill_mode ::= PREV",
 /* 106 */ "fill_mode ::= NULL",
 /* 107 */ "fill_mode ::= LINEAR",
 /* 108 */ "fill_mode ::= NEXT",
 /* 109 */ "group_by_clause_opt ::=",
 /* 110 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 111 */ "group_by_list ::= expression",
 /* 112 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 113 */ "having_clause_opt ::=",
 /* 114 */ "having_clause_opt ::= HAVING search_condition",
 /* 115 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 116 */ "query_expression_body ::= query_primary",
 /* 117 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 118 */ "query_primary ::= query_specification",
 /* 119 */ "order_by_clause_opt ::=",
 /* 120 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 121 */ "slimit_clause_opt ::=",
 /* 122 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 123 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 124 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 125 */ "limit_clause_opt ::=",
 /* 126 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 127 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 128 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 129 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 130 */ "search_condition ::= common_expression",
 /* 131 */ "sort_specification_list ::= sort_specification",
 /* 132 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 133 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 134 */ "ordering_specification_opt ::=",
 /* 135 */ "ordering_specification_opt ::= ASC",
 /* 136 */ "ordering_specification_opt ::= DESC",
 /* 137 */ "null_ordering_opt ::=",
 /* 138 */ "null_ordering_opt ::= NULLS FIRST",
 /* 139 */ "null_ordering_opt ::= NULLS LAST",
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
    case 72: /* cmd */
    case 73: /* query_expression */
    case 74: /* literal */
    case 75: /* duration_literal */
    case 83: /* expression */
    case 84: /* column_reference */
    case 86: /* subquery */
    case 87: /* predicate */
    case 90: /* in_predicate_value */
    case 91: /* boolean_value_expression */
    case 92: /* boolean_primary */
    case 93: /* common_expression */
    case 94: /* from_clause */
    case 95: /* table_reference_list */
    case 96: /* table_reference */
    case 97: /* table_primary */
    case 98: /* joined_table */
    case 100: /* parenthesized_joined_table */
    case 102: /* search_condition */
    case 103: /* query_specification */
    case 106: /* where_clause_opt */
    case 108: /* twindow_clause_opt */
    case 110: /* having_clause_opt */
    case 112: /* select_item */
    case 113: /* sliding_opt */
    case 114: /* fill_opt */
    case 117: /* query_expression_body */
    case 119: /* slimit_clause_opt */
    case 120: /* limit_clause_opt */
    case 121: /* query_primary */
    case 123: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy56)); 
}
      break;
    case 76: /* literal_list */
    case 85: /* expression_list */
    case 105: /* select_list */
    case 107: /* partition_by_clause_opt */
    case 109: /* group_by_clause_opt */
    case 111: /* select_sublist */
    case 116: /* group_by_list */
    case 118: /* order_by_clause_opt */
    case 122: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy208)); 
}
      break;
    case 77: /* db_name */
    case 78: /* table_name */
    case 79: /* column_name */
    case 80: /* function_name */
    case 81: /* table_alias */
    case 82: /* column_alias */
    case 99: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 88: /* compare_op */
    case 89: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 101: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 104: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 115: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 124: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 125: /* null_ordering_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
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
  {   72,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   72,   -1 }, /* (1) cmd ::= query_expression */
  {   74,   -1 }, /* (2) literal ::= NK_INTEGER */
  {   74,   -1 }, /* (3) literal ::= NK_FLOAT */
  {   74,   -1 }, /* (4) literal ::= NK_STRING */
  {   74,   -1 }, /* (5) literal ::= NK_BOOL */
  {   74,   -2 }, /* (6) literal ::= TIMESTAMP NK_STRING */
  {   74,   -1 }, /* (7) literal ::= duration_literal */
  {   75,   -1 }, /* (8) duration_literal ::= NK_VARIABLE */
  {   76,   -1 }, /* (9) literal_list ::= literal */
  {   76,   -3 }, /* (10) literal_list ::= literal_list NK_COMMA literal */
  {   77,   -1 }, /* (11) db_name ::= NK_ID */
  {   78,   -1 }, /* (12) table_name ::= NK_ID */
  {   79,   -1 }, /* (13) column_name ::= NK_ID */
  {   80,   -1 }, /* (14) function_name ::= NK_ID */
  {   81,   -1 }, /* (15) table_alias ::= NK_ID */
  {   82,   -1 }, /* (16) column_alias ::= NK_ID */
  {   83,   -1 }, /* (17) expression ::= literal */
  {   83,   -1 }, /* (18) expression ::= column_reference */
  {   83,   -4 }, /* (19) expression ::= function_name NK_LP expression_list NK_RP */
  {   83,   -4 }, /* (20) expression ::= function_name NK_LP NK_STAR NK_RP */
  {   83,   -1 }, /* (21) expression ::= subquery */
  {   83,   -3 }, /* (22) expression ::= NK_LP expression NK_RP */
  {   83,   -2 }, /* (23) expression ::= NK_PLUS expression */
  {   83,   -2 }, /* (24) expression ::= NK_MINUS expression */
  {   83,   -3 }, /* (25) expression ::= expression NK_PLUS expression */
  {   83,   -3 }, /* (26) expression ::= expression NK_MINUS expression */
  {   83,   -3 }, /* (27) expression ::= expression NK_STAR expression */
  {   83,   -3 }, /* (28) expression ::= expression NK_SLASH expression */
  {   83,   -3 }, /* (29) expression ::= expression NK_REM expression */
  {   85,   -1 }, /* (30) expression_list ::= expression */
  {   85,   -3 }, /* (31) expression_list ::= expression_list NK_COMMA expression */
  {   84,   -1 }, /* (32) column_reference ::= column_name */
  {   84,   -3 }, /* (33) column_reference ::= table_name NK_DOT column_name */
  {   87,   -3 }, /* (34) predicate ::= expression compare_op expression */
  {   87,   -5 }, /* (35) predicate ::= expression BETWEEN expression AND expression */
  {   87,   -6 }, /* (36) predicate ::= expression NOT BETWEEN expression AND expression */
  {   87,   -3 }, /* (37) predicate ::= expression IS NULL */
  {   87,   -4 }, /* (38) predicate ::= expression IS NOT NULL */
  {   87,   -3 }, /* (39) predicate ::= expression in_op in_predicate_value */
  {   88,   -1 }, /* (40) compare_op ::= NK_LT */
  {   88,   -1 }, /* (41) compare_op ::= NK_GT */
  {   88,   -1 }, /* (42) compare_op ::= NK_LE */
  {   88,   -1 }, /* (43) compare_op ::= NK_GE */
  {   88,   -1 }, /* (44) compare_op ::= NK_NE */
  {   88,   -1 }, /* (45) compare_op ::= NK_EQ */
  {   88,   -1 }, /* (46) compare_op ::= LIKE */
  {   88,   -2 }, /* (47) compare_op ::= NOT LIKE */
  {   88,   -1 }, /* (48) compare_op ::= MATCH */
  {   88,   -1 }, /* (49) compare_op ::= NMATCH */
  {   89,   -1 }, /* (50) in_op ::= IN */
  {   89,   -2 }, /* (51) in_op ::= NOT IN */
  {   90,   -3 }, /* (52) in_predicate_value ::= NK_LP expression_list NK_RP */
  {   91,   -1 }, /* (53) boolean_value_expression ::= boolean_primary */
  {   91,   -2 }, /* (54) boolean_value_expression ::= NOT boolean_primary */
  {   91,   -3 }, /* (55) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {   91,   -3 }, /* (56) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {   92,   -1 }, /* (57) boolean_primary ::= predicate */
  {   92,   -3 }, /* (58) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {   93,   -1 }, /* (59) common_expression ::= expression */
  {   93,   -1 }, /* (60) common_expression ::= boolean_value_expression */
  {   94,   -2 }, /* (61) from_clause ::= FROM table_reference_list */
  {   95,   -1 }, /* (62) table_reference_list ::= table_reference */
  {   95,   -3 }, /* (63) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {   96,   -1 }, /* (64) table_reference ::= table_primary */
  {   96,   -1 }, /* (65) table_reference ::= joined_table */
  {   97,   -2 }, /* (66) table_primary ::= table_name alias_opt */
  {   97,   -4 }, /* (67) table_primary ::= db_name NK_DOT table_name alias_opt */
  {   97,   -2 }, /* (68) table_primary ::= subquery alias_opt */
  {   97,   -1 }, /* (69) table_primary ::= parenthesized_joined_table */
  {   99,    0 }, /* (70) alias_opt ::= */
  {   99,   -1 }, /* (71) alias_opt ::= table_alias */
  {   99,   -2 }, /* (72) alias_opt ::= AS table_alias */
  {  100,   -3 }, /* (73) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  100,   -3 }, /* (74) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {   98,   -6 }, /* (75) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  101,    0 }, /* (76) join_type ::= */
  {  101,   -1 }, /* (77) join_type ::= INNER */
  {  103,   -9 }, /* (78) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  104,    0 }, /* (79) set_quantifier_opt ::= */
  {  104,   -1 }, /* (80) set_quantifier_opt ::= DISTINCT */
  {  104,   -1 }, /* (81) set_quantifier_opt ::= ALL */
  {  105,   -1 }, /* (82) select_list ::= NK_STAR */
  {  105,   -1 }, /* (83) select_list ::= select_sublist */
  {  111,   -1 }, /* (84) select_sublist ::= select_item */
  {  111,   -3 }, /* (85) select_sublist ::= select_sublist NK_COMMA select_item */
  {  112,   -1 }, /* (86) select_item ::= common_expression */
  {  112,   -2 }, /* (87) select_item ::= common_expression column_alias */
  {  112,   -3 }, /* (88) select_item ::= common_expression AS column_alias */
  {  112,   -3 }, /* (89) select_item ::= table_name NK_DOT NK_STAR */
  {  106,    0 }, /* (90) where_clause_opt ::= */
  {  106,   -2 }, /* (91) where_clause_opt ::= WHERE search_condition */
  {  107,    0 }, /* (92) partition_by_clause_opt ::= */
  {  107,   -3 }, /* (93) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  108,    0 }, /* (94) twindow_clause_opt ::= */
  {  108,   -6 }, /* (95) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  108,   -4 }, /* (96) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  108,   -6 }, /* (97) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  108,   -8 }, /* (98) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  113,    0 }, /* (99) sliding_opt ::= */
  {  113,   -4 }, /* (100) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  114,    0 }, /* (101) fill_opt ::= */
  {  114,   -4 }, /* (102) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  114,   -6 }, /* (103) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  115,   -1 }, /* (104) fill_mode ::= NONE */
  {  115,   -1 }, /* (105) fill_mode ::= PREV */
  {  115,   -1 }, /* (106) fill_mode ::= NULL */
  {  115,   -1 }, /* (107) fill_mode ::= LINEAR */
  {  115,   -1 }, /* (108) fill_mode ::= NEXT */
  {  109,    0 }, /* (109) group_by_clause_opt ::= */
  {  109,   -3 }, /* (110) group_by_clause_opt ::= GROUP BY group_by_list */
  {  116,   -1 }, /* (111) group_by_list ::= expression */
  {  116,   -3 }, /* (112) group_by_list ::= group_by_list NK_COMMA expression */
  {  110,    0 }, /* (113) having_clause_opt ::= */
  {  110,   -2 }, /* (114) having_clause_opt ::= HAVING search_condition */
  {   73,   -4 }, /* (115) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  117,   -1 }, /* (116) query_expression_body ::= query_primary */
  {  117,   -4 }, /* (117) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  121,   -1 }, /* (118) query_primary ::= query_specification */
  {  118,    0 }, /* (119) order_by_clause_opt ::= */
  {  118,   -3 }, /* (120) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  119,    0 }, /* (121) slimit_clause_opt ::= */
  {  119,   -2 }, /* (122) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  119,   -4 }, /* (123) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  119,   -4 }, /* (124) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  120,    0 }, /* (125) limit_clause_opt ::= */
  {  120,   -2 }, /* (126) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  120,   -4 }, /* (127) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  120,   -4 }, /* (128) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   86,   -3 }, /* (129) subquery ::= NK_LP query_expression NK_RP */
  {  102,   -1 }, /* (130) search_condition ::= common_expression */
  {  122,   -1 }, /* (131) sort_specification_list ::= sort_specification */
  {  122,   -3 }, /* (132) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  123,   -3 }, /* (133) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  124,    0 }, /* (134) ordering_specification_opt ::= */
  {  124,   -1 }, /* (135) ordering_specification_opt ::= ASC */
  {  124,   -1 }, /* (136) ordering_specification_opt ::= DESC */
  {  125,    0 }, /* (137) null_ordering_opt ::= */
  {  125,   -2 }, /* (138) null_ordering_opt ::= NULLS FIRST */
  {  125,   -2 }, /* (139) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= SHOW DATABASES */
{ PARSER_TRACE; createShowStmt(pCxt, SHOW_TYPE_DATABASE); }
        break;
      case 1: /* cmd ::= query_expression */
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy56; }
        break;
      case 2: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 3: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 4: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 5: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 6: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 7: /* literal ::= duration_literal */
      case 17: /* expression ::= literal */ yytestcase(yyruleno==17);
      case 18: /* expression ::= column_reference */ yytestcase(yyruleno==18);
      case 21: /* expression ::= subquery */ yytestcase(yyruleno==21);
      case 53: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==53);
      case 57: /* boolean_primary ::= predicate */ yytestcase(yyruleno==57);
      case 62: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==62);
      case 64: /* table_reference ::= table_primary */ yytestcase(yyruleno==64);
      case 65: /* table_reference ::= joined_table */ yytestcase(yyruleno==65);
      case 69: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==69);
      case 116: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==116);
      case 118: /* query_primary ::= query_specification */ yytestcase(yyruleno==118);
{ PARSER_TRACE; yylhsminor.yy56 = yymsp[0].minor.yy56; }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 8: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 9: /* literal_list ::= literal */
      case 30: /* expression_list ::= expression */ yytestcase(yyruleno==30);
{ PARSER_TRACE; yylhsminor.yy208 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy56)); }
  yymsp[0].minor.yy208 = yylhsminor.yy208;
        break;
      case 10: /* literal_list ::= literal_list NK_COMMA literal */
      case 31: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==31);
{ PARSER_TRACE; yylhsminor.yy208 = addNodeToList(pCxt, yymsp[-2].minor.yy208, releaseRawExprNode(pCxt, yymsp[0].minor.yy56)); }
  yymsp[-2].minor.yy208 = yylhsminor.yy208;
        break;
      case 11: /* db_name ::= NK_ID */
      case 12: /* table_name ::= NK_ID */ yytestcase(yyruleno==12);
      case 13: /* column_name ::= NK_ID */ yytestcase(yyruleno==13);
      case 14: /* function_name ::= NK_ID */ yytestcase(yyruleno==14);
      case 15: /* table_alias ::= NK_ID */ yytestcase(yyruleno==15);
      case 16: /* column_alias ::= NK_ID */ yytestcase(yyruleno==16);
{ PARSER_TRACE; yylhsminor.yy29 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy29 = yylhsminor.yy29;
        break;
      case 19: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy29, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy29, yymsp[-1].minor.yy208)); }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 20: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy29, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy29, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 22: /* expression ::= NK_LP expression NK_RP */
      case 58: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==58);
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy56)); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 23: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy56));
                                                                                  }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 24: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy56), NULL));
                                                                                  }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 25: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); 
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 26: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); 
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 27: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); 
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 28: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); 
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 29: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); 
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 32: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNode(pCxt, &yymsp[0].minor.yy29, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy29)); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 33: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy29, &yymsp[0].minor.yy29, createColumnNode(pCxt, &yymsp[-2].minor.yy29, &yymsp[0].minor.yy29)); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 34: /* predicate ::= expression compare_op expression */
      case 39: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==39);
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy128, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56)));
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 35: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy56), releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56)));
                                                                                  }
  yymsp[-4].minor.yy56 = yylhsminor.yy56;
        break;
      case 36: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[-5].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56)));
                                                                                  }
  yymsp[-5].minor.yy56 = yylhsminor.yy56;
        break;
      case 37: /* predicate ::= expression IS NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), NULL));
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 38: /* predicate ::= expression IS NOT NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy56), NULL));
                                                                                  }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 40: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_LOWER_THAN; }
        break;
      case 41: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_GREATER_THAN; }
        break;
      case 42: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 43: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 44: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_NOT_EQUAL; }
        break;
      case 45: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_EQUAL; }
        break;
      case 46: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_LIKE; }
        break;
      case 47: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy128 = OP_TYPE_NOT_LIKE; }
        break;
      case 48: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_MATCH; }
        break;
      case 49: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_NMATCH; }
        break;
      case 50: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy128 = OP_TYPE_IN; }
        break;
      case 51: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy128 = OP_TYPE_NOT_IN; }
        break;
      case 52: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy208)); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 54: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy56), NULL));
                                                                                  }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 55: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56)));
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 56: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy56);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), releaseRawExprNode(pCxt, yymsp[0].minor.yy56)));
                                                                                  }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 59: /* common_expression ::= expression */
      case 60: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==60);
{ yylhsminor.yy56 = yymsp[0].minor.yy56; }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 61: /* from_clause ::= FROM table_reference_list */
      case 91: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==91);
      case 114: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==114);
{ PARSER_TRACE; yymsp[-1].minor.yy56 = yymsp[0].minor.yy56; }
        break;
      case 63: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy56 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy56, yymsp[0].minor.yy56, NULL); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 66: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy56 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy29, &yymsp[0].minor.yy29); }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 67: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy56 = createRealTableNode(pCxt, &yymsp[-3].minor.yy29, &yymsp[-1].minor.yy29, &yymsp[0].minor.yy29); }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 68: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy56 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy56), &yymsp[0].minor.yy29); }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 70: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy29 = nil_token;  }
        break;
      case 71: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy29 = yymsp[0].minor.yy29; }
  yymsp[0].minor.yy29 = yylhsminor.yy29;
        break;
      case 72: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy29 = yymsp[0].minor.yy29; }
        break;
      case 73: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 74: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==74);
{ PARSER_TRACE; yymsp[-2].minor.yy56 = yymsp[-1].minor.yy56; }
        break;
      case 75: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy56 = createJoinTableNode(pCxt, yymsp[-4].minor.yy36, yymsp[-5].minor.yy56, yymsp[-2].minor.yy56, yymsp[0].minor.yy56); }
  yymsp[-5].minor.yy56 = yylhsminor.yy56;
        break;
      case 76: /* join_type ::= */
{ PARSER_TRACE; yymsp[1].minor.yy36 = JOIN_TYPE_INNER; }
        break;
      case 77: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy36 = JOIN_TYPE_INNER; }
        break;
      case 78: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy56 = createSelectStmt(pCxt, yymsp[-7].minor.yy173, yymsp[-6].minor.yy208, yymsp[-5].minor.yy56);
                                                                                    yymsp[-8].minor.yy56 = addWhereClause(pCxt, yymsp[-8].minor.yy56, yymsp[-4].minor.yy56);
                                                                                    yymsp[-8].minor.yy56 = addPartitionByClause(pCxt, yymsp[-8].minor.yy56, yymsp[-3].minor.yy208);
                                                                                    yymsp[-8].minor.yy56 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy56, yymsp[-2].minor.yy56);
                                                                                    yymsp[-8].minor.yy56 = addGroupByClause(pCxt, yymsp[-8].minor.yy56, yymsp[-1].minor.yy208);
                                                                                    yymsp[-8].minor.yy56 = addHavingClause(pCxt, yymsp[-8].minor.yy56, yymsp[0].minor.yy56);
                                                                                  }
        break;
      case 79: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy173 = false; }
        break;
      case 80: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy173 = true; }
        break;
      case 81: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy173 = false; }
        break;
      case 82: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy208 = NULL; }
        break;
      case 83: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy208 = yymsp[0].minor.yy208; }
  yymsp[0].minor.yy208 = yylhsminor.yy208;
        break;
      case 84: /* select_sublist ::= select_item */
      case 131: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==131);
{ PARSER_TRACE; yylhsminor.yy208 = createNodeList(pCxt, yymsp[0].minor.yy56); }
  yymsp[0].minor.yy208 = yylhsminor.yy208;
        break;
      case 85: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 132: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==132);
{ PARSER_TRACE; yylhsminor.yy208 = addNodeToList(pCxt, yymsp[-2].minor.yy208, yymsp[0].minor.yy56); }
  yymsp[-2].minor.yy208 = yylhsminor.yy208;
        break;
      case 86: /* select_item ::= common_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy56);
                                                                                    yylhsminor.yy56 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy56), &t);
                                                                                  }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 87: /* select_item ::= common_expression column_alias */
{ PARSER_TRACE; yylhsminor.yy56 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy56), &yymsp[0].minor.yy29); }
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 88: /* select_item ::= common_expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy56 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), &yymsp[0].minor.yy29); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 89: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy56 = createColumnNode(pCxt, &yymsp[-2].minor.yy29, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 90: /* where_clause_opt ::= */
      case 94: /* twindow_clause_opt ::= */ yytestcase(yyruleno==94);
      case 99: /* sliding_opt ::= */ yytestcase(yyruleno==99);
      case 101: /* fill_opt ::= */ yytestcase(yyruleno==101);
      case 113: /* having_clause_opt ::= */ yytestcase(yyruleno==113);
      case 121: /* slimit_clause_opt ::= */ yytestcase(yyruleno==121);
      case 125: /* limit_clause_opt ::= */ yytestcase(yyruleno==125);
{ PARSER_TRACE; yymsp[1].minor.yy56 = NULL; }
        break;
      case 92: /* partition_by_clause_opt ::= */
      case 109: /* group_by_clause_opt ::= */ yytestcase(yyruleno==109);
      case 119: /* order_by_clause_opt ::= */ yytestcase(yyruleno==119);
{ PARSER_TRACE; yymsp[1].minor.yy208 = NULL; }
        break;
      case 93: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 110: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==110);
      case 120: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==120);
{ PARSER_TRACE; yymsp[-2].minor.yy208 = yymsp[0].minor.yy208; }
        break;
      case 95: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy56 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy56), &yymsp[-1].minor.yy0); }
        break;
      case 96: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy56 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy56)); }
        break;
      case 97: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy56 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy56, NULL, yymsp[-1].minor.yy56, yymsp[0].minor.yy56); }
        break;
      case 98: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy56 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy56, yymsp[-3].minor.yy56, yymsp[-1].minor.yy56, yymsp[0].minor.yy56); }
        break;
      case 100: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy56 = yymsp[-1].minor.yy56; }
        break;
      case 102: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy56 = createFillNode(pCxt, yymsp[-1].minor.yy18, NULL); }
        break;
      case 103: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy56 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy208)); }
        break;
      case 104: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy18 = FILL_MODE_NONE; }
        break;
      case 105: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy18 = FILL_MODE_PREV; }
        break;
      case 106: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy18 = FILL_MODE_NULL; }
        break;
      case 107: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy18 = FILL_MODE_LINEAR; }
        break;
      case 108: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy18 = FILL_MODE_NEXT; }
        break;
      case 111: /* group_by_list ::= expression */
{ PARSER_TRACE; yylhsminor.yy208 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); }
  yymsp[0].minor.yy208 = yylhsminor.yy208;
        break;
      case 112: /* group_by_list ::= group_by_list NK_COMMA expression */
{ PARSER_TRACE; yylhsminor.yy208 = addNodeToList(pCxt, yymsp[-2].minor.yy208, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy56))); }
  yymsp[-2].minor.yy208 = yylhsminor.yy208;
        break;
      case 115: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy56 = addOrderByClause(pCxt, yymsp[-3].minor.yy56, yymsp[-2].minor.yy208);
                                                                                    yylhsminor.yy56 = addSlimitClause(pCxt, yylhsminor.yy56, yymsp[-1].minor.yy56);
                                                                                    yylhsminor.yy56 = addLimitClause(pCxt, yylhsminor.yy56, yymsp[0].minor.yy56);
                                                                                  }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 117: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy56 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy56, yymsp[0].minor.yy56); }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 122: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 126: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==126);
{ PARSER_TRACE; yymsp[-1].minor.yy56 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 123: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 127: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==127);
{ PARSER_TRACE; yymsp[-3].minor.yy56 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 124: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 128: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==128);
{ PARSER_TRACE; yymsp[-3].minor.yy56 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 129: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy56 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy56); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 130: /* search_condition ::= common_expression */
{ PARSER_TRACE; yylhsminor.yy56 = releaseRawExprNode(pCxt, yymsp[0].minor.yy56); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 133: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy56 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy56), yymsp[-1].minor.yy218, yymsp[0].minor.yy109); }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 134: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy218 = ORDER_ASC; }
        break;
      case 135: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy218 = ORDER_ASC; }
        break;
      case 136: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy218 = ORDER_DESC; }
        break;
      case 137: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy109 = NULL_ORDER_DEFAULT; }
        break;
      case 138: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy109 = NULL_ORDER_FIRST; }
        break;
      case 139: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy109 = NULL_ORDER_LAST; }
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
 PARSER_COMPLETE; 
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
