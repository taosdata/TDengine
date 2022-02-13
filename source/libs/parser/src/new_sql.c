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
#define YYNOCODE 124
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EOrder yy10;
  EFillMode yy14;
  SNode* yy168;
  ENullOrder yy177;
  SNodeList* yy192;
  bool yy209;
  EOperatorType yy228;
  EJoinType yy229;
  SToken yy241;
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
#define YYNSTATE             143
#define YYNRULE              134
#define YYNTOKEN             72
#define YY_MAX_SHIFT         142
#define YY_MIN_SHIFTREDUCE   238
#define YY_MAX_SHIFTREDUCE   371
#define YY_ERROR_ACTION      372
#define YY_ACCEPT_ACTION     373
#define YY_NO_ACTION         374
#define YY_MIN_REDUCE        375
#define YY_MAX_REDUCE        508
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
#define YY_ACTTAB_COUNT (705)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   384,  382,   89,   22,   64,  385,  382,  458,   29,   27,
 /*    10 */    25,   24,   23,  392,  382,  137,  406,  126,  406,  138,
 /*    20 */    71,  109,   44,  393,  259,  395,   21,   84,   93,  142,
 /*    30 */   277,  278,  279,  280,  281,  282,  283,  285,  286,  287,
 /*    40 */    29,   27,   25,   24,   23,   25,   24,   23,  125,    9,
 /*    50 */   456,  240,  241,  242,  243,  139,  246,  106,   21,   84,
 /*    60 */    93,   51,  277,  278,  279,  280,  281,  282,  283,  285,
 /*    70 */   286,  287,  127,  392,  382,  137,  406,  137,  406,  138,
 /*    80 */   315,  113,   38,  393,  114,  395,  431,  253,   28,   26,
 /*    90 */    83,  427,  487,  443,  131,  240,  241,  242,  243,  139,
 /*   100 */   246,  487,   98,    1,  340,  486,   67,   10,   51,  485,
 /*   110 */   440,  392,  382,   60,   50,  137,  406,  138,  485,  123,
 /*   120 */    39,  393,  314,  395,  431,   51,  125,    9,   91,  427,
 /*   130 */   105,  338,  339,  341,  342,  392,  382,  132,  462,  137,
 /*   140 */   406,  138,    4,  311,   39,  393,  407,  395,  431,   51,
 /*   150 */   443,  443,   91,  427,   29,   27,   25,   24,   23,  323,
 /*   160 */   392,  382,  483,   73,  137,  406,  138,  439,  438,   39,
 /*   170 */   393,  251,  395,  431,  389,   18,  387,   91,  427,    7,
 /*   180 */     6,   29,   27,   25,   24,   23,  134,  447,  127,  392,
 /*   190 */   382,    7,    6,  137,  406,  138,   28,   26,   77,  393,
 /*   200 */     8,  395,  295,  240,  241,  242,  243,  139,  246,  107,
 /*   210 */    98,    1,   28,   26,  469,   10,  104,  487,   52,  240,
 /*   220 */   241,  242,  243,  139,  246,  246,   98,    5,  102,   40,
 /*   230 */    50,  135,  337,   19,  485,  392,  382,  130,  468,  137,
 /*   240 */   406,  138,  103,  284,   39,  393,  288,  395,  431,   51,
 /*   250 */    56,   54,  430,  427,   57,  370,  371,   29,   27,   25,
 /*   260 */    24,   23,   30,  392,  382,  289,   90,  137,  406,  138,
 /*   270 */     3,  254,   39,  393,  117,  395,  431,   28,   26,  316,
 /*   280 */   118,  427,   47,  449,  240,  241,  242,  243,  139,  246,
 /*   290 */    70,   98,    5,  101,  392,  382,  129,  127,  126,  406,
 /*   300 */   138,  122,   43,   44,  393,  119,  395,  274,   30,   59,
 /*   310 */    41,  257,    2,   29,   27,   25,   24,   23,   61,   65,
 /*   320 */   436,  121,   15,  120,   69,  311,  487,   20,  413,  250,
 /*   330 */    99,  455,   42,   29,   27,   25,   24,   23,  253,   50,
 /*   340 */    28,   26,  444,  485,   31,   62,  254,  240,  241,  242,
 /*   350 */   243,  139,  246,  459,   98,    1,  392,  382,   94,  502,
 /*   360 */   137,  406,  138,  136,  484,   39,  393,   72,  395,  431,
 /*   370 */    28,   26,  367,  368,  428,  133,  251,  240,  241,  242,
 /*   380 */   243,  139,  246,   13,   98,    5,   29,   27,   25,   24,
 /*   390 */    23,  115,  110,  108,  392,  382,   12,   30,  137,  406,
 /*   400 */   138,   53,  259,   45,  393,  334,  395,   55,   34,  336,
 /*   410 */   330,   46,   58,   35,  392,  382,  373,  140,  137,  406,
 /*   420 */   138,  329,  111,   81,  393,  100,  395,  392,  382,  112,
 /*   430 */   387,  137,  406,  138,   36,    6,   81,  393,  116,  395,
 /*   440 */   128,  500,   14,  392,  382,  275,  487,  137,  406,  138,
 /*   450 */   309,  308,   81,  393,   92,  395,  392,  382,   33,   50,
 /*   460 */   137,  406,  138,  485,   66,   45,  393,   32,  395,  392,
 /*   470 */   382,  386,   49,  137,  406,  138,  361,   16,   81,  393,
 /*   480 */    97,  395,   37,   11,  356,  355,   74,   95,  360,  392,
 /*   490 */   382,  359,   96,  137,  406,  138,  244,   17,   78,  393,
 /*   500 */   376,  395,  375,  501,  374,  141,  392,  382,  374,  374,
 /*   510 */   137,  406,  138,  374,  374,   75,  393,  374,  395,  392,
 /*   520 */   382,  374,  374,  137,  406,  138,  374,  374,   79,  393,
 /*   530 */   374,  395,  392,  382,  374,  374,  137,  406,  138,  374,
 /*   540 */   374,   76,  393,  374,  395,  392,  382,  374,  374,  137,
 /*   550 */   406,  138,  374,  374,   80,  393,  374,  395,  374,  374,
 /*   560 */   374,  374,  374,  374,  392,  382,  374,  374,  137,  406,
 /*   570 */   138,  374,  374,  403,  393,  374,  395,  374,  392,  382,
 /*   580 */   374,  374,  137,  406,  138,  374,  374,  402,  393,  374,
 /*   590 */   395,  392,  382,  374,  374,  137,  406,  138,  374,  374,
 /*   600 */   401,  393,  374,  395,  392,  382,  374,  374,  137,  406,
 /*   610 */   138,  374,  374,   87,  393,  374,  395,  392,  382,  374,
 /*   620 */   374,  137,  406,  138,  374,  374,   86,  393,  374,  395,
 /*   630 */   392,  382,  374,  374,  137,  406,  138,  374,  374,   88,
 /*   640 */   393,  374,  395,  392,  382,  374,  374,  137,  406,  138,
 /*   650 */   374,  374,   85,  393,  374,  395,  374,  392,  382,  374,
 /*   660 */   374,  137,  406,  138,  374,  374,   82,  393,  374,  395,
 /*   670 */   122,   43,  374,  374,  374,  122,   43,  374,  374,   41,
 /*   680 */   374,  374,  122,   43,   41,  374,  374,  124,   63,  436,
 /*   690 */   437,   41,  441,   48,  436,  437,  374,  441,  374,  374,
 /*   700 */    68,  436,  437,  374,  441,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    74,   75,   76,   88,   89,   74,   75,   82,    8,    9,
 /*    10 */    10,   11,   12,   74,   75,   78,   79,   78,   79,   80,
 /*    20 */   122,   84,   83,   84,   24,   86,   26,   27,   28,   13,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     8,    9,   10,   11,   12,   10,   11,   12,   22,   23,
 /*    50 */   111,   15,   16,   17,   18,   19,   20,  114,   26,   27,
 /*    60 */    28,   45,   30,   31,   32,   33,   34,   35,   36,   37,
 /*    70 */    38,   39,   73,   74,   75,   78,   79,   78,   79,   80,
 /*    80 */     4,   84,   83,   84,   22,   86,   87,   22,    8,    9,
 /*    90 */    91,   92,  102,   81,   21,   15,   16,   17,   18,   19,
 /*   100 */    20,  102,   22,   23,   29,  115,   41,   27,   45,  119,
 /*   110 */    98,   74,   75,  107,  115,   78,   79,   80,  119,  100,
 /*   120 */    83,   84,   46,   86,   87,   45,   22,   23,   91,   92,
 /*   130 */    55,   56,   57,   58,   59,   74,   75,   64,  101,   78,
 /*   140 */    79,   80,   43,   44,   83,   84,   79,   86,   87,   45,
 /*   150 */    81,   81,   91,   92,    8,    9,   10,   11,   12,   10,
 /*   160 */    74,   75,  101,  116,   78,   79,   80,   98,   98,   83,
 /*   170 */    84,   22,   86,   87,   23,    2,   25,   91,   92,    1,
 /*   180 */     2,    8,    9,   10,   11,   12,   21,  101,   73,   74,
 /*   190 */    75,    1,    2,   78,   79,   80,    8,    9,   83,   84,
 /*   200 */   103,   86,   24,   15,   16,   17,   18,   19,   20,   54,
 /*   210 */    22,   23,    8,    9,  113,   27,   53,  102,  112,   15,
 /*   220 */    16,   17,   18,   19,   20,   20,   22,   23,   75,   21,
 /*   230 */   115,   66,   24,   26,  119,   74,   75,    3,  113,   78,
 /*   240 */    79,   80,   75,   36,   83,   84,   39,   86,   87,   45,
 /*   250 */   112,   21,   91,   92,   24,   70,   71,    8,    9,   10,
 /*   260 */    11,   12,   21,   74,   75,   24,   75,   78,   79,   80,
 /*   270 */    61,   22,   83,   84,   60,   86,   87,    8,    9,   10,
 /*   280 */    91,   92,  106,  109,   15,   16,   17,   18,   19,   20,
 /*   290 */    41,   22,   23,   48,   74,   75,   62,   73,   78,   79,
 /*   300 */    80,   77,   78,   83,   84,   27,   86,   29,   21,  108,
 /*   310 */    86,   24,   47,    8,    9,   10,   11,   12,  105,   95,
 /*   320 */    96,   97,   23,   99,  104,   44,  102,    2,   90,   22,
 /*   330 */   110,  111,   78,    8,    9,   10,   11,   12,   22,  115,
 /*   340 */     8,    9,   81,  119,   40,   93,   22,   15,   16,   17,
 /*   350 */    18,   19,   20,   82,   22,   23,   74,   75,   69,  123,
 /*   360 */    78,   79,   80,   65,  118,   83,   84,  117,   86,   87,
 /*   370 */     8,    9,   67,   68,   92,   63,   22,   15,   16,   17,
 /*   380 */    18,   19,   20,   49,   22,   23,    8,    9,   10,   11,
 /*   390 */    12,   50,   51,   52,   74,   75,   21,   21,   78,   79,
 /*   400 */    80,   24,   24,   83,   84,   24,   86,   23,   21,   24,
 /*   410 */    24,   23,   23,   23,   74,   75,   72,   73,   78,   79,
 /*   420 */    80,   24,   15,   83,   84,   85,   86,   74,   75,   21,
 /*   430 */    25,   78,   79,   80,   23,    2,   83,   84,   85,   86,
 /*   440 */   120,  121,   49,   74,   75,   29,  102,   78,   79,   80,
 /*   450 */    24,   24,   83,   84,   85,   86,   74,   75,   21,  115,
 /*   460 */    78,   79,   80,  119,   25,   83,   84,   42,   86,   74,
 /*   470 */    75,   25,   25,   78,   79,   80,   24,   21,   83,   84,
 /*   480 */    85,   86,    4,   49,   15,   15,   25,   15,   15,   74,
 /*   490 */    75,   15,   15,   78,   79,   80,   17,   23,   83,   84,
 /*   500 */     0,   86,    0,  121,  124,   14,   74,   75,  124,  124,
 /*   510 */    78,   79,   80,  124,  124,   83,   84,  124,   86,   74,
 /*   520 */    75,  124,  124,   78,   79,   80,  124,  124,   83,   84,
 /*   530 */   124,   86,   74,   75,  124,  124,   78,   79,   80,  124,
 /*   540 */   124,   83,   84,  124,   86,   74,   75,  124,  124,   78,
 /*   550 */    79,   80,  124,  124,   83,   84,  124,   86,  124,  124,
 /*   560 */   124,  124,  124,  124,   74,   75,  124,  124,   78,   79,
 /*   570 */    80,  124,  124,   83,   84,  124,   86,  124,   74,   75,
 /*   580 */   124,  124,   78,   79,   80,  124,  124,   83,   84,  124,
 /*   590 */    86,   74,   75,  124,  124,   78,   79,   80,  124,  124,
 /*   600 */    83,   84,  124,   86,   74,   75,  124,  124,   78,   79,
 /*   610 */    80,  124,  124,   83,   84,  124,   86,   74,   75,  124,
 /*   620 */   124,   78,   79,   80,  124,  124,   83,   84,  124,   86,
 /*   630 */    74,   75,  124,  124,   78,   79,   80,  124,  124,   83,
 /*   640 */    84,  124,   86,   74,   75,  124,  124,   78,   79,   80,
 /*   650 */   124,  124,   83,   84,  124,   86,  124,   74,   75,  124,
 /*   660 */   124,   78,   79,   80,  124,  124,   83,   84,  124,   86,
 /*   670 */    77,   78,  124,  124,  124,   77,   78,  124,  124,   86,
 /*   680 */   124,  124,   77,   78,   86,  124,  124,   94,   95,   96,
 /*   690 */    97,   86,   99,   95,   96,   97,  124,   99,  124,  124,
 /*   700 */    95,   96,   97,  124,   99,
};
#define YY_SHIFT_COUNT    (142)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (502)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    16,   80,  188,  188,  188,  204,  188,  188,  269,  104,
 /*    10 */   332,  362,  362,  362,  362,  362,  362,  362,  362,  362,
 /*    20 */   362,  362,  362,  362,  362,  362,  362,  362,  362,  362,
 /*    30 */   362,   26,   26,   26,   36,   62,   62,   63,    0,   32,
 /*    40 */    36,   65,   65,   65,  249,  305,   75,  341,   99,  149,
 /*    50 */   234,   76,  155,  163,  205,  205,  155,  163,  205,  209,
 /*    60 */   214,  245,  265,  281,  299,  281,  307,  316,  281,  304,
 /*    70 */   324,  289,  298,  312,  354,  173,  325,  378,  146,  146,
 /*    80 */   146,  146,  146,  178,  207,   35,   35,   35,   35,  208,
 /*    90 */   230,  190,  241,  278,  185,   73,  165,  287,  151,  375,
 /*   100 */   376,  334,  377,  381,  384,  387,  385,  388,  389,  386,
 /*   110 */   390,  397,  407,  408,  405,  411,  376,  393,  433,  416,
 /*   120 */   426,  427,  439,  425,  437,  446,  447,  452,  456,  434,
 /*   130 */   478,  469,  470,  472,  473,  476,  477,  461,  474,  479,
 /*   140 */   500,  502,  491,
};
#define YY_REDUCE_COUNT (74)
#define YY_REDUCE_MIN   (-102)
#define YY_REDUCE_MAX   (605)
static const short yy_reduce_ofst[] = {
 /*     0 */   344,   -1,   37,   61,   86,  115,  161,  189,  220,  224,
 /*    10 */   282,  320,  -61,  340,  353,  369,  382,  395,  415,  432,
 /*    20 */   445,  458,  471,  490,  504,  517,  530,  543,  556,  569,
 /*    30 */   583,  593,  598,  605,  -74,  -63,   -3,  -10,  -85,  -85,
 /*    40 */   -69,   12,   69,   70,  -75, -102,  -57,    6,   19,   67,
 /*    50 */    47,   97,  101,  106,  153,  167,  125,  138,  191,  174,
 /*    60 */   201,  176,  213,   19,  238,   19,  254,  261,   19,  252,
 /*    70 */   271,  236,  246,  250,   67,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   372,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*    10 */   372,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*    20 */   372,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*    30 */   372,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*    40 */   372,  442,  442,  442,  457,  503,  372,  465,  372,  372,
 /*    50 */   488,  450,  472,  470,  372,  372,  472,  470,  372,  482,
 /*    60 */   480,  463,  461,  434,  372,  372,  372,  372,  435,  372,
 /*    70 */   372,  506,  494,  490,  372,  372,  372,  372,  410,  409,
 /*    80 */   408,  404,  405,  372,  372,  399,  400,  398,  397,  372,
 /*    90 */   372,  499,  372,  372,  372,  491,  495,  372,  388,  454,
 /*   100 */   464,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*   110 */   372,  372,  372,  372,  388,  372,  481,  372,  429,  372,
 /*   120 */   441,  437,  372,  372,  433,  387,  372,  372,  489,  372,
 /*   130 */   372,  372,  372,  372,  372,  372,  372,  372,  372,  372,
 /*   140 */   372,  372,  372,
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
  /*   93 */ "from_clause",
  /*   94 */ "table_reference_list",
  /*   95 */ "table_reference",
  /*   96 */ "table_primary",
  /*   97 */ "joined_table",
  /*   98 */ "alias_opt",
  /*   99 */ "parenthesized_joined_table",
  /*  100 */ "join_type",
  /*  101 */ "search_condition",
  /*  102 */ "query_specification",
  /*  103 */ "set_quantifier_opt",
  /*  104 */ "select_list",
  /*  105 */ "where_clause_opt",
  /*  106 */ "partition_by_clause_opt",
  /*  107 */ "twindow_clause_opt",
  /*  108 */ "group_by_clause_opt",
  /*  109 */ "having_clause_opt",
  /*  110 */ "select_sublist",
  /*  111 */ "select_item",
  /*  112 */ "sliding_opt",
  /*  113 */ "fill_opt",
  /*  114 */ "fill_mode",
  /*  115 */ "query_expression_body",
  /*  116 */ "order_by_clause_opt",
  /*  117 */ "slimit_clause_opt",
  /*  118 */ "limit_clause_opt",
  /*  119 */ "query_primary",
  /*  120 */ "sort_specification_list",
  /*  121 */ "sort_specification",
  /*  122 */ "ordering_specification_opt",
  /*  123 */ "null_ordering_opt",
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
 /*  20 */ "expression ::= subquery",
 /*  21 */ "expression ::= NK_LP expression NK_RP",
 /*  22 */ "expression ::= NK_PLUS expression",
 /*  23 */ "expression ::= NK_MINUS expression",
 /*  24 */ "expression ::= expression NK_PLUS expression",
 /*  25 */ "expression ::= expression NK_MINUS expression",
 /*  26 */ "expression ::= expression NK_STAR expression",
 /*  27 */ "expression ::= expression NK_SLASH expression",
 /*  28 */ "expression ::= expression NK_REM expression",
 /*  29 */ "expression_list ::= expression",
 /*  30 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  31 */ "column_reference ::= column_name",
 /*  32 */ "column_reference ::= table_name NK_DOT column_name",
 /*  33 */ "predicate ::= expression compare_op expression",
 /*  34 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  35 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  36 */ "predicate ::= expression IS NULL",
 /*  37 */ "predicate ::= expression IS NOT NULL",
 /*  38 */ "predicate ::= expression in_op in_predicate_value",
 /*  39 */ "compare_op ::= NK_LT",
 /*  40 */ "compare_op ::= NK_GT",
 /*  41 */ "compare_op ::= NK_LE",
 /*  42 */ "compare_op ::= NK_GE",
 /*  43 */ "compare_op ::= NK_NE",
 /*  44 */ "compare_op ::= NK_EQ",
 /*  45 */ "compare_op ::= LIKE",
 /*  46 */ "compare_op ::= NOT LIKE",
 /*  47 */ "compare_op ::= MATCH",
 /*  48 */ "compare_op ::= NMATCH",
 /*  49 */ "in_op ::= IN",
 /*  50 */ "in_op ::= NOT IN",
 /*  51 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /*  52 */ "boolean_value_expression ::= boolean_primary",
 /*  53 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  54 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  55 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  56 */ "boolean_primary ::= predicate",
 /*  57 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  58 */ "from_clause ::= FROM table_reference_list",
 /*  59 */ "table_reference_list ::= table_reference",
 /*  60 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  61 */ "table_reference ::= table_primary",
 /*  62 */ "table_reference ::= joined_table",
 /*  63 */ "table_primary ::= table_name alias_opt",
 /*  64 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /*  65 */ "table_primary ::= subquery alias_opt",
 /*  66 */ "table_primary ::= parenthesized_joined_table",
 /*  67 */ "alias_opt ::=",
 /*  68 */ "alias_opt ::= table_alias",
 /*  69 */ "alias_opt ::= AS table_alias",
 /*  70 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  71 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  72 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  73 */ "join_type ::= INNER",
 /*  74 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  75 */ "set_quantifier_opt ::=",
 /*  76 */ "set_quantifier_opt ::= DISTINCT",
 /*  77 */ "set_quantifier_opt ::= ALL",
 /*  78 */ "select_list ::= NK_STAR",
 /*  79 */ "select_list ::= select_sublist",
 /*  80 */ "select_sublist ::= select_item",
 /*  81 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  82 */ "select_item ::= expression",
 /*  83 */ "select_item ::= expression column_alias",
 /*  84 */ "select_item ::= expression AS column_alias",
 /*  85 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  86 */ "where_clause_opt ::=",
 /*  87 */ "where_clause_opt ::= WHERE search_condition",
 /*  88 */ "partition_by_clause_opt ::=",
 /*  89 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /*  90 */ "twindow_clause_opt ::=",
 /*  91 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /*  92 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /*  93 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /*  94 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /*  95 */ "sliding_opt ::=",
 /*  96 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /*  97 */ "fill_opt ::=",
 /*  98 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /*  99 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 100 */ "fill_mode ::= NONE",
 /* 101 */ "fill_mode ::= PREV",
 /* 102 */ "fill_mode ::= NULL",
 /* 103 */ "fill_mode ::= LINEAR",
 /* 104 */ "fill_mode ::= NEXT",
 /* 105 */ "group_by_clause_opt ::=",
 /* 106 */ "group_by_clause_opt ::= GROUP BY expression_list",
 /* 107 */ "having_clause_opt ::=",
 /* 108 */ "having_clause_opt ::= HAVING search_condition",
 /* 109 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 110 */ "query_expression_body ::= query_primary",
 /* 111 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 112 */ "query_primary ::= query_specification",
 /* 113 */ "order_by_clause_opt ::=",
 /* 114 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 115 */ "slimit_clause_opt ::=",
 /* 116 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 117 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 118 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 119 */ "limit_clause_opt ::=",
 /* 120 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 121 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 122 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 123 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 124 */ "search_condition ::= boolean_value_expression",
 /* 125 */ "sort_specification_list ::= sort_specification",
 /* 126 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 127 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 128 */ "ordering_specification_opt ::=",
 /* 129 */ "ordering_specification_opt ::= ASC",
 /* 130 */ "ordering_specification_opt ::= DESC",
 /* 131 */ "null_ordering_opt ::=",
 /* 132 */ "null_ordering_opt ::= NULLS FIRST",
 /* 133 */ "null_ordering_opt ::= NULLS LAST",
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
    case 93: /* from_clause */
    case 94: /* table_reference_list */
    case 95: /* table_reference */
    case 96: /* table_primary */
    case 97: /* joined_table */
    case 99: /* parenthesized_joined_table */
    case 101: /* search_condition */
    case 102: /* query_specification */
    case 105: /* where_clause_opt */
    case 107: /* twindow_clause_opt */
    case 109: /* having_clause_opt */
    case 111: /* select_item */
    case 112: /* sliding_opt */
    case 113: /* fill_opt */
    case 115: /* query_expression_body */
    case 117: /* slimit_clause_opt */
    case 118: /* limit_clause_opt */
    case 119: /* query_primary */
    case 121: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy168)); 
}
      break;
    case 76: /* literal_list */
    case 85: /* expression_list */
    case 104: /* select_list */
    case 106: /* partition_by_clause_opt */
    case 108: /* group_by_clause_opt */
    case 110: /* select_sublist */
    case 116: /* order_by_clause_opt */
    case 120: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy192)); 
}
      break;
    case 77: /* db_name */
    case 78: /* table_name */
    case 79: /* column_name */
    case 80: /* function_name */
    case 81: /* table_alias */
    case 82: /* column_alias */
    case 98: /* alias_opt */
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
    case 100: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 103: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 114: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 122: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 123: /* null_ordering_opt */
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
  {   83,   -1 }, /* (20) expression ::= subquery */
  {   83,   -3 }, /* (21) expression ::= NK_LP expression NK_RP */
  {   83,   -2 }, /* (22) expression ::= NK_PLUS expression */
  {   83,   -2 }, /* (23) expression ::= NK_MINUS expression */
  {   83,   -3 }, /* (24) expression ::= expression NK_PLUS expression */
  {   83,   -3 }, /* (25) expression ::= expression NK_MINUS expression */
  {   83,   -3 }, /* (26) expression ::= expression NK_STAR expression */
  {   83,   -3 }, /* (27) expression ::= expression NK_SLASH expression */
  {   83,   -3 }, /* (28) expression ::= expression NK_REM expression */
  {   85,   -1 }, /* (29) expression_list ::= expression */
  {   85,   -3 }, /* (30) expression_list ::= expression_list NK_COMMA expression */
  {   84,   -1 }, /* (31) column_reference ::= column_name */
  {   84,   -3 }, /* (32) column_reference ::= table_name NK_DOT column_name */
  {   87,   -3 }, /* (33) predicate ::= expression compare_op expression */
  {   87,   -5 }, /* (34) predicate ::= expression BETWEEN expression AND expression */
  {   87,   -6 }, /* (35) predicate ::= expression NOT BETWEEN expression AND expression */
  {   87,   -3 }, /* (36) predicate ::= expression IS NULL */
  {   87,   -4 }, /* (37) predicate ::= expression IS NOT NULL */
  {   87,   -3 }, /* (38) predicate ::= expression in_op in_predicate_value */
  {   88,   -1 }, /* (39) compare_op ::= NK_LT */
  {   88,   -1 }, /* (40) compare_op ::= NK_GT */
  {   88,   -1 }, /* (41) compare_op ::= NK_LE */
  {   88,   -1 }, /* (42) compare_op ::= NK_GE */
  {   88,   -1 }, /* (43) compare_op ::= NK_NE */
  {   88,   -1 }, /* (44) compare_op ::= NK_EQ */
  {   88,   -1 }, /* (45) compare_op ::= LIKE */
  {   88,   -2 }, /* (46) compare_op ::= NOT LIKE */
  {   88,   -1 }, /* (47) compare_op ::= MATCH */
  {   88,   -1 }, /* (48) compare_op ::= NMATCH */
  {   89,   -1 }, /* (49) in_op ::= IN */
  {   89,   -2 }, /* (50) in_op ::= NOT IN */
  {   90,   -3 }, /* (51) in_predicate_value ::= NK_LP expression_list NK_RP */
  {   91,   -1 }, /* (52) boolean_value_expression ::= boolean_primary */
  {   91,   -2 }, /* (53) boolean_value_expression ::= NOT boolean_primary */
  {   91,   -3 }, /* (54) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {   91,   -3 }, /* (55) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {   92,   -1 }, /* (56) boolean_primary ::= predicate */
  {   92,   -3 }, /* (57) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {   93,   -2 }, /* (58) from_clause ::= FROM table_reference_list */
  {   94,   -1 }, /* (59) table_reference_list ::= table_reference */
  {   94,   -3 }, /* (60) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {   95,   -1 }, /* (61) table_reference ::= table_primary */
  {   95,   -1 }, /* (62) table_reference ::= joined_table */
  {   96,   -2 }, /* (63) table_primary ::= table_name alias_opt */
  {   96,   -4 }, /* (64) table_primary ::= db_name NK_DOT table_name alias_opt */
  {   96,   -2 }, /* (65) table_primary ::= subquery alias_opt */
  {   96,   -1 }, /* (66) table_primary ::= parenthesized_joined_table */
  {   98,    0 }, /* (67) alias_opt ::= */
  {   98,   -1 }, /* (68) alias_opt ::= table_alias */
  {   98,   -2 }, /* (69) alias_opt ::= AS table_alias */
  {   99,   -3 }, /* (70) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {   99,   -3 }, /* (71) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {   97,   -6 }, /* (72) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  100,   -1 }, /* (73) join_type ::= INNER */
  {  102,   -9 }, /* (74) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  103,    0 }, /* (75) set_quantifier_opt ::= */
  {  103,   -1 }, /* (76) set_quantifier_opt ::= DISTINCT */
  {  103,   -1 }, /* (77) set_quantifier_opt ::= ALL */
  {  104,   -1 }, /* (78) select_list ::= NK_STAR */
  {  104,   -1 }, /* (79) select_list ::= select_sublist */
  {  110,   -1 }, /* (80) select_sublist ::= select_item */
  {  110,   -3 }, /* (81) select_sublist ::= select_sublist NK_COMMA select_item */
  {  111,   -1 }, /* (82) select_item ::= expression */
  {  111,   -2 }, /* (83) select_item ::= expression column_alias */
  {  111,   -3 }, /* (84) select_item ::= expression AS column_alias */
  {  111,   -3 }, /* (85) select_item ::= table_name NK_DOT NK_STAR */
  {  105,    0 }, /* (86) where_clause_opt ::= */
  {  105,   -2 }, /* (87) where_clause_opt ::= WHERE search_condition */
  {  106,    0 }, /* (88) partition_by_clause_opt ::= */
  {  106,   -3 }, /* (89) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  107,    0 }, /* (90) twindow_clause_opt ::= */
  {  107,   -6 }, /* (91) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  107,   -4 }, /* (92) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  107,   -6 }, /* (93) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  107,   -8 }, /* (94) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  112,    0 }, /* (95) sliding_opt ::= */
  {  112,   -4 }, /* (96) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  113,    0 }, /* (97) fill_opt ::= */
  {  113,   -4 }, /* (98) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  113,   -6 }, /* (99) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  114,   -1 }, /* (100) fill_mode ::= NONE */
  {  114,   -1 }, /* (101) fill_mode ::= PREV */
  {  114,   -1 }, /* (102) fill_mode ::= NULL */
  {  114,   -1 }, /* (103) fill_mode ::= LINEAR */
  {  114,   -1 }, /* (104) fill_mode ::= NEXT */
  {  108,    0 }, /* (105) group_by_clause_opt ::= */
  {  108,   -3 }, /* (106) group_by_clause_opt ::= GROUP BY expression_list */
  {  109,    0 }, /* (107) having_clause_opt ::= */
  {  109,   -2 }, /* (108) having_clause_opt ::= HAVING search_condition */
  {   73,   -4 }, /* (109) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  115,   -1 }, /* (110) query_expression_body ::= query_primary */
  {  115,   -4 }, /* (111) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  119,   -1 }, /* (112) query_primary ::= query_specification */
  {  116,    0 }, /* (113) order_by_clause_opt ::= */
  {  116,   -3 }, /* (114) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  117,    0 }, /* (115) slimit_clause_opt ::= */
  {  117,   -2 }, /* (116) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  117,   -4 }, /* (117) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  117,   -4 }, /* (118) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  118,    0 }, /* (119) limit_clause_opt ::= */
  {  118,   -2 }, /* (120) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  118,   -4 }, /* (121) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  118,   -4 }, /* (122) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   86,   -3 }, /* (123) subquery ::= NK_LP query_expression NK_RP */
  {  101,   -1 }, /* (124) search_condition ::= boolean_value_expression */
  {  120,   -1 }, /* (125) sort_specification_list ::= sort_specification */
  {  120,   -3 }, /* (126) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  121,   -3 }, /* (127) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  122,    0 }, /* (128) ordering_specification_opt ::= */
  {  122,   -1 }, /* (129) ordering_specification_opt ::= ASC */
  {  122,   -1 }, /* (130) ordering_specification_opt ::= DESC */
  {  123,    0 }, /* (131) null_ordering_opt ::= */
  {  123,   -2 }, /* (132) null_ordering_opt ::= NULLS FIRST */
  {  123,   -2 }, /* (133) null_ordering_opt ::= NULLS LAST */
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
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy168; }
        break;
      case 2: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 3: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 4: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 5: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 6: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 7: /* literal ::= duration_literal */
      case 17: /* expression ::= literal */ yytestcase(yyruleno==17);
      case 18: /* expression ::= column_reference */ yytestcase(yyruleno==18);
      case 20: /* expression ::= subquery */ yytestcase(yyruleno==20);
      case 52: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==52);
      case 56: /* boolean_primary ::= predicate */ yytestcase(yyruleno==56);
      case 59: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==59);
      case 61: /* table_reference ::= table_primary */ yytestcase(yyruleno==61);
      case 62: /* table_reference ::= joined_table */ yytestcase(yyruleno==62);
      case 66: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==66);
      case 110: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==110);
      case 112: /* query_primary ::= query_specification */ yytestcase(yyruleno==112);
      case 124: /* search_condition ::= boolean_value_expression */ yytestcase(yyruleno==124);
{ PARSER_TRACE; yylhsminor.yy168 = yymsp[0].minor.yy168; }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 8: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 9: /* literal_list ::= literal */
      case 29: /* expression_list ::= expression */ yytestcase(yyruleno==29);
{ PARSER_TRACE; yylhsminor.yy192 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[0].minor.yy192 = yylhsminor.yy192;
        break;
      case 10: /* literal_list ::= literal_list NK_COMMA literal */
      case 30: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==30);
{ PARSER_TRACE; yylhsminor.yy192 = addNodeToList(pCxt, yymsp[-2].minor.yy192, releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[-2].minor.yy192 = yylhsminor.yy192;
        break;
      case 11: /* db_name ::= NK_ID */
      case 12: /* table_name ::= NK_ID */ yytestcase(yyruleno==12);
      case 13: /* column_name ::= NK_ID */ yytestcase(yyruleno==13);
      case 14: /* function_name ::= NK_ID */ yytestcase(yyruleno==14);
      case 15: /* table_alias ::= NK_ID */ yytestcase(yyruleno==15);
      case 16: /* column_alias ::= NK_ID */ yytestcase(yyruleno==16);
{ PARSER_TRACE; yylhsminor.yy241 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy241 = yylhsminor.yy241;
        break;
      case 19: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy241, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy241, yymsp[-1].minor.yy192)); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 21: /* expression ::= NK_LP expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 22: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy168));
                                                                                  }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 23: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy168), NULL));
                                                                                  }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 24: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 25: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 26: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 27: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 28: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 31: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy241, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy241)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 32: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241, createColumnNode(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 33: /* predicate ::= expression compare_op expression */
{ PARSER_TRACE; yylhsminor.yy168 = createOperatorNode(pCxt, yymsp[-1].minor.yy228, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 34: /* predicate ::= expression BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy168 = createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy168), releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 35: /* predicate ::= expression NOT BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy168 = createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[-5].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 36: /* predicate ::= expression IS NULL */
{ PARSER_TRACE; yylhsminor.yy168 = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), true); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 37: /* predicate ::= expression IS NOT NULL */
{ PARSER_TRACE; yylhsminor.yy168 = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), false); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 38: /* predicate ::= expression in_op in_predicate_value */
{ PARSER_TRACE; yylhsminor.yy168 = createOperatorNode(pCxt, yymsp[-1].minor.yy228, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 39: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_LOWER_THAN; }
        break;
      case 40: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_GREATER_THAN; }
        break;
      case 41: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 42: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 43: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_NOT_EQUAL; }
        break;
      case 44: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_EQUAL; }
        break;
      case 45: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_LIKE; }
        break;
      case 46: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy228 = OP_TYPE_NOT_LIKE; }
        break;
      case 47: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_MATCH; }
        break;
      case 48: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_NMATCH; }
        break;
      case 49: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy228 = OP_TYPE_IN; }
        break;
      case 50: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy228 = OP_TYPE_NOT_IN; }
        break;
      case 51: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yymsp[-2].minor.yy168 = createNodeListNode(pCxt, yymsp[-1].minor.yy192); }
        break;
      case 53: /* boolean_value_expression ::= NOT boolean_primary */
{ PARSER_TRACE; yymsp[-1].minor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, yymsp[0].minor.yy168, NULL); }
        break;
      case 54: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 55: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 57: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */
      case 70: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */ yytestcase(yyruleno==70);
      case 71: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==71);
{ PARSER_TRACE; yymsp[-2].minor.yy168 = yymsp[-1].minor.yy168; }
        break;
      case 58: /* from_clause ::= FROM table_reference_list */
      case 87: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==87);
      case 108: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==108);
{ PARSER_TRACE; yymsp[-1].minor.yy168 = yymsp[0].minor.yy168; }
        break;
      case 60: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy168 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy168, yymsp[0].minor.yy168, NULL); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 63: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy241, &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 64: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createRealTableNode(pCxt, &yymsp[-3].minor.yy241, &yymsp[-1].minor.yy241, &yymsp[0].minor.yy241); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 65: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 67: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy241 = nil_token;  }
        break;
      case 68: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy241 = yymsp[0].minor.yy241; }
  yymsp[0].minor.yy241 = yylhsminor.yy241;
        break;
      case 69: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy241 = yymsp[0].minor.yy241; }
        break;
      case 72: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy168 = createJoinTableNode(pCxt, yymsp[-4].minor.yy229, yymsp[-5].minor.yy168, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 73: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy229 = JOIN_TYPE_INNER; }
        break;
      case 74: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy168 = createSelectStmt(pCxt, yymsp[-7].minor.yy209, yymsp[-6].minor.yy192, yymsp[-5].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addWhereClause(pCxt, yymsp[-8].minor.yy168, yymsp[-4].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addPartitionByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-3].minor.yy192);
                                                                                    yymsp[-8].minor.yy168 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy168, yymsp[-2].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addGroupByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-1].minor.yy192);
                                                                                    yymsp[-8].minor.yy168 = addHavingClause(pCxt, yymsp[-8].minor.yy168, yymsp[0].minor.yy168);
                                                                                  }
        break;
      case 75: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy209 = false; }
        break;
      case 76: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy209 = true; }
        break;
      case 77: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy209 = false; }
        break;
      case 78: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy192 = NULL; }
        break;
      case 79: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy192 = yymsp[0].minor.yy192; }
  yymsp[0].minor.yy192 = yylhsminor.yy192;
        break;
      case 80: /* select_sublist ::= select_item */
      case 125: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==125);
{ PARSER_TRACE; yylhsminor.yy192 = createNodeList(pCxt, yymsp[0].minor.yy168); }
  yymsp[0].minor.yy192 = yylhsminor.yy192;
        break;
      case 81: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 126: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==126);
{ PARSER_TRACE; yylhsminor.yy192 = addNodeToList(pCxt, yymsp[-2].minor.yy192, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy192 = yylhsminor.yy192;
        break;
      case 82: /* select_item ::= expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168), &t);
                                                                                  }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 83: /* select_item ::= expression column_alias */
{ PARSER_TRACE; yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 84: /* select_item ::= expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 85: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy168 = createColumnNode(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 86: /* where_clause_opt ::= */
      case 90: /* twindow_clause_opt ::= */ yytestcase(yyruleno==90);
      case 95: /* sliding_opt ::= */ yytestcase(yyruleno==95);
      case 97: /* fill_opt ::= */ yytestcase(yyruleno==97);
      case 107: /* having_clause_opt ::= */ yytestcase(yyruleno==107);
      case 115: /* slimit_clause_opt ::= */ yytestcase(yyruleno==115);
      case 119: /* limit_clause_opt ::= */ yytestcase(yyruleno==119);
{ PARSER_TRACE; yymsp[1].minor.yy168 = NULL; }
        break;
      case 88: /* partition_by_clause_opt ::= */
      case 105: /* group_by_clause_opt ::= */ yytestcase(yyruleno==105);
      case 113: /* order_by_clause_opt ::= */ yytestcase(yyruleno==113);
{ PARSER_TRACE; yymsp[1].minor.yy192 = NULL; }
        break;
      case 89: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 106: /* group_by_clause_opt ::= GROUP BY expression_list */ yytestcase(yyruleno==106);
      case 114: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==114);
{ PARSER_TRACE; yymsp[-2].minor.yy192 = yymsp[0].minor.yy192; }
        break;
      case 91: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy168 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), &yymsp[-1].minor.yy0); }
        break;
      case 92: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168)); }
        break;
      case 93: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy168 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy168, NULL, yymsp[-1].minor.yy168, yymsp[0].minor.yy168); }
        break;
      case 94: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy168 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy168, yymsp[-3].minor.yy168, yymsp[-1].minor.yy168, yymsp[0].minor.yy168); }
        break;
      case 96: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy168 = yymsp[-1].minor.yy168; }
        break;
      case 98: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createFillNode(pCxt, yymsp[-1].minor.yy14, NULL); }
        break;
      case 99: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy168 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy192)); }
        break;
      case 100: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy14 = FILL_MODE_NONE; }
        break;
      case 101: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy14 = FILL_MODE_PREV; }
        break;
      case 102: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy14 = FILL_MODE_NULL; }
        break;
      case 103: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy14 = FILL_MODE_LINEAR; }
        break;
      case 104: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy14 = FILL_MODE_NEXT; }
        break;
      case 109: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy168 = addOrderByClause(pCxt, yymsp[-3].minor.yy168, yymsp[-2].minor.yy192);
                                                                                    yylhsminor.yy168 = addSlimitClause(pCxt, yylhsminor.yy168, yymsp[-1].minor.yy168);
                                                                                    yylhsminor.yy168 = addLimitClause(pCxt, yylhsminor.yy168, yymsp[0].minor.yy168);
                                                                                  }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 111: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy168 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 116: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 120: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==120);
{ PARSER_TRACE; yymsp[-1].minor.yy168 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 117: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 121: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==121);
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 118: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 122: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==122);
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 123: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 127: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), yymsp[-1].minor.yy10, yymsp[0].minor.yy177); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 128: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy10 = ORDER_ASC; }
        break;
      case 129: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy10 = ORDER_ASC; }
        break;
      case 130: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy10 = ORDER_DESC; }
        break;
      case 131: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy177 = NULL_ORDER_DEFAULT; }
        break;
      case 132: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy177 = NULL_ORDER_FIRST; }
        break;
      case 133: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy177 = NULL_ORDER_LAST; }
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
