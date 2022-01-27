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

#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
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
#define YYNOCODE 98
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  SNodeList* yy40;
  EOrder yy106;
  EJoinType yy148;
  SNode* yy168;
  SToken yy169;
  bool yy173;
  ENullOrder yy193;
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
#define YYNSTATE             92
#define YYNRULE              106
#define YYNTOKEN             45
#define YY_MAX_SHIFT         91
#define YY_MIN_SHIFTREDUCE   169
#define YY_MAX_SHIFTREDUCE   274
#define YY_ERROR_ACTION      275
#define YY_ACCEPT_ACTION     276
#define YY_NO_ACTION         277
#define YY_MIN_REDUCE        278
#define YY_MAX_REDUCE        383
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
#define YY_ACTTAB_COUNT (464)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    60,   60,   77,   60,   60,   60,  290,   80,  289,   47,
 /*    10 */    60,   60,   62,   60,   60,   60,  171,  172,  245,  246,
 /*    20 */   247,   70,  249,    6,   21,   20,   76,   33,  292,  295,
 /*    30 */    46,   11,   10,    9,    8,   31,   72,  318,   34,   22,
 /*    40 */    19,   42,  300,   75,   49,   74,  187,   60,   62,   60,
 /*    50 */    60,   60,   60,  179,   60,   60,   60,   22,   87,  289,
 /*    60 */    44,   60,   60,   37,   60,   60,   60,   18,  202,   63,
 /*    70 */   291,  295,  294,  291,  295,   73,  291,  295,   60,   60,
 /*    80 */   310,   60,   60,   60,  262,   80,  289,    5,   60,   60,
 /*    90 */    62,   60,   60,   60,   83,  341,   51,   50,   60,   37,
 /*   100 */    60,   60,  171,   79,   53,   15,   60,   60,  306,   60,
 /*   110 */    60,   60,    1,   87,  289,  319,   60,   60,   37,   60,
 /*   120 */    60,   60,   19,  304,  206,   60,   62,   60,   60,   60,
 /*   130 */    60,  306,   60,   60,   60,  202,   87,  289,  212,   60,
 /*   140 */    60,   81,   60,   60,   60,   71,  303,   61,  291,  295,
 /*   150 */   342,  205,  177,   60,   37,   60,   60,   60,   60,  306,
 /*   160 */    60,   60,   60,   32,   87,  289,   23,   60,   60,   88,
 /*   170 */    60,   60,   60,  214,  302,   40,   60,   81,   60,   60,
 /*   180 */    60,   60,  178,   60,   60,   60,  327,   87,  289,  327,
 /*   190 */    60,   60,   64,   60,   60,   60,   38,  236,  237,  326,
 /*   200 */   325,    9,    8,  325,   60,   88,   60,   60,  381,  381,
 /*   210 */   307,  381,  381,  381,  179,   87,  289,   14,  381,  381,
 /*   220 */    67,  381,  381,  381,  282,   28,  334,   60,   64,   60,
 /*   230 */    60,  380,  380,   59,  380,  380,  380,   29,   87,  289,
 /*   240 */   343,  380,  380,   91,  380,  380,  380,  282,  282,  333,
 /*   250 */    68,  282,   23,  338,  282,  381,  337,  381,  381,   65,
 /*   260 */    65,   40,   65,   65,   65,   86,   87,  289,   27,   65,
 /*   270 */    65,   30,   65,   65,   65,  282,   85,   69,  380,   48,
 /*   280 */   380,  380,   66,   66,  171,   66,   66,   66,  284,   87,
 /*   290 */   289,  283,   66,   66,   84,   66,   66,   66,   21,   20,
 /*   300 */   239,  240,   25,    7,  324,  286,   65,   52,   65,   65,
 /*   310 */   377,  377,   26,  377,  377,  377,  178,   87,  289,  312,
 /*   320 */   377,  377,   56,  377,  377,  377,   55,   57,   58,   66,
 /*   330 */   213,   66,   66,  376,  376,    3,  376,  376,  376,   20,
 /*   340 */    87,  289,  200,  376,  376,  199,  376,  376,  376,   43,
 /*   350 */    17,   16,  285,   36,  263,  230,   24,  377,   13,   12,
 /*   360 */   377,    4,  219,  250,  171,  172,  245,  246,  247,   70,
 /*   370 */   249,    6,    2,  279,   54,  276,   89,  278,   90,   82,
 /*   380 */   376,  277,  277,  376,  277,  277,  277,  277,   19,  277,
 /*   390 */    13,   12,  207,  277,  277,  277,  171,  172,  245,  246,
 /*   400 */   247,   70,  249,    6,  277,  327,  277,  277,  327,  277,
 /*   410 */   277,  277,   76,   33,  277,   39,  277,  277,   39,  325,
 /*   420 */    19,   31,  325,   76,   33,  277,   78,   41,  300,  301,
 /*   430 */   277,  300,   31,  277,  277,  277,  277,  277,   35,  300,
 /*   440 */   301,  277,  300,  277,   76,   33,  277,  277,  277,  277,
 /*   450 */   277,  277,  277,   31,  277,  277,  277,  277,  277,   45,
 /*   460 */   300,  301,  277,  300,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    47,   48,   73,   50,   51,   52,   55,   54,   55,   92,
 /*    10 */    57,   58,   59,   60,   61,   62,   15,   16,   17,   18,
 /*    20 */    19,   20,   21,   22,    1,    2,   53,   54,   64,   65,
 /*    30 */    77,    9,   10,   11,   12,   62,   83,   84,    3,   22,
 /*    40 */    39,   68,   69,   70,   86,   72,   23,   94,   95,   96,
 /*    50 */    97,   47,   48,   20,   50,   51,   52,   22,   54,   55,
 /*    60 */    27,   57,   58,   59,   60,   61,   62,   29,   30,   63,
 /*    70 */    64,   65,   63,   64,   65,   63,   64,   65,   47,   48,
 /*    80 */    74,   50,   51,   52,   23,   54,   55,   26,   57,   58,
 /*    90 */    59,   60,   61,   62,   90,   91,    9,   10,   94,   95,
 /*   100 */    96,   97,   15,   20,   86,   22,   47,   48,   56,   50,
 /*   110 */    51,   52,   76,   54,   55,   84,   57,   58,   59,   60,
 /*   120 */    61,   62,   39,   71,    5,   94,   95,   96,   97,   47,
 /*   130 */    48,   56,   50,   51,   52,   30,   54,   55,   20,   57,
 /*   140 */    58,   59,   60,   61,   62,   27,   71,   63,   64,   65,
 /*   150 */    91,   32,   20,   94,   95,   96,   97,   47,   48,   56,
 /*   160 */    50,   51,   52,   54,   54,   55,   22,   57,   58,   59,
 /*   170 */    60,   61,   62,   11,   71,   31,   94,   95,   96,   97,
 /*   180 */    47,   48,   20,   50,   51,   52,   75,   54,   55,   75,
 /*   190 */    57,   58,   59,   60,   61,   62,   85,   40,   41,   85,
 /*   200 */    89,   11,   12,   89,   94,   95,   96,   97,   47,   48,
 /*   210 */    56,   50,   51,   52,   20,   54,   55,   25,   57,   58,
 /*   220 */    42,   60,   61,   62,   47,   26,   49,   94,   95,   96,
 /*   230 */    97,   47,   48,   66,   50,   51,   52,   38,   54,   55,
 /*   240 */    93,   57,   58,   13,   60,   61,   62,   47,   47,   49,
 /*   250 */    49,   47,   22,   49,   47,   94,   49,   96,   97,   47,
 /*   260 */    48,   31,   50,   51,   52,    4,   54,   55,   35,   57,
 /*   270 */    58,   37,   60,   61,   62,   47,   87,   49,   94,   88,
 /*   280 */    96,   97,   47,   48,   15,   50,   51,   52,   47,   54,
 /*   290 */    55,   47,   57,   58,   33,   60,   61,   62,    1,    2,
 /*   300 */    43,   44,   26,   22,   88,   24,   94,   87,   96,   97,
 /*   310 */    47,   48,   36,   50,   51,   52,   20,   54,   55,   82,
 /*   320 */    57,   58,   80,   60,   61,   62,   81,   79,   78,   94,
 /*   330 */    20,   96,   97,   47,   48,   26,   50,   51,   52,    2,
 /*   340 */    54,   55,   23,   57,   58,   23,   60,   61,   62,   24,
 /*   350 */    26,   28,   24,   24,   23,   23,    5,   94,    9,   10,
 /*   360 */    97,   26,   23,   23,   15,   16,   17,   18,   19,   20,
 /*   370 */    21,   22,   34,    0,   24,   45,   46,    0,   14,   46,
 /*   380 */    94,   98,   98,   97,   98,   98,   98,   98,   39,   98,
 /*   390 */     9,   10,   11,   98,   98,   98,   15,   16,   17,   18,
 /*   400 */    19,   20,   21,   22,   98,   75,   98,   98,   75,   98,
 /*   410 */    98,   98,   53,   54,   98,   85,   98,   98,   85,   89,
 /*   420 */    39,   62,   89,   53,   54,   98,   67,   68,   69,   70,
 /*   430 */    98,   72,   62,   98,   98,   98,   98,   98,   68,   69,
 /*   440 */    70,   98,   72,   98,   53,   54,   98,   98,   98,   98,
 /*   450 */    98,   98,   98,   62,   98,   98,   98,   98,   98,   68,
 /*   460 */    69,   70,   98,   72,
};
#define YY_SHIFT_COUNT    (91)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (381)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   230,  381,  349,  349,  349,  349,  349,  349,  349,  349,
 /*    10 */   349,  349,    1,    1,   83,   83,   83,   83,   35,  144,
 /*    20 */    35,   35,   35,  144,  144,   87,   87,   87,   87,   87,
 /*    30 */    87,   33,   33,   33,   17,   38,  162,  157,  261,  261,
 /*    40 */   119,  105,  105,  132,  194,  105,  192,  178,  233,  234,
 /*    50 */   269,  269,  234,  233,  296,  464,  464,  464,  464,  464,
 /*    60 */    22,   23,  118,  297,   61,  190,  190,  257,  276,  199,
 /*    70 */   281,  310,  309,  337,  319,  322,  325,  323,  324,  328,
 /*    80 */   329,  331,  332,  335,  338,  339,  351,  350,  340,  373,
 /*    90 */   377,  364,
};
#define YY_REDUCE_COUNT (59)
#define YY_REDUCE_MIN   (-83)
#define YY_REDUCE_MAX   (391)
static const short yy_reduce_ofst[] = {
 /*     0 */   330,  -47,    4,   31,   59,   82,  110,  133,  161,  184,
 /*    10 */   212,  235,  263,  286,  359,  -27,  370,  391,    6,  333,
 /*    20 */     9,   12,   84,  111,  114,  177,  200,  201,  204,  207,
 /*    30 */   228,   52,   75,  103,  -36,  -71,  -49,  -83,  -42,   18,
 /*    40 */    36,  -71,  -71,  109,  154,  -71,  167,  147,  189,  191,
 /*    50 */   241,  244,  216,  220,  -49,  237,  245,  242,  248,  250,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*    10 */   275,  275,  275,  275,  275,  275,  275,  275,  365,  275,
 /*    20 */   365,  365,  365,  275,  275,  275,  275,  275,  275,  275,
 /*    30 */   275,  305,  305,  305,  365,  275,  275,  344,  329,  329,
 /*    40 */   313,  298,  275,  275,  275,  299,  275,  347,  331,  335,
 /*    50 */   275,  275,  335,  331,  275,  370,  369,  368,  367,  366,
 /*    60 */   374,  275,  320,  340,  275,  379,  378,  275,  332,  336,
 /*    70 */   287,  275,  317,  293,  364,  301,  275,  275,  297,  286,
 /*    80 */   275,  275,  275,  330,  275,  275,  275,  275,  275,  275,
 /*    90 */   275,  275,
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
  /*    3 */ "NOT",
  /*    4 */ "UNION",
  /*    5 */ "ALL",
  /*    6 */ "MINUS",
  /*    7 */ "EXCEPT",
  /*    8 */ "INTERSECT",
  /*    9 */ "NK_PLUS",
  /*   10 */ "NK_MINUS",
  /*   11 */ "NK_STAR",
  /*   12 */ "NK_SLASH",
  /*   13 */ "SHOW",
  /*   14 */ "DATABASES",
  /*   15 */ "NK_INTEGER",
  /*   16 */ "NK_FLOAT",
  /*   17 */ "NK_STRING",
  /*   18 */ "NK_BOOL",
  /*   19 */ "NK_NOW",
  /*   20 */ "NK_ID",
  /*   21 */ "NK_QUESTION",
  /*   22 */ "NK_LP",
  /*   23 */ "NK_RP",
  /*   24 */ "NK_DOT",
  /*   25 */ "FROM",
  /*   26 */ "NK_COMMA",
  /*   27 */ "AS",
  /*   28 */ "JOIN",
  /*   29 */ "ON",
  /*   30 */ "INNER",
  /*   31 */ "SELECT",
  /*   32 */ "DISTINCT",
  /*   33 */ "ORDER",
  /*   34 */ "BY",
  /*   35 */ "SLIMIT",
  /*   36 */ "SOFFSET",
  /*   37 */ "LIMIT",
  /*   38 */ "OFFSET",
  /*   39 */ "NK_LR",
  /*   40 */ "ASC",
  /*   41 */ "DESC",
  /*   42 */ "NULLS",
  /*   43 */ "FIRST",
  /*   44 */ "LAST",
  /*   45 */ "cmd",
  /*   46 */ "query_expression",
  /*   47 */ "unsigned_integer",
  /*   48 */ "unsigned_approximate_numeric",
  /*   49 */ "signed_integer",
  /*   50 */ "unsigned_literal",
  /*   51 */ "unsigned_numeric_literal",
  /*   52 */ "general_literal",
  /*   53 */ "db_name",
  /*   54 */ "table_name",
  /*   55 */ "column_name",
  /*   56 */ "table_alias",
  /*   57 */ "unsigned_value_specification",
  /*   58 */ "value_expression_primary",
  /*   59 */ "value_expression",
  /*   60 */ "nonparenthesized_value_expression_primary",
  /*   61 */ "column_reference",
  /*   62 */ "subquery",
  /*   63 */ "boolean_value_expression",
  /*   64 */ "boolean_primary",
  /*   65 */ "predicate",
  /*   66 */ "from_clause",
  /*   67 */ "table_reference_list",
  /*   68 */ "table_reference",
  /*   69 */ "table_primary",
  /*   70 */ "joined_table",
  /*   71 */ "correlation_or_recognition_opt",
  /*   72 */ "parenthesized_joined_table",
  /*   73 */ "join_type",
  /*   74 */ "search_condition",
  /*   75 */ "query_specification",
  /*   76 */ "set_quantifier_opt",
  /*   77 */ "select_list",
  /*   78 */ "where_clause_opt",
  /*   79 */ "partition_by_clause_opt",
  /*   80 */ "twindow_clause_opt",
  /*   81 */ "group_by_clause_opt",
  /*   82 */ "having_clause_opt",
  /*   83 */ "select_sublist",
  /*   84 */ "select_item",
  /*   85 */ "query_expression_body",
  /*   86 */ "order_by_clause_opt",
  /*   87 */ "slimit_clause_opt",
  /*   88 */ "limit_clause_opt",
  /*   89 */ "query_primary",
  /*   90 */ "sort_specification_list",
  /*   91 */ "sort_specification",
  /*   92 */ "ordering_specification_opt",
  /*   93 */ "null_ordering_opt",
  /*   94 */ "value_function",
  /*   95 */ "common_value_expression",
  /*   96 */ "numeric_value_expression",
  /*   97 */ "numeric_primary",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= SHOW DATABASES",
 /*   1 */ "cmd ::= query_expression",
 /*   2 */ "unsigned_integer ::= NK_INTEGER",
 /*   3 */ "unsigned_approximate_numeric ::= NK_FLOAT",
 /*   4 */ "signed_integer ::= unsigned_integer",
 /*   5 */ "signed_integer ::= NK_PLUS unsigned_integer",
 /*   6 */ "signed_integer ::= NK_MINUS unsigned_integer",
 /*   7 */ "db_name ::= NK_ID",
 /*   8 */ "table_name ::= NK_ID",
 /*   9 */ "column_name ::= NK_ID",
 /*  10 */ "table_alias ::= NK_ID",
 /*  11 */ "column_reference ::= column_name",
 /*  12 */ "column_reference ::= table_name NK_DOT column_name",
 /*  13 */ "boolean_value_expression ::= boolean_primary",
 /*  14 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  15 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  16 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  17 */ "boolean_primary ::= predicate",
 /*  18 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  19 */ "from_clause ::= FROM table_reference_list",
 /*  20 */ "table_reference_list ::= table_reference",
 /*  21 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  22 */ "table_reference ::= table_primary",
 /*  23 */ "table_reference ::= joined_table",
 /*  24 */ "table_primary ::= table_name correlation_or_recognition_opt",
 /*  25 */ "table_primary ::= db_name NK_DOT table_name correlation_or_recognition_opt",
 /*  26 */ "table_primary ::= subquery correlation_or_recognition_opt",
 /*  27 */ "correlation_or_recognition_opt ::=",
 /*  28 */ "correlation_or_recognition_opt ::= table_alias",
 /*  29 */ "correlation_or_recognition_opt ::= AS table_alias",
 /*  30 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  31 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  32 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  33 */ "join_type ::= INNER",
 /*  34 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  35 */ "set_quantifier_opt ::=",
 /*  36 */ "set_quantifier_opt ::= DISTINCT",
 /*  37 */ "set_quantifier_opt ::= ALL",
 /*  38 */ "select_list ::= NK_STAR",
 /*  39 */ "select_list ::= select_sublist",
 /*  40 */ "select_sublist ::= select_item",
 /*  41 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  42 */ "select_item ::= value_expression",
 /*  43 */ "select_item ::= value_expression NK_ID",
 /*  44 */ "select_item ::= value_expression AS NK_ID",
 /*  45 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  46 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /*  47 */ "query_expression_body ::= query_primary",
 /*  48 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /*  49 */ "query_primary ::= query_specification",
 /*  50 */ "query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP",
 /*  51 */ "order_by_clause_opt ::=",
 /*  52 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /*  53 */ "slimit_clause_opt ::=",
 /*  54 */ "slimit_clause_opt ::= SLIMIT signed_integer",
 /*  55 */ "slimit_clause_opt ::= SLIMIT signed_integer SOFFSET signed_integer",
 /*  56 */ "slimit_clause_opt ::= SLIMIT signed_integer NK_COMMA signed_integer",
 /*  57 */ "limit_clause_opt ::=",
 /*  58 */ "limit_clause_opt ::= LIMIT signed_integer",
 /*  59 */ "limit_clause_opt ::= LIMIT signed_integer OFFSET signed_integer",
 /*  60 */ "limit_clause_opt ::= LIMIT signed_integer NK_COMMA signed_integer",
 /*  61 */ "subquery ::= NK_LR query_expression NK_RP",
 /*  62 */ "search_condition ::= boolean_value_expression",
 /*  63 */ "sort_specification_list ::= sort_specification",
 /*  64 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /*  65 */ "sort_specification ::= value_expression ordering_specification_opt null_ordering_opt",
 /*  66 */ "ordering_specification_opt ::=",
 /*  67 */ "ordering_specification_opt ::= ASC",
 /*  68 */ "ordering_specification_opt ::= DESC",
 /*  69 */ "null_ordering_opt ::=",
 /*  70 */ "null_ordering_opt ::= NULLS FIRST",
 /*  71 */ "null_ordering_opt ::= NULLS LAST",
 /*  72 */ "unsigned_literal ::= unsigned_numeric_literal",
 /*  73 */ "unsigned_literal ::= general_literal",
 /*  74 */ "unsigned_numeric_literal ::= unsigned_integer",
 /*  75 */ "unsigned_numeric_literal ::= unsigned_approximate_numeric",
 /*  76 */ "general_literal ::= NK_STRING",
 /*  77 */ "general_literal ::= NK_BOOL",
 /*  78 */ "general_literal ::= NK_NOW",
 /*  79 */ "unsigned_value_specification ::= unsigned_literal",
 /*  80 */ "unsigned_value_specification ::= NK_QUESTION",
 /*  81 */ "value_expression_primary ::= NK_LP value_expression NK_RP",
 /*  82 */ "value_expression_primary ::= nonparenthesized_value_expression_primary",
 /*  83 */ "nonparenthesized_value_expression_primary ::= unsigned_value_specification",
 /*  84 */ "nonparenthesized_value_expression_primary ::= column_reference",
 /*  85 */ "nonparenthesized_value_expression_primary ::= subquery",
 /*  86 */ "table_primary ::= parenthesized_joined_table",
 /*  87 */ "predicate ::=",
 /*  88 */ "where_clause_opt ::=",
 /*  89 */ "partition_by_clause_opt ::=",
 /*  90 */ "twindow_clause_opt ::=",
 /*  91 */ "group_by_clause_opt ::=",
 /*  92 */ "having_clause_opt ::=",
 /*  93 */ "value_function ::= NK_ID NK_LP value_expression NK_RP",
 /*  94 */ "value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP",
 /*  95 */ "value_expression ::= common_value_expression",
 /*  96 */ "common_value_expression ::= numeric_value_expression",
 /*  97 */ "numeric_value_expression ::= numeric_primary",
 /*  98 */ "numeric_value_expression ::= NK_PLUS numeric_primary",
 /*  99 */ "numeric_value_expression ::= NK_MINUS numeric_primary",
 /* 100 */ "numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression",
 /* 101 */ "numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression",
 /* 102 */ "numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression",
 /* 103 */ "numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression",
 /* 104 */ "numeric_primary ::= value_expression_primary",
 /* 105 */ "numeric_primary ::= value_function",
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
    case 45: /* cmd */
    case 46: /* query_expression */
    case 47: /* unsigned_integer */
    case 48: /* unsigned_approximate_numeric */
    case 49: /* signed_integer */
    case 50: /* unsigned_literal */
    case 51: /* unsigned_numeric_literal */
    case 52: /* general_literal */
    case 57: /* unsigned_value_specification */
    case 58: /* value_expression_primary */
    case 59: /* value_expression */
    case 60: /* nonparenthesized_value_expression_primary */
    case 61: /* column_reference */
    case 62: /* subquery */
    case 63: /* boolean_value_expression */
    case 64: /* boolean_primary */
    case 65: /* predicate */
    case 66: /* from_clause */
    case 67: /* table_reference_list */
    case 68: /* table_reference */
    case 69: /* table_primary */
    case 70: /* joined_table */
    case 72: /* parenthesized_joined_table */
    case 74: /* search_condition */
    case 75: /* query_specification */
    case 78: /* where_clause_opt */
    case 80: /* twindow_clause_opt */
    case 82: /* having_clause_opt */
    case 84: /* select_item */
    case 85: /* query_expression_body */
    case 87: /* slimit_clause_opt */
    case 88: /* limit_clause_opt */
    case 89: /* query_primary */
    case 91: /* sort_specification */
    case 94: /* value_function */
    case 95: /* common_value_expression */
    case 96: /* numeric_value_expression */
    case 97: /* numeric_primary */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy168)); 
}
      break;
    case 53: /* db_name */
    case 54: /* table_name */
    case 55: /* column_name */
    case 56: /* table_alias */
    case 71: /* correlation_or_recognition_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 73: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 76: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 77: /* select_list */
    case 79: /* partition_by_clause_opt */
    case 81: /* group_by_clause_opt */
    case 83: /* select_sublist */
    case 86: /* order_by_clause_opt */
    case 90: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy40)); 
}
      break;
    case 92: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 93: /* null_ordering_opt */
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
  {   45,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   45,   -1 }, /* (1) cmd ::= query_expression */
  {   47,   -1 }, /* (2) unsigned_integer ::= NK_INTEGER */
  {   48,   -1 }, /* (3) unsigned_approximate_numeric ::= NK_FLOAT */
  {   49,   -1 }, /* (4) signed_integer ::= unsigned_integer */
  {   49,   -2 }, /* (5) signed_integer ::= NK_PLUS unsigned_integer */
  {   49,   -2 }, /* (6) signed_integer ::= NK_MINUS unsigned_integer */
  {   53,   -1 }, /* (7) db_name ::= NK_ID */
  {   54,   -1 }, /* (8) table_name ::= NK_ID */
  {   55,   -1 }, /* (9) column_name ::= NK_ID */
  {   56,   -1 }, /* (10) table_alias ::= NK_ID */
  {   61,   -1 }, /* (11) column_reference ::= column_name */
  {   61,   -3 }, /* (12) column_reference ::= table_name NK_DOT column_name */
  {   63,   -1 }, /* (13) boolean_value_expression ::= boolean_primary */
  {   63,   -2 }, /* (14) boolean_value_expression ::= NOT boolean_primary */
  {   63,   -3 }, /* (15) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {   63,   -3 }, /* (16) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {   64,   -1 }, /* (17) boolean_primary ::= predicate */
  {   64,   -3 }, /* (18) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {   66,   -2 }, /* (19) from_clause ::= FROM table_reference_list */
  {   67,   -1 }, /* (20) table_reference_list ::= table_reference */
  {   67,   -3 }, /* (21) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {   68,   -1 }, /* (22) table_reference ::= table_primary */
  {   68,   -1 }, /* (23) table_reference ::= joined_table */
  {   69,   -2 }, /* (24) table_primary ::= table_name correlation_or_recognition_opt */
  {   69,   -4 }, /* (25) table_primary ::= db_name NK_DOT table_name correlation_or_recognition_opt */
  {   69,   -2 }, /* (26) table_primary ::= subquery correlation_or_recognition_opt */
  {   71,    0 }, /* (27) correlation_or_recognition_opt ::= */
  {   71,   -1 }, /* (28) correlation_or_recognition_opt ::= table_alias */
  {   71,   -2 }, /* (29) correlation_or_recognition_opt ::= AS table_alias */
  {   72,   -3 }, /* (30) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {   72,   -3 }, /* (31) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {   70,   -6 }, /* (32) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {   73,   -1 }, /* (33) join_type ::= INNER */
  {   75,   -9 }, /* (34) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {   76,    0 }, /* (35) set_quantifier_opt ::= */
  {   76,   -1 }, /* (36) set_quantifier_opt ::= DISTINCT */
  {   76,   -1 }, /* (37) set_quantifier_opt ::= ALL */
  {   77,   -1 }, /* (38) select_list ::= NK_STAR */
  {   77,   -1 }, /* (39) select_list ::= select_sublist */
  {   83,   -1 }, /* (40) select_sublist ::= select_item */
  {   83,   -3 }, /* (41) select_sublist ::= select_sublist NK_COMMA select_item */
  {   84,   -1 }, /* (42) select_item ::= value_expression */
  {   84,   -2 }, /* (43) select_item ::= value_expression NK_ID */
  {   84,   -3 }, /* (44) select_item ::= value_expression AS NK_ID */
  {   84,   -3 }, /* (45) select_item ::= table_name NK_DOT NK_STAR */
  {   46,   -4 }, /* (46) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {   85,   -1 }, /* (47) query_expression_body ::= query_primary */
  {   85,   -4 }, /* (48) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {   89,   -1 }, /* (49) query_primary ::= query_specification */
  {   89,   -6 }, /* (50) query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
  {   86,    0 }, /* (51) order_by_clause_opt ::= */
  {   86,   -3 }, /* (52) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {   87,    0 }, /* (53) slimit_clause_opt ::= */
  {   87,   -2 }, /* (54) slimit_clause_opt ::= SLIMIT signed_integer */
  {   87,   -4 }, /* (55) slimit_clause_opt ::= SLIMIT signed_integer SOFFSET signed_integer */
  {   87,   -4 }, /* (56) slimit_clause_opt ::= SLIMIT signed_integer NK_COMMA signed_integer */
  {   88,    0 }, /* (57) limit_clause_opt ::= */
  {   88,   -2 }, /* (58) limit_clause_opt ::= LIMIT signed_integer */
  {   88,   -4 }, /* (59) limit_clause_opt ::= LIMIT signed_integer OFFSET signed_integer */
  {   88,   -4 }, /* (60) limit_clause_opt ::= LIMIT signed_integer NK_COMMA signed_integer */
  {   62,   -3 }, /* (61) subquery ::= NK_LR query_expression NK_RP */
  {   74,   -1 }, /* (62) search_condition ::= boolean_value_expression */
  {   90,   -1 }, /* (63) sort_specification_list ::= sort_specification */
  {   90,   -3 }, /* (64) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {   91,   -3 }, /* (65) sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
  {   92,    0 }, /* (66) ordering_specification_opt ::= */
  {   92,   -1 }, /* (67) ordering_specification_opt ::= ASC */
  {   92,   -1 }, /* (68) ordering_specification_opt ::= DESC */
  {   93,    0 }, /* (69) null_ordering_opt ::= */
  {   93,   -2 }, /* (70) null_ordering_opt ::= NULLS FIRST */
  {   93,   -2 }, /* (71) null_ordering_opt ::= NULLS LAST */
  {   50,   -1 }, /* (72) unsigned_literal ::= unsigned_numeric_literal */
  {   50,   -1 }, /* (73) unsigned_literal ::= general_literal */
  {   51,   -1 }, /* (74) unsigned_numeric_literal ::= unsigned_integer */
  {   51,   -1 }, /* (75) unsigned_numeric_literal ::= unsigned_approximate_numeric */
  {   52,   -1 }, /* (76) general_literal ::= NK_STRING */
  {   52,   -1 }, /* (77) general_literal ::= NK_BOOL */
  {   52,   -1 }, /* (78) general_literal ::= NK_NOW */
  {   57,   -1 }, /* (79) unsigned_value_specification ::= unsigned_literal */
  {   57,   -1 }, /* (80) unsigned_value_specification ::= NK_QUESTION */
  {   58,   -3 }, /* (81) value_expression_primary ::= NK_LP value_expression NK_RP */
  {   58,   -1 }, /* (82) value_expression_primary ::= nonparenthesized_value_expression_primary */
  {   60,   -1 }, /* (83) nonparenthesized_value_expression_primary ::= unsigned_value_specification */
  {   60,   -1 }, /* (84) nonparenthesized_value_expression_primary ::= column_reference */
  {   60,   -1 }, /* (85) nonparenthesized_value_expression_primary ::= subquery */
  {   69,   -1 }, /* (86) table_primary ::= parenthesized_joined_table */
  {   65,    0 }, /* (87) predicate ::= */
  {   78,    0 }, /* (88) where_clause_opt ::= */
  {   79,    0 }, /* (89) partition_by_clause_opt ::= */
  {   80,    0 }, /* (90) twindow_clause_opt ::= */
  {   81,    0 }, /* (91) group_by_clause_opt ::= */
  {   82,    0 }, /* (92) having_clause_opt ::= */
  {   94,   -4 }, /* (93) value_function ::= NK_ID NK_LP value_expression NK_RP */
  {   94,   -6 }, /* (94) value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
  {   59,   -1 }, /* (95) value_expression ::= common_value_expression */
  {   95,   -1 }, /* (96) common_value_expression ::= numeric_value_expression */
  {   96,   -1 }, /* (97) numeric_value_expression ::= numeric_primary */
  {   96,   -2 }, /* (98) numeric_value_expression ::= NK_PLUS numeric_primary */
  {   96,   -2 }, /* (99) numeric_value_expression ::= NK_MINUS numeric_primary */
  {   96,   -3 }, /* (100) numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
  {   96,   -3 }, /* (101) numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */
  {   96,   -3 }, /* (102) numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */
  {   96,   -3 }, /* (103) numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */
  {   97,   -1 }, /* (104) numeric_primary ::= value_expression_primary */
  {   97,   -1 }, /* (105) numeric_primary ::= value_function */
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
      case 2: /* unsigned_integer ::= NK_INTEGER */
{ yylhsminor.yy168 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 3: /* unsigned_approximate_numeric ::= NK_FLOAT */
{ yylhsminor.yy168 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 4: /* signed_integer ::= unsigned_integer */
{ yylhsminor.yy168 = yymsp[0].minor.yy168; }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 5: /* signed_integer ::= NK_PLUS unsigned_integer */
{ yymsp[-1].minor.yy168 = yymsp[0].minor.yy168; }
        break;
      case 6: /* signed_integer ::= NK_MINUS unsigned_integer */
{ yymsp[-1].minor.yy168 = addMinusSign(pCxt, yymsp[0].minor.yy168);}
        break;
      case 7: /* db_name ::= NK_ID */
      case 8: /* table_name ::= NK_ID */ yytestcase(yyruleno==8);
      case 9: /* column_name ::= NK_ID */ yytestcase(yyruleno==9);
      case 10: /* table_alias ::= NK_ID */ yytestcase(yyruleno==10);
{ PARSER_TRACE; yylhsminor.yy169 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy169 = yylhsminor.yy169;
        break;
      case 11: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy168 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy169); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 12: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy168 = createColumnNode(pCxt, &yymsp[-2].minor.yy169, &yymsp[0].minor.yy169); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 13: /* boolean_value_expression ::= boolean_primary */
      case 17: /* boolean_primary ::= predicate */ yytestcase(yyruleno==17);
      case 20: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==20);
      case 22: /* table_reference ::= table_primary */ yytestcase(yyruleno==22);
      case 23: /* table_reference ::= joined_table */ yytestcase(yyruleno==23);
      case 42: /* select_item ::= value_expression */ yytestcase(yyruleno==42);
      case 47: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==47);
      case 49: /* query_primary ::= query_specification */ yytestcase(yyruleno==49);
      case 62: /* search_condition ::= boolean_value_expression */ yytestcase(yyruleno==62);
{ PARSER_TRACE; yylhsminor.yy168 = yymsp[0].minor.yy168; }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 14: /* boolean_value_expression ::= NOT boolean_primary */
{ PARSER_TRACE; yymsp[-1].minor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, yymsp[0].minor.yy168, NULL); }
        break;
      case 15: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 16: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy168 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 18: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */
      case 30: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */ yytestcase(yyruleno==30);
      case 31: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==31);
      case 61: /* subquery ::= NK_LR query_expression NK_RP */ yytestcase(yyruleno==61);
{ PARSER_TRACE; yymsp[-2].minor.yy168 = yymsp[-1].minor.yy168; }
        break;
      case 19: /* from_clause ::= FROM table_reference_list */
{ PARSER_TRACE; yymsp[-1].minor.yy168 = yymsp[0].minor.yy168; }
        break;
      case 21: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy168 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy168, yymsp[0].minor.yy168, NULL); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 24: /* table_primary ::= table_name correlation_or_recognition_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy169, &yymsp[0].minor.yy169); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 25: /* table_primary ::= db_name NK_DOT table_name correlation_or_recognition_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createRealTableNode(pCxt, &yymsp[-3].minor.yy169, &yymsp[-1].minor.yy169, &yymsp[0].minor.yy169); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 26: /* table_primary ::= subquery correlation_or_recognition_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createTempTableNode(pCxt, yymsp[-1].minor.yy168, &yymsp[0].minor.yy169); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 27: /* correlation_or_recognition_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy169 = nil_token;  }
        break;
      case 28: /* correlation_or_recognition_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy169 = yymsp[0].minor.yy169; }
  yymsp[0].minor.yy169 = yylhsminor.yy169;
        break;
      case 29: /* correlation_or_recognition_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy169 = yymsp[0].minor.yy169; }
        break;
      case 32: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy168 = createJoinTableNode(pCxt, yymsp[-4].minor.yy148, yymsp[-5].minor.yy168, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 33: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy148 = JOIN_TYPE_INNER; }
        break;
      case 34: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy168 = createSelectStmt(pCxt, yymsp[-7].minor.yy173, yymsp[-6].minor.yy40, yymsp[-5].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addWhereClause(pCxt, yymsp[-8].minor.yy168, yymsp[-4].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addPartitionByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-3].minor.yy40);
                                                                                    yymsp[-8].minor.yy168 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy168, yymsp[-2].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addGroupByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-1].minor.yy40);
                                                                                    yymsp[-8].minor.yy168 = addHavingClause(pCxt, yymsp[-8].minor.yy168, yymsp[0].minor.yy168);
                                                                                  }
        break;
      case 35: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy173 = false; }
        break;
      case 36: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy173 = true; }
        break;
      case 37: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy173 = false; }
        break;
      case 38: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy40 = NULL; }
        break;
      case 39: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy40 = yymsp[0].minor.yy40; }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 40: /* select_sublist ::= select_item */
      case 63: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==63);
{ PARSER_TRACE; yylhsminor.yy40 = createNodeList(pCxt, yymsp[0].minor.yy168); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 41: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 64: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==64);
{ PARSER_TRACE; yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 43: /* select_item ::= value_expression NK_ID */
{ PARSER_TRACE; yylhsminor.yy168 = setProjectionAlias(pCxt, yymsp[-1].minor.yy168, &yymsp[0].minor.yy0); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 44: /* select_item ::= value_expression AS NK_ID */
{ PARSER_TRACE; yylhsminor.yy168 = setProjectionAlias(pCxt, yymsp[-2].minor.yy168, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 45: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy168 = createColumnNode(pCxt, &yymsp[-2].minor.yy169, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 46: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy168 = addOrderByClause(pCxt, yymsp[-3].minor.yy168, yymsp[-2].minor.yy40);
                                                                                    yylhsminor.yy168 = addSlimitClause(pCxt, yylhsminor.yy168, yymsp[-1].minor.yy168);
                                                                                    yylhsminor.yy168 = addLimitClause(pCxt, yylhsminor.yy168, yymsp[0].minor.yy168);
                                                                                  }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 48: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy168 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 50: /* query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy168 = yymsp[-4].minor.yy168;}
  yy_destructor(yypParser,86,&yymsp[-3].minor);
  yy_destructor(yypParser,88,&yymsp[-2].minor);
  yy_destructor(yypParser,87,&yymsp[-1].minor);
        break;
      case 51: /* order_by_clause_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy40 = NULL; }
        break;
      case 52: /* order_by_clause_opt ::= ORDER BY sort_specification_list */
{ PARSER_TRACE; yymsp[-2].minor.yy40 = yymsp[0].minor.yy40; }
        break;
      case 53: /* slimit_clause_opt ::= */
      case 57: /* limit_clause_opt ::= */ yytestcase(yyruleno==57);
{ PARSER_TRACE; yymsp[1].minor.yy168 = NULL; }
        break;
      case 54: /* slimit_clause_opt ::= SLIMIT signed_integer */
      case 58: /* limit_clause_opt ::= LIMIT signed_integer */ yytestcase(yyruleno==58);
{ PARSER_TRACE; yymsp[-1].minor.yy168 = createLimitNode(pCxt, yymsp[0].minor.yy168, 0); }
        break;
      case 55: /* slimit_clause_opt ::= SLIMIT signed_integer SOFFSET signed_integer */
      case 59: /* limit_clause_opt ::= LIMIT signed_integer OFFSET signed_integer */ yytestcase(yyruleno==59);
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createLimitNode(pCxt, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
        break;
      case 56: /* slimit_clause_opt ::= SLIMIT signed_integer NK_COMMA signed_integer */
      case 60: /* limit_clause_opt ::= LIMIT signed_integer NK_COMMA signed_integer */ yytestcase(yyruleno==60);
{ PARSER_TRACE; yymsp[-3].minor.yy168 = createLimitNode(pCxt, yymsp[0].minor.yy168, yymsp[-2].minor.yy168); }
        break;
      case 65: /* sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy168 = createOrderByExprNode(pCxt, yymsp[-2].minor.yy168, yymsp[-1].minor.yy106, yymsp[0].minor.yy193); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 66: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy106 = ORDER_ASC; }
        break;
      case 67: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy106 = ORDER_ASC; }
        break;
      case 68: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy106 = ORDER_DESC; }
        break;
      case 69: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy193 = NULL_ORDER_DEFAULT; }
        break;
      case 70: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy193 = NULL_ORDER_FIRST; }
        break;
      case 71: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy193 = NULL_ORDER_LAST; }
        break;
      case 72: /* unsigned_literal ::= unsigned_numeric_literal */
{  yy_destructor(yypParser,51,&yymsp[0].minor);
{
}
}
        break;
      case 73: /* unsigned_literal ::= general_literal */
{  yy_destructor(yypParser,52,&yymsp[0].minor);
{
}
}
        break;
      case 74: /* unsigned_numeric_literal ::= unsigned_integer */
{  yy_destructor(yypParser,47,&yymsp[0].minor);
{
}
}
        break;
      case 75: /* unsigned_numeric_literal ::= unsigned_approximate_numeric */
{  yy_destructor(yypParser,48,&yymsp[0].minor);
{
}
}
        break;
      case 79: /* unsigned_value_specification ::= unsigned_literal */
{  yy_destructor(yypParser,50,&yymsp[0].minor);
{
}
}
        break;
      case 81: /* value_expression_primary ::= NK_LP value_expression NK_RP */
      case 93: /* value_function ::= NK_ID NK_LP value_expression NK_RP */ yytestcase(yyruleno==93);
{
}
  yy_destructor(yypParser,59,&yymsp[-1].minor);
        break;
      case 82: /* value_expression_primary ::= nonparenthesized_value_expression_primary */
{  yy_destructor(yypParser,60,&yymsp[0].minor);
{
}
}
        break;
      case 83: /* nonparenthesized_value_expression_primary ::= unsigned_value_specification */
{  yy_destructor(yypParser,57,&yymsp[0].minor);
{
}
}
        break;
      case 84: /* nonparenthesized_value_expression_primary ::= column_reference */
{  yy_destructor(yypParser,61,&yymsp[0].minor);
{
}
}
        break;
      case 85: /* nonparenthesized_value_expression_primary ::= subquery */
{  yy_destructor(yypParser,62,&yymsp[0].minor);
{
}
}
        break;
      case 86: /* table_primary ::= parenthesized_joined_table */
{  yy_destructor(yypParser,72,&yymsp[0].minor);
{
}
}
        break;
      case 94: /* value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
{
}
  yy_destructor(yypParser,59,&yymsp[-3].minor);
  yy_destructor(yypParser,59,&yymsp[-1].minor);
        break;
      case 95: /* value_expression ::= common_value_expression */
{  yy_destructor(yypParser,95,&yymsp[0].minor);
{
}
}
        break;
      case 96: /* common_value_expression ::= numeric_value_expression */
{  yy_destructor(yypParser,96,&yymsp[0].minor);
{
}
}
        break;
      case 97: /* numeric_value_expression ::= numeric_primary */
{  yy_destructor(yypParser,97,&yymsp[0].minor);
{
}
}
        break;
      case 98: /* numeric_value_expression ::= NK_PLUS numeric_primary */
      case 99: /* numeric_value_expression ::= NK_MINUS numeric_primary */ yytestcase(yyruleno==99);
{
}
  yy_destructor(yypParser,97,&yymsp[0].minor);
        break;
      case 100: /* numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
      case 101: /* numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */ yytestcase(yyruleno==101);
      case 102: /* numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */ yytestcase(yyruleno==102);
      case 103: /* numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */ yytestcase(yyruleno==103);
{  yy_destructor(yypParser,96,&yymsp[-2].minor);
{
}
  yy_destructor(yypParser,96,&yymsp[0].minor);
}
        break;
      case 104: /* numeric_primary ::= value_expression_primary */
{  yy_destructor(yypParser,58,&yymsp[0].minor);
{
}
}
        break;
      case 105: /* numeric_primary ::= value_function */
{  yy_destructor(yypParser,94,&yymsp[0].minor);
{
}
}
        break;
      default:
      /* (76) general_literal ::= NK_STRING */ yytestcase(yyruleno==76);
      /* (77) general_literal ::= NK_BOOL */ yytestcase(yyruleno==77);
      /* (78) general_literal ::= NK_NOW */ yytestcase(yyruleno==78);
      /* (80) unsigned_value_specification ::= NK_QUESTION */ yytestcase(yyruleno==80);
      /* (87) predicate ::= */ yytestcase(yyruleno==87);
      /* (88) where_clause_opt ::= */ yytestcase(yyruleno==88);
      /* (89) partition_by_clause_opt ::= */ yytestcase(yyruleno==89);
      /* (90) twindow_clause_opt ::= */ yytestcase(yyruleno==90);
      /* (91) group_by_clause_opt ::= */ yytestcase(yyruleno==91);
      /* (92) having_clause_opt ::= */ yytestcase(yyruleno==92);
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
 printf("parsing complete!\n" );
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
