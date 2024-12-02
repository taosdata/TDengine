/* Convert timestamp from time_t to struct tm.  */

/*
** This file is in the public domain, so clarified as of
** 1996-06-05 by Arthur David Olson.
*/

/*
** Leap second handling from Bradley White.
** POSIX.1-1988 style TZ environment variable handling from Guy Harris.
*/

/*LINTLIBRARY*/

#define LOCALTIME_IMPLEMENTATION
#include "private.h"

#include "tzdir.h"
#include "tzfile.h"
#include <fcntl.h>

#if defined THREAD_SAFE && THREAD_SAFE
# include <pthread.h>
static pthread_mutex_t locallock = PTHREAD_MUTEX_INITIALIZER;
static int lock(void) { return pthread_mutex_lock(&locallock); }
static void unlock(void) { pthread_mutex_unlock(&locallock); }
#else
static int lock(void) { return 0; }
static void unlock(void) { }
#endif

/* A signed type wider than int, so that we can add 1900 + tm_mon/12 to tm_year
   without overflow.  The static_assert checks that it is indeed wider
   than int; if this fails on your platform please let us know.  */
#if INT_MAX < LONG_MAX
typedef long iinntt;
# define IINNTT_MIN LONG_MIN
# define IINNTT_MAX LONG_MAX
#elif INT_MAX < LLONG_MAX
typedef long long iinntt;
# define IINNTT_MIN LLONG_MIN
# define IINNTT_MAX LLONG_MAX
#else
typedef intmax_t iinntt;
# define IINNTT_MIN INTMAX_MIN
# define IINNTT_MAX INTMAX_MAX
#endif
static_assert(IINNTT_MIN < INT_MIN && INT_MAX < IINNTT_MAX);

/* On platforms where offtime or mktime might overflow,
   strftime.c defines USE_TIMEX_T to be true and includes us.
   This tells us to #define time_t to an internal type timex_t that is
   wide enough so that strftime %s never suffers from integer overflow,
   and to #define offtime (if TM_GMTOFF is defined) or mktime (otherwise)
   to a static function that returns the redefined time_t.
   It also tells us to define only data and code needed
   to support the offtime or mktime variant.  */
#ifndef USE_TIMEX_T
# define USE_TIMEX_T false
#endif
#if USE_TIMEX_T
# undef TIME_T_MIN
# undef TIME_T_MAX
# undef time_t
# define time_t timex_t
# if MKTIME_FITS_IN(LONG_MIN, LONG_MAX)
typedef long timex_t;
# define TIME_T_MIN LONG_MIN
# define TIME_T_MAX LONG_MAX
# elif MKTIME_FITS_IN(LLONG_MIN, LLONG_MAX)
typedef long long timex_t;
# define TIME_T_MIN LLONG_MIN
# define TIME_T_MAX LLONG_MAX
# else
typedef intmax_t timex_t;
# define TIME_T_MIN INTMAX_MIN
# define TIME_T_MAX INTMAX_MAX
# endif

# ifdef TM_GMTOFF
#  undef timeoff
#  define timeoff timex_timeoff
#  undef EXTERN_TIMEOFF
# else
#  undef mktime
#  define mktime timex_mktime
# endif
#endif

#ifndef TZ_ABBR_CHAR_SET
# define TZ_ABBR_CHAR_SET \
	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 :+-._"
#endif /* !defined TZ_ABBR_CHAR_SET */

#ifndef TZ_ABBR_ERR_CHAR
# define TZ_ABBR_ERR_CHAR '_'
#endif /* !defined TZ_ABBR_ERR_CHAR */

/*
** Support non-POSIX platforms that distinguish between text and binary files.
*/

#ifndef O_BINARY
# define O_BINARY 0
#endif

#ifndef WILDABBR
/*
** Someone might make incorrect use of a time zone abbreviation:
**	1.	They might reference tzname[0] before calling tzset (explicitly
**		or implicitly).
**	2.	They might reference tzname[1] before calling tzset (explicitly
**		or implicitly).
**	3.	They might reference tzname[1] after setting to a time zone
**		in which Daylight Saving Time is never observed.
**	4.	They might reference tzname[0] after setting to a time zone
**		in which Standard Time is never observed.
**	5.	They might reference tm.TM_ZONE after calling offtime.
** What's best to do in the above cases is open to debate;
** for now, we just set things up so that in any of the five cases
** WILDABBR is used. Another possibility: initialize tzname[0] to the
** string "tzname[0] used before set", and similarly for the other cases.
** And another: initialize tzname[0] to "ERA", with an explanation in the
** manual page of what this "time zone abbreviation" means (doing this so
** that tzname[0] has the "normal" length of three characters).
*/
# define WILDABBR "   "
#endif /* !defined WILDABBR */

static const char	wildabbr[] = WILDABBR;

static char const etc_utc[] = "Etc/UTC";

#if defined TM_ZONE || ((!USE_TIMEX_T || !defined TM_GMTOFF) && defined TZ_NAME)
static char const *utc = etc_utc + sizeof "Etc/" - 1;
#endif

/*
** The DST rules to use if TZ has no rules and we can't load TZDEFRULES.
** Default to US rules as of 2017-05-07.
** POSIX does not specify the default DST rules;
** for historical reasons, US rules are a common default.
*/
#ifndef TZDEFRULESTRING
# define TZDEFRULESTRING ",M3.2.0,M11.1.0"
#endif

/* Limit to time zone abbreviation length in proleptic TZ strings.
   This is distinct from TZ_MAX_CHARS, which limits TZif file contents.
   It defaults to 254, not 255, so that desigidx_type can be an unsigned char.
   unsigned char suffices for TZif files, so the only reason to increase
   TZNAME_MAXIMUM is to support TZ strings specifying abbreviations
   longer than 254 bytes.  There is little reason to do that, though,
   as strings that long are hardly "abbreviations".  */
#ifndef TZNAME_MAXIMUM
# define TZNAME_MAXIMUM 254
#endif

#if TZNAME_MAXIMUM < UCHAR_MAX
typedef unsigned char desigidx_type;
#elif TZNAME_MAXIMUM < INT_MAX
typedef int desigidx_type;
#elif TZNAME_MAXIMUM < PTRDIFF_MAX
typedef ptrdiff_t desigidx_type;
#else
# error "TZNAME_MAXIMUM too large"
#endif

struct ttinfo {				/* time type information */
	int_least32_t	tt_utoff;	/* UT offset in seconds */
	desigidx_type	tt_desigidx;	/* abbreviation list index */
	bool		tt_isdst;	/* used to set tm_isdst */
	bool		tt_ttisstd;	/* transition is std time */
	bool		tt_ttisut;	/* transition is UT */
};

struct lsinfo {				/* leap second information */
	time_t		ls_trans;	/* transition time */
	int_fast32_t	ls_corr;	/* correction to apply */
};

/* This abbreviation means local time is unspecified.  */
static char const UNSPEC[] = "-00";

/* How many extra bytes are needed at the end of struct state's chars array.
   This needs to be at least 1 for null termination in case the input
   data isn't properly terminated, and it also needs to be big enough
   for ttunspecified to work without crashing.  */
enum { CHARS_EXTRA = max(sizeof UNSPEC, 2) - 1 };

/* A representation of the contents of a TZif file.  Ideally this
   would have no size limits; the following sizes should suffice for
   practical use.  This struct should not be too large, as instances
   are put on the stack and stacks are relatively small on some platforms.
   See tzfile.h for more about the sizes.  */
struct state {
	int		leapcnt;
	int		timecnt;
	int		typecnt;
	int		charcnt;
	bool		goback;
	bool		goahead;
	time_t		ats[TZ_MAX_TIMES];
	unsigned char	types[TZ_MAX_TIMES];
	struct ttinfo	ttis[TZ_MAX_TYPES];
	char chars[max(max(TZ_MAX_CHARS + CHARS_EXTRA, sizeof "UTC"),
		       2 * (TZNAME_MAXIMUM + 1))];
	struct lsinfo	lsis[TZ_MAX_LEAPS];
};

enum r_type {
  JULIAN_DAY,		/* Jn = Julian day */
  DAY_OF_YEAR,		/* n = day of year */
  MONTH_NTH_DAY_OF_WEEK	/* Mm.n.d = month, week, day of week */
};

struct rule {
	enum r_type	r_type;		/* type of rule */
	int		r_day;		/* day number of rule */
	int		r_week;		/* week number of rule */
	int		r_mon;		/* month number of rule */
	int_fast32_t	r_time;		/* transition time of rule */
};

static struct tm *gmtsub(struct state const *, time_t const *, int_fast32_t,
			 struct tm *);
static bool increment_overflow(int *, int);
static bool increment_overflow_time(time_t *, int_fast32_t);
static int_fast32_t leapcorr(struct state const *, time_t);
static struct tm *timesub(time_t const *, int_fast32_t, struct state const *,
			  struct tm *);
static bool tzparse(char const *, struct state *, struct state const *);

#ifdef ALL_STATE
static struct state *	lclptr;
static struct state *	gmtptr;
#endif /* defined ALL_STATE */

#ifndef ALL_STATE
static struct state	lclmem;
static struct state	gmtmem;
static struct state *const lclptr = &lclmem;
static struct state *const gmtptr = &gmtmem;
#endif /* State Farm */

#ifndef TZ_STRLEN_MAX
# define TZ_STRLEN_MAX 255
#endif /* !defined TZ_STRLEN_MAX */

#if !USE_TIMEX_T || !defined TM_GMTOFF
static char		lcl_TZname[TZ_STRLEN_MAX + 1];
static int		lcl_is_set;
#endif

/*
** Section 4.12.3 of X3.159-1989 requires that
**	Except for the strftime function, these functions [asctime,
**	ctime, gmtime, localtime] return values in one of two static
**	objects: a broken-down time structure and an array of char.
** Thanks to Paul Eggert for noting this.
**
** Although this requirement was removed in C99 it is still present in POSIX.
** Follow the requirement if SUPPORT_C89, even though this is more likely to
** trigger latent bugs in programs.
*/

#if !USE_TIMEX_T

# if SUPPORT_C89
static struct tm	tm;
#endif

# if 2 <= HAVE_TZNAME + TZ_TIME_T
char *			tzname[2] = {
	(char *) wildabbr,
	(char *) wildabbr
};
# endif
# if 2 <= USG_COMPAT + TZ_TIME_T
long			timezone;
int			daylight;
# endif
# if 2 <= ALTZONE + TZ_TIME_T
long			altzone;
# endif

#endif

/* Initialize *S to a value based on UTOFF, ISDST, and DESIGIDX.  */
static void
init_ttinfo(struct ttinfo *s, int_fast32_t utoff, bool isdst,
	    desigidx_type desigidx)
{
  s->tt_utoff = utoff;
  s->tt_isdst = isdst;
  s->tt_desigidx = desigidx;
  s->tt_ttisstd = false;
  s->tt_ttisut = false;
}

/* Return true if SP's time type I does not specify local time.  */
static bool
ttunspecified(struct state const *sp, int i)
{
  char const *abbr = &sp->chars[sp->ttis[i].tt_desigidx];
  /* memcmp is likely faster than strcmp, and is safe due to CHARS_EXTRA.  */
  return memcmp(abbr, UNSPEC, sizeof UNSPEC) == 0;
}

static int_fast32_t
detzcode(const char *const codep)
{
	register int_fast32_t	result;
	register int		i;
	int_fast32_t one = 1;
	int_fast32_t halfmaxval = one << (32 - 2);
	int_fast32_t maxval = halfmaxval - 1 + halfmaxval;
	int_fast32_t minval = -1 - maxval;

	result = codep[0] & 0x7f;
	for (i = 1; i < 4; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80) {
	  /* Do two's-complement negation even on non-two's-complement machines.
	     If the result would be minval - 1, return minval.  */
	  result -= !TWOS_COMPLEMENT(int_fast32_t) && result != 0;
	  result += minval;
	}
	return result;
}

static int_fast64_t
detzcode64(const char *const codep)
{
	register int_fast64_t result;
	register int	i;
	int_fast64_t one = 1;
	int_fast64_t halfmaxval = one << (64 - 2);
	int_fast64_t maxval = halfmaxval - 1 + halfmaxval;
	int_fast64_t minval = -TWOS_COMPLEMENT(int_fast64_t) - maxval;

	result = codep[0] & 0x7f;
	for (i = 1; i < 8; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80) {
	  /* Do two's-complement negation even on non-two's-complement machines.
	     If the result would be minval - 1, return minval.  */
	  result -= !TWOS_COMPLEMENT(int_fast64_t) && result != 0;
	  result += minval;
	}
	return result;
}

#if !USE_TIMEX_T || !defined TM_GMTOFF

static void
update_tzname_etc(struct state const *sp, struct ttinfo const *ttisp)
{
# if HAVE_TZNAME
  tzname[ttisp->tt_isdst] = (char *) &sp->chars[ttisp->tt_desigidx];
# endif
# if USG_COMPAT
  if (!ttisp->tt_isdst)
    timezone = - ttisp->tt_utoff;
# endif
# if ALTZONE
  if (ttisp->tt_isdst)
    altzone = - ttisp->tt_utoff;
# endif
}

/* If STDDST_MASK indicates that SP's TYPE provides useful info,
   update tzname, timezone, and/or altzone and return STDDST_MASK,
   diminished by the provided info if it is a specified local time.
   Otherwise, return STDDST_MASK.  See settzname for STDDST_MASK.  */
static int
may_update_tzname_etc(int stddst_mask, struct state *sp, int type)
{
  struct ttinfo *ttisp = &sp->ttis[type];
  int this_bit = 1 << ttisp->tt_isdst;
  if (stddst_mask & this_bit) {
    update_tzname_etc(sp, ttisp);
    if (!ttunspecified(sp, type))
      return stddst_mask & ~this_bit;
  }
  return stddst_mask;
}

static void
settzname(void)
{
	register struct state * const	sp = lclptr;
	register int			i;

	/* If STDDST_MASK & 1 we need info about a standard time.
	   If STDDST_MASK & 2 we need info about a daylight saving time.
	   When STDDST_MASK becomes zero we can stop looking.  */
	int stddst_mask = 0;

# if HAVE_TZNAME
	tzname[0] = tzname[1] = (char *) (sp ? wildabbr : utc);
	stddst_mask = 3;
# endif
# if USG_COMPAT
	timezone = 0;
	stddst_mask = 3;
# endif
# if ALTZONE
	altzone = 0;
	stddst_mask |= 2;
# endif
	/*
	** And to get the latest time zone abbreviations into tzname. . .
	*/
	if (sp) {
	  for (i = sp->timecnt - 1; stddst_mask && 0 <= i; i--)
	    stddst_mask = may_update_tzname_etc(stddst_mask, sp, sp->types[i]);
	  for (i = sp->typecnt - 1; stddst_mask && 0 <= i; i--)
	    stddst_mask = may_update_tzname_etc(stddst_mask, sp, i);
	}
# if USG_COMPAT
	daylight = stddst_mask >> 1 ^ 1;
# endif
}

/* Replace bogus characters in time zone abbreviations.
   Return 0 on success, an errno value if a time zone abbreviation is
   too long.  */
static int
scrub_abbrs(struct state *sp)
{
	int i;

	/* Reject overlong abbreviations.  */
	for (i = 0; i < sp->charcnt - (TZNAME_MAXIMUM + 1); ) {
	  int len = strlen(&sp->chars[i]);
	  if (TZNAME_MAXIMUM < len)
	    return EOVERFLOW;
	  i += len + 1;
	}

	/* Replace bogus characters.  */
	for (i = 0; i < sp->charcnt; ++i)
		if (strchr(TZ_ABBR_CHAR_SET, sp->chars[i]) == NULL)
			sp->chars[i] = TZ_ABBR_ERR_CHAR;

	return 0;
}

#endif

/* Input buffer for data read from a compiled tz file.  */
union input_buffer {
  /* The first part of the buffer, interpreted as a header.  */
  struct tzhead tzhead;

  /* The entire buffer.  Ideally this would have no size limits;
     the following should suffice for practical use.  */
  char buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
	   + 4 * TZ_MAX_TIMES];
};

/* TZDIR with a trailing '/' rather than a trailing '\0'.  */
static char const tzdirslash[sizeof TZDIR] = TZDIR "/";

/* Local storage needed for 'tzloadbody'.  */
union local_storage {
  /* The results of analyzing the file's contents after it is opened.  */
  struct file_analysis {
    /* The input buffer.  */
    union input_buffer u;

    /* A temporary state used for parsing a TZ string in the file.  */
    struct state st;
  } u;

  /* The name of the file to be opened.  Ideally this would have no
     size limits, to support arbitrarily long Zone names.
     Limiting Zone names to 1024 bytes should suffice for practical use.
     However, there is no need for this to be smaller than struct
     file_analysis as that struct is allocated anyway, as the other
     union member.  */
  char fullname[max(sizeof(struct file_analysis), sizeof tzdirslash + 1024)];
};

/* Load tz data from the file named NAME into *SP.  Read extended
   format if DOEXTEND.  Use *LSP for temporary storage.  Return 0 on
   success, an errno value on failure.  */
static int
tzloadbody(char const *name, struct state *sp, bool doextend,
	   union local_storage *lsp)
{
	register int			i;
	register int			fid;
	register int			stored;
	register ssize_t		nread;
	register bool doaccess;
	register union input_buffer *up = &lsp->u.u;
	register int tzheadsize = sizeof(struct tzhead);

	sp->goback = sp->goahead = false;

	if (! name) {
		name = TZDEFAULT;
		if (! name)
		  return EINVAL;
	}

	if (name[0] == ':')
		++name;
#ifdef SUPPRESS_TZDIR
	/* Do not prepend TZDIR.  This is intended for specialized
	   applications only, due to its security implications.  */
	doaccess = true;
#else
	doaccess = name[0] == '/';
#endif
	if (!doaccess) {
		char const *dot;
		if (sizeof lsp->fullname - sizeof tzdirslash <= strlen(name))
		  return ENAMETOOLONG;

		/* Create a string "TZDIR/NAME".  Using sprintf here
		   would pull in stdio (and would fail if the
		   resulting string length exceeded INT_MAX!).  */
		memcpy(lsp->fullname, tzdirslash, sizeof tzdirslash);
		strcpy(lsp->fullname + sizeof tzdirslash, name);

		/* Set doaccess if NAME contains a ".." file name
		   component, as such a name could read a file outside
		   the TZDIR virtual subtree.  */
		for (dot = name; (dot = strchr(dot, '.')); dot++)
		  if ((dot == name || dot[-1] == '/') && dot[1] == '.'
		      && (dot[2] == '/' || !dot[2])) {
		    doaccess = true;
		    break;
		  }

		name = lsp->fullname;
	}
	if (doaccess && access(name, R_OK) != 0)
	  return errno;
	fid = open(name, O_RDONLY | O_BINARY);
	if (fid < 0)
	  return errno;

	nread = read(fid, up->buf, sizeof up->buf);
	if (nread < tzheadsize) {
	  int err = nread < 0 ? errno : EINVAL;
	  close(fid);
	  return err;
	}
	if (close(fid) < 0)
	  return errno;
	for (stored = 4; stored <= 8; stored *= 2) {
	    char version = up->tzhead.tzh_version[0];
	    bool skip_datablock = stored == 4 && version;
	    int_fast32_t datablock_size;
	    int_fast32_t ttisstdcnt = detzcode(up->tzhead.tzh_ttisstdcnt);
	    int_fast32_t ttisutcnt = detzcode(up->tzhead.tzh_ttisutcnt);
	    int_fast64_t prevtr = -1;
	    int_fast32_t prevcorr;
	    int_fast32_t leapcnt = detzcode(up->tzhead.tzh_leapcnt);
	    int_fast32_t timecnt = detzcode(up->tzhead.tzh_timecnt);
	    int_fast32_t typecnt = detzcode(up->tzhead.tzh_typecnt);
	    int_fast32_t charcnt = detzcode(up->tzhead.tzh_charcnt);
	    char const *p = up->buf + tzheadsize;
	    /* Although tzfile(5) currently requires typecnt to be nonzero,
	       support future formats that may allow zero typecnt
	       in files that have a TZ string and no transitions.  */
	    if (! (0 <= leapcnt && leapcnt < TZ_MAX_LEAPS
		   && 0 <= typecnt && typecnt < TZ_MAX_TYPES
		   && 0 <= timecnt && timecnt < TZ_MAX_TIMES
		   && 0 <= charcnt && charcnt < TZ_MAX_CHARS
		   && 0 <= ttisstdcnt && ttisstdcnt < TZ_MAX_TYPES
		   && 0 <= ttisutcnt && ttisutcnt < TZ_MAX_TYPES))
	      return EINVAL;
	    datablock_size
		    = (timecnt * stored		/* ats */
		       + timecnt		/* types */
		       + typecnt * 6		/* ttinfos */
		       + charcnt		/* chars */
		       + leapcnt * (stored + 4)	/* lsinfos */
		       + ttisstdcnt		/* ttisstds */
		       + ttisutcnt);		/* ttisuts */
	    if (nread < tzheadsize + datablock_size)
	      return EINVAL;
	    if (skip_datablock)
		p += datablock_size;
	    else {
		if (! ((ttisstdcnt == typecnt || ttisstdcnt == 0)
		       && (ttisutcnt == typecnt || ttisutcnt == 0)))
		  return EINVAL;

		sp->leapcnt = leapcnt;
		sp->timecnt = timecnt;
		sp->typecnt = typecnt;
		sp->charcnt = charcnt;

		/* Read transitions, discarding those out of time_t range.
		   But pretend the last transition before TIME_T_MIN
		   occurred at TIME_T_MIN.  */
		timecnt = 0;
		for (i = 0; i < sp->timecnt; ++i) {
			int_fast64_t at
			  = stored == 4 ? detzcode(p) : detzcode64(p);
			sp->types[i] = at <= TIME_T_MAX;
			if (sp->types[i]) {
			  time_t attime
			    = ((TYPE_SIGNED(time_t) ? at < TIME_T_MIN : at < 0)
			       ? TIME_T_MIN : at);
			  if (timecnt && attime <= sp->ats[timecnt - 1]) {
			    if (attime < sp->ats[timecnt - 1])
			      return EINVAL;
			    sp->types[i - 1] = 0;
			    timecnt--;
			  }
			  sp->ats[timecnt++] = attime;
			}
			p += stored;
		}

		timecnt = 0;
		for (i = 0; i < sp->timecnt; ++i) {
			unsigned char typ = *p++;
			if (sp->typecnt <= typ)
			  return EINVAL;
			if (sp->types[i])
				sp->types[timecnt++] = typ;
		}
		sp->timecnt = timecnt;
		for (i = 0; i < sp->typecnt; ++i) {
			register struct ttinfo *	ttisp;
			unsigned char isdst, desigidx;

			ttisp = &sp->ttis[i];
			ttisp->tt_utoff = detzcode(p);
			p += 4;
			isdst = *p++;
			if (! (isdst < 2))
			  return EINVAL;
			ttisp->tt_isdst = isdst;
			desigidx = *p++;
			if (! (desigidx < sp->charcnt))
			  return EINVAL;
			ttisp->tt_desigidx = desigidx;
		}
		for (i = 0; i < sp->charcnt; ++i)
			sp->chars[i] = *p++;
		/* Ensure '\0'-terminated, and make it safe to call
		   ttunspecified later.  */
		memset(&sp->chars[i], 0, CHARS_EXTRA);

		/* Read leap seconds, discarding those out of time_t range.  */
		leapcnt = 0;
		for (i = 0; i < sp->leapcnt; ++i) {
		  int_fast64_t tr = stored == 4 ? detzcode(p) : detzcode64(p);
		  int_fast32_t corr = detzcode(p + stored);
		  p += stored + 4;

		  /* Leap seconds cannot occur before the Epoch,
		     or out of order.  */
		  if (tr <= prevtr)
		    return EINVAL;

		  /* To avoid other botches in this code, each leap second's
		     correction must differ from the previous one's by 1
		     second or less, except that the first correction can be
		     any value; these requirements are more generous than
		     RFC 9636, to allow future RFC extensions.  */
		  if (! (i == 0
			 || (prevcorr < corr
			     ? corr == prevcorr + 1
			     : (corr == prevcorr
				|| corr == prevcorr - 1))))
		    return EINVAL;
		  prevtr = tr;
		  prevcorr = corr;

		  if (tr <= TIME_T_MAX) {
		    sp->lsis[leapcnt].ls_trans = tr;
		    sp->lsis[leapcnt].ls_corr = corr;
		    leapcnt++;
		  }
		}
		sp->leapcnt = leapcnt;

		for (i = 0; i < sp->typecnt; ++i) {
			register struct ttinfo *	ttisp;

			ttisp = &sp->ttis[i];
			if (ttisstdcnt == 0)
				ttisp->tt_ttisstd = false;
			else {
				if (*p != true && *p != false)
				  return EINVAL;
				ttisp->tt_ttisstd = *p++;
			}
		}
		for (i = 0; i < sp->typecnt; ++i) {
			register struct ttinfo *	ttisp;

			ttisp = &sp->ttis[i];
			if (ttisutcnt == 0)
				ttisp->tt_ttisut = false;
			else {
				if (*p != true && *p != false)
						return EINVAL;
				ttisp->tt_ttisut = *p++;
			}
		}
	    }

	    nread -= p - up->buf;
	    memmove(up->buf, p, nread);

	    /* If this is an old file, we're done.  */
	    if (!version)
	      break;
	}
	if (doextend && nread > 2 &&
		up->buf[0] == '\n' && up->buf[nread - 1] == '\n' &&
		sp->typecnt + 2 <= TZ_MAX_TYPES) {
			struct state	*ts = &lsp->u.st;

			up->buf[nread - 1] = '\0';
			if (tzparse(&up->buf[1], ts, sp)) {

			  /* Attempt to reuse existing abbreviations.
			     Without this, America/Anchorage would be right on
			     the edge after 2037 when TZ_MAX_CHARS is 50, as
			     sp->charcnt equals 40 (for LMT AST AWT APT AHST
			     AHDT YST AKDT AKST) and ts->charcnt equals 10
			     (for AKST AKDT).  Reusing means sp->charcnt can
			     stay 40 in this example.  */
			  int gotabbr = 0;
			  int charcnt = sp->charcnt;
			  for (i = 0; i < ts->typecnt; i++) {
			    char *tsabbr = ts->chars + ts->ttis[i].tt_desigidx;
			    int j;
			    for (j = 0; j < charcnt; j++)
			      if (strcmp(sp->chars + j, tsabbr) == 0) {
				ts->ttis[i].tt_desigidx = j;
				gotabbr++;
				break;
			      }
			    if (! (j < charcnt)) {
			      int tsabbrlen = strlen(tsabbr);
			      if (j + tsabbrlen < TZ_MAX_CHARS) {
				strcpy(sp->chars + j, tsabbr);
				charcnt = j + tsabbrlen + 1;
				ts->ttis[i].tt_desigidx = j;
				gotabbr++;
			      }
			    }
			  }
			  if (gotabbr == ts->typecnt) {
			    sp->charcnt = charcnt;

			    /* Ignore any trailing, no-op transitions generated
			       by zic as they don't help here and can run afoul
			       of bugs in zic 2016j or earlier.  */
			    while (1 < sp->timecnt
				   && (sp->types[sp->timecnt - 1]
				       == sp->types[sp->timecnt - 2]))
			      sp->timecnt--;

			    sp->goahead = ts->goahead;

			    for (i = 0; i < ts->timecnt; i++) {
			      time_t t = ts->ats[i];
			      if (increment_overflow_time(&t, leapcorr(sp, t))
				  || (0 < sp->timecnt
				      && t <= sp->ats[sp->timecnt - 1]))
				continue;
			      if (TZ_MAX_TIMES <= sp->timecnt) {
				sp->goahead = false;
				break;
			      }
			      sp->ats[sp->timecnt] = t;
			      sp->types[sp->timecnt] = (sp->typecnt
							+ ts->types[i]);
			      sp->timecnt++;
			    }
			    for (i = 0; i < ts->typecnt; i++)
			      sp->ttis[sp->typecnt++] = ts->ttis[i];
			  }
			}
	}
	if (sp->typecnt == 0)
	  return EINVAL;

	return 0;
}

/* Load tz data from the file named NAME into *SP.  Read extended
   format if DOEXTEND.  Return 0 on success, an errno value on failure.  */
static int
tzload(char const *name, struct state *sp, bool doextend)
{
#ifdef ALL_STATE
  union local_storage *lsp = malloc(sizeof *lsp);
  if (!lsp) {
    return HAVE_MALLOC_ERRNO ? errno : ENOMEM;
  } else {
    int err = tzloadbody(name, sp, doextend, lsp);
    free(lsp);
    return err;
  }
#else
  union local_storage ls;
  return tzloadbody(name, sp, doextend, &ls);
#endif
}

static const int	mon_lengths[2][MONSPERYEAR] = {
	{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
	{ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
};

static const int	year_lengths[2] = {
	DAYSPERNYEAR, DAYSPERLYEAR
};

/* Is C an ASCII digit?  */
static bool
is_digit(char c)
{
  return '0' <= c && c <= '9';
}

/*
** Given a pointer into a timezone string, scan until a character that is not
** a valid character in a time zone abbreviation is found.
** Return a pointer to that character.
*/

ATTRIBUTE_PURE_114833 static const char *
getzname(register const char *strp)
{
	register char	c;

	while ((c = *strp) != '\0' && !is_digit(c) && c != ',' && c != '-' &&
		c != '+')
			++strp;
	return strp;
}

/*
** Given a pointer into an extended timezone string, scan until the ending
** delimiter of the time zone abbreviation is located.
** Return a pointer to the delimiter.
**
** As with getzname above, the legal character set is actually quite
** restricted, with other characters producing undefined results.
** We don't do any checking here; checking is done later in common-case code.
*/

ATTRIBUTE_PURE_114833 static const char *
getqzname(register const char *strp, const int delim)
{
	register int	c;

	while ((c = *strp) != '\0' && c != delim)
		++strp;
	return strp;
}

/*
** Given a pointer into a timezone string, extract a number from that string.
** Check that the number is within a specified range; if it is not, return
** NULL.
** Otherwise, return a pointer to the first character not part of the number.
*/

static const char *
getnum(register const char *strp, int *const nump, const int min, const int max)
{
	register char	c;
	register int	num;

	if (strp == NULL || !is_digit(c = *strp))
		return NULL;
	num = 0;
	do {
		num = num * 10 + (c - '0');
		if (num > max)
			return NULL;	/* illegal value */
		c = *++strp;
	} while (is_digit(c));
	if (num < min)
		return NULL;		/* illegal value */
	*nump = num;
	return strp;
}

/*
** Given a pointer into a timezone string, extract a number of seconds,
** in hh[:mm[:ss]] form, from the string.
** If any error occurs, return NULL.
** Otherwise, return a pointer to the first character not part of the number
** of seconds.
*/

static const char *
getsecs(register const char *strp, int_fast32_t *const secsp)
{
	int	num;
	int_fast32_t secsperhour = SECSPERHOUR;

	/*
	** 'HOURSPERDAY * DAYSPERWEEK - 1' allows quasi-POSIX rules like
	** "M10.4.6/26", which does not conform to POSIX,
	** but which specifies the equivalent of
	** "02:00 on the first Sunday on or after 23 Oct".
	*/
	strp = getnum(strp, &num, 0, HOURSPERDAY * DAYSPERWEEK - 1);
	if (strp == NULL)
		return NULL;
	*secsp = num * secsperhour;
	if (*strp == ':') {
		++strp;
		strp = getnum(strp, &num, 0, MINSPERHOUR - 1);
		if (strp == NULL)
			return NULL;
		*secsp += num * SECSPERMIN;
		if (*strp == ':') {
			++strp;
			/* 'SECSPERMIN' allows for leap seconds.  */
			strp = getnum(strp, &num, 0, SECSPERMIN);
			if (strp == NULL)
				return NULL;
			*secsp += num;
		}
	}
	return strp;
}

/*
** Given a pointer into a timezone string, extract an offset, in
** [+-]hh[:mm[:ss]] form, from the string.
** If any error occurs, return NULL.
** Otherwise, return a pointer to the first character not part of the time.
*/

static const char *
getoffset(register const char *strp, int_fast32_t *const offsetp)
{
	register bool neg = false;

	if (*strp == '-') {
		neg = true;
		++strp;
	} else if (*strp == '+')
		++strp;
	strp = getsecs(strp, offsetp);
	if (strp == NULL)
		return NULL;		/* illegal time */
	if (neg)
		*offsetp = -*offsetp;
	return strp;
}

/*
** Given a pointer into a timezone string, extract a rule in the form
** date[/time]. See POSIX Base Definitions section 8.3 variable TZ
** for the format of "date" and "time".
** If a valid rule is not found, return NULL.
** Otherwise, return a pointer to the first character not part of the rule.
*/

static const char *
getrule(const char *strp, register struct rule *const rulep)
{
	if (*strp == 'J') {
		/*
		** Julian day.
		*/
		rulep->r_type = JULIAN_DAY;
		++strp;
		strp = getnum(strp, &rulep->r_day, 1, DAYSPERNYEAR);
	} else if (*strp == 'M') {
		/*
		** Month, week, day.
		*/
		rulep->r_type = MONTH_NTH_DAY_OF_WEEK;
		++strp;
		strp = getnum(strp, &rulep->r_mon, 1, MONSPERYEAR);
		if (strp == NULL)
			return NULL;
		if (*strp++ != '.')
			return NULL;
		strp = getnum(strp, &rulep->r_week, 1, 5);
		if (strp == NULL)
			return NULL;
		if (*strp++ != '.')
			return NULL;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERWEEK - 1);
	} else if (is_digit(*strp)) {
		/*
		** Day of year.
		*/
		rulep->r_type = DAY_OF_YEAR;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERLYEAR - 1);
	} else	return NULL;		/* invalid format */
	if (strp == NULL)
		return NULL;
	if (*strp == '/') {
		/*
		** Time specified.
		*/
		++strp;
		strp = getoffset(strp, &rulep->r_time);
	} else	rulep->r_time = 2 * SECSPERHOUR;	/* default = 2:00:00 */
	return strp;
}

/*
** Given a year, a rule, and the offset from UT at the time that rule takes
** effect, calculate the year-relative time that rule takes effect.
*/

static int_fast32_t
transtime(const int year, register const struct rule *const rulep,
	  const int_fast32_t offset)
{
	register bool	leapyear;
	register int_fast32_t value;
	register int	i;
	int		d, m1, yy0, yy1, yy2, dow;

	leapyear = isleap(year);
	switch (rulep->r_type) {

	case JULIAN_DAY:
		/*
		** Jn - Julian day, 1 == January 1, 60 == March 1 even in leap
		** years.
		** In non-leap years, or if the day number is 59 or less, just
		** add SECSPERDAY times the day number-1 to the time of
		** January 1, midnight, to get the day.
		*/
		value = (rulep->r_day - 1) * SECSPERDAY;
		if (leapyear && rulep->r_day >= 60)
			value += SECSPERDAY;
		break;

	case DAY_OF_YEAR:
		/*
		** n - day of year.
		** Just add SECSPERDAY times the day number to the time of
		** January 1, midnight, to get the day.
		*/
		value = rulep->r_day * SECSPERDAY;
		break;

	case MONTH_NTH_DAY_OF_WEEK:
		/*
		** Mm.n.d - nth "dth day" of month m.
		*/

		/*
		** Use Zeller's Congruence to get day-of-week of first day of
		** month.
		*/
		m1 = (rulep->r_mon + 9) % 12 + 1;
		yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
		yy1 = yy0 / 100;
		yy2 = yy0 % 100;
		dow = ((26 * m1 - 2) / 10 +
			1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
		if (dow < 0)
			dow += DAYSPERWEEK;

		/*
		** "dow" is the day-of-week of the first day of the month. Get
		** the day-of-month (zero-origin) of the first "dow" day of the
		** month.
		*/
		d = rulep->r_day - dow;
		if (d < 0)
			d += DAYSPERWEEK;
		for (i = 1; i < rulep->r_week; ++i) {
			if (d + DAYSPERWEEK >=
				mon_lengths[leapyear][rulep->r_mon - 1])
					break;
			d += DAYSPERWEEK;
		}

		/*
		** "d" is the day-of-month (zero-origin) of the day we want.
		*/
		value = d * SECSPERDAY;
		for (i = 0; i < rulep->r_mon - 1; ++i)
			value += mon_lengths[leapyear][i] * SECSPERDAY;
		break;

	default: unreachable();
	}

	/*
	** "value" is the year-relative time of 00:00:00 UT on the day in
	** question. To get the year-relative time of the specified local
	** time on that day, add the transition time and the current offset
	** from UT.
	*/
	return value + rulep->r_time + offset;
}

/*
** Given a POSIX.1 proleptic TZ string, fill in the rule tables as
** appropriate.
*/

static bool
tzparse(const char *name, struct state *sp, struct state const *basep)
{
	const char *			stdname;
	const char *			dstname;
	int_fast32_t			stdoffset;
	int_fast32_t			dstoffset;
	register char *			cp;
	register bool			load_ok;
	ptrdiff_t stdlen, dstlen, charcnt;
	time_t atlo = TIME_T_MIN, leaplo = TIME_T_MIN;

	stdname = name;
	if (*name == '<') {
	  name++;
	  stdname = name;
	  name = getqzname(name, '>');
	  if (*name != '>')
	    return false;
	  stdlen = name - stdname;
	  name++;
	} else {
	  name = getzname(name);
	  stdlen = name - stdname;
	}
	if (! (0 < stdlen && stdlen <= TZNAME_MAXIMUM))
	  return false;
	name = getoffset(name, &stdoffset);
	if (name == NULL)
	  return false;
	charcnt = stdlen + 1;
	if (basep) {
	  if (0 < basep->timecnt)
	    atlo = basep->ats[basep->timecnt - 1];
	  load_ok = false;
	  sp->leapcnt = basep->leapcnt;
	  memcpy(sp->lsis, basep->lsis, sp->leapcnt * sizeof *sp->lsis);
	} else {
	  load_ok = tzload(TZDEFRULES, sp, false) == 0;
	  if (!load_ok)
	    sp->leapcnt = 0;	/* So, we're off a little.  */
	}
	if (0 < sp->leapcnt)
	  leaplo = sp->lsis[sp->leapcnt - 1].ls_trans;
	sp->goback = sp->goahead = false;
	if (*name != '\0') {
		if (*name == '<') {
			dstname = ++name;
			name = getqzname(name, '>');
			if (*name != '>')
			  return false;
			dstlen = name - dstname;
			name++;
		} else {
			dstname = name;
			name = getzname(name);
			dstlen = name - dstname; /* length of DST abbr. */
		}
		if (! (0 < dstlen && dstlen <= TZNAME_MAXIMUM))
		  return false;
		charcnt += dstlen + 1;
		if (*name != '\0' && *name != ',' && *name != ';') {
			name = getoffset(name, &dstoffset);
			if (name == NULL)
			  return false;
		} else	dstoffset = stdoffset - SECSPERHOUR;
		if (*name == '\0' && !load_ok)
			name = TZDEFRULESTRING;
		if (*name == ',' || *name == ';') {
			struct rule	start;
			struct rule	end;
			register int	year;
			register int	timecnt;
			time_t		janfirst;
			int_fast32_t janoffset = 0;
			int yearbeg, yearlim;

			++name;
			if ((name = getrule(name, &start)) == NULL)
			  return false;
			if (*name++ != ',')
			  return false;
			if ((name = getrule(name, &end)) == NULL)
			  return false;
			if (*name != '\0')
			  return false;
			sp->typecnt = 2;	/* standard time and DST */
			/*
			** Two transitions per year, from EPOCH_YEAR forward.
			*/
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			timecnt = 0;
			janfirst = 0;
			yearbeg = EPOCH_YEAR;

			do {
			  int_fast32_t yearsecs
			    = year_lengths[isleap(yearbeg - 1)] * SECSPERDAY;
			  time_t janfirst1 = janfirst;
			  yearbeg--;
			  if (increment_overflow_time(&janfirst1, -yearsecs)) {
			    janoffset = -yearsecs;
			    break;
			  }
			  janfirst = janfirst1;
			} while (atlo < janfirst
				 && EPOCH_YEAR - YEARSPERREPEAT / 2 < yearbeg);

			while (true) {
			  int_fast32_t yearsecs
			    = year_lengths[isleap(yearbeg)] * SECSPERDAY;
			  int yearbeg1 = yearbeg;
			  time_t janfirst1 = janfirst;
			  if (increment_overflow_time(&janfirst1, yearsecs)
			      || increment_overflow(&yearbeg1, 1)
			      || atlo <= janfirst1)
			    break;
			  yearbeg = yearbeg1;
			  janfirst = janfirst1;
			}

			yearlim = yearbeg;
			if (increment_overflow(&yearlim, years_of_observations))
			  yearlim = INT_MAX;
			for (year = yearbeg; year < yearlim; year++) {
				int_fast32_t
				  starttime = transtime(year, &start, stdoffset),
				  endtime = transtime(year, &end, dstoffset);
				int_fast32_t
				  yearsecs = (year_lengths[isleap(year)]
					      * SECSPERDAY);
				bool reversed = endtime < starttime;
				if (reversed) {
					int_fast32_t swap = starttime;
					starttime = endtime;
					endtime = swap;
				}
				if (reversed
				    || (starttime < endtime
					&& endtime - starttime < yearsecs)) {
					if (TZ_MAX_TIMES - 2 < timecnt)
						break;
					sp->ats[timecnt] = janfirst;
					if (! increment_overflow_time
					    (&sp->ats[timecnt],
					     janoffset + starttime)
					    && atlo <= sp->ats[timecnt])
					  sp->types[timecnt++] = !reversed;
					sp->ats[timecnt] = janfirst;
					if (! increment_overflow_time
					    (&sp->ats[timecnt],
					     janoffset + endtime)
					    && atlo <= sp->ats[timecnt]) {
					  sp->types[timecnt++] = reversed;
					}
				}
				if (endtime < leaplo) {
				  yearlim = year;
				  if (increment_overflow(&yearlim,
							 years_of_observations))
				    yearlim = INT_MAX;
				}
				if (increment_overflow_time
				    (&janfirst, janoffset + yearsecs))
					break;
				janoffset = 0;
			}
			sp->timecnt = timecnt;
			if (! timecnt) {
				sp->ttis[0] = sp->ttis[1];
				sp->typecnt = 1;	/* Perpetual DST.  */
			} else if (years_of_observations <= year - yearbeg)
				sp->goback = sp->goahead = true;
		} else {
			register int_fast32_t	theirstdoffset;
			register int_fast32_t	theirdstoffset;
			register int_fast32_t	theiroffset;
			register bool		isdst;
			register int		i;
			register int		j;

			if (*name != '\0')
			  return false;
			/*
			** Initial values of theirstdoffset and theirdstoffset.
			*/
			theirstdoffset = 0;
			for (i = 0; i < sp->timecnt; ++i) {
				j = sp->types[i];
				if (!sp->ttis[j].tt_isdst) {
					theirstdoffset =
						- sp->ttis[j].tt_utoff;
					break;
				}
			}
			theirdstoffset = 0;
			for (i = 0; i < sp->timecnt; ++i) {
				j = sp->types[i];
				if (sp->ttis[j].tt_isdst) {
					theirdstoffset =
						- sp->ttis[j].tt_utoff;
					break;
				}
			}
			/*
			** Initially we're assumed to be in standard time.
			*/
			isdst = false;
			/*
			** Now juggle transition times and types
			** tracking offsets as you do.
			*/
			for (i = 0; i < sp->timecnt; ++i) {
				j = sp->types[i];
				sp->types[i] = sp->ttis[j].tt_isdst;
				if (sp->ttis[j].tt_ttisut) {
					/* No adjustment to transition time */
				} else {
					/*
					** If daylight saving time is in
					** effect, and the transition time was
					** not specified as standard time, add
					** the daylight saving time offset to
					** the transition time; otherwise, add
					** the standard time offset to the
					** transition time.
					*/
					/*
					** Transitions from DST to DDST
					** will effectively disappear since
					** proleptic TZ strings have only one
					** DST offset.
					*/
					if (isdst && !sp->ttis[j].tt_ttisstd) {
						sp->ats[i] += dstoffset -
							theirdstoffset;
					} else {
						sp->ats[i] += stdoffset -
							theirstdoffset;
					}
				}
				theiroffset = -sp->ttis[j].tt_utoff;
				if (sp->ttis[j].tt_isdst)
					theirdstoffset = theiroffset;
				else	theirstdoffset = theiroffset;
			}
			/*
			** Finally, fill in ttis.
			*/
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			sp->typecnt = 2;
		}
	} else {
		dstlen = 0;
		sp->typecnt = 1;		/* only standard time */
		sp->timecnt = 0;
		init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
	}
	sp->charcnt = charcnt;
	cp = sp->chars;
	memcpy(cp, stdname, stdlen);
	cp += stdlen;
	*cp++ = '\0';
	if (dstlen != 0) {
		memcpy(cp, dstname, dstlen);
		*(cp + dstlen) = '\0';
	}
	return true;
}

static void
gmtload(struct state *const sp)
{
	if (tzload(etc_utc, sp, true) != 0)
	  tzparse("UTC0", sp, NULL);
}

#if !USE_TIMEX_T || !defined TM_GMTOFF

/* Initialize *SP to a value appropriate for the TZ setting NAME.
   Return 0 on success, an errno value on failure.  */
static int
zoneinit(struct state *sp, char const *name)
{
  if (name && ! name[0]) {
    /*
    ** User wants it fast rather than right.
    */
    sp->leapcnt = 0;		/* so, we're off a little */
    sp->timecnt = 0;
    sp->typecnt = 0;
    sp->charcnt = 0;
    sp->goback = sp->goahead = false;
    init_ttinfo(&sp->ttis[0], 0, false, 0);
    strcpy(sp->chars, utc);
    return 0;
  } else {
    int err = tzload(name, sp, true);
    if (err != 0 && name && name[0] != ':' && tzparse(name, sp, NULL))
      err = 0;
    if (err == 0)
      err = scrub_abbrs(sp);
    return err;
  }
}

static void
tzset_unlocked(void)
{
  char const *name = getenv("TZ");
  struct state *sp = lclptr;
  int lcl = name ? strlen(name) < sizeof lcl_TZname : -1;
  if (lcl < 0
      ? lcl_is_set < 0
      : 0 < lcl_is_set && strcmp(lcl_TZname, name) == 0)
    return;
# ifdef ALL_STATE
  if (! sp)
    lclptr = sp = malloc(sizeof *lclptr);
# endif
  if (sp) {
    if (zoneinit(sp, name) != 0)
      zoneinit(sp, "");
    if (0 < lcl)
      strcpy(lcl_TZname, name);
  }
  settzname();
  lcl_is_set = lcl;
}

#endif

#if !USE_TIMEX_T
void
tzset(void)
{
  if (lock() != 0)
    return;
  tzset_unlocked();
  unlock();
}
#endif

static void
gmtcheck(void)
{
  static bool gmt_is_set;
  if (lock() != 0)
    return;
  if (! gmt_is_set) {
#ifdef ALL_STATE
    gmtptr = malloc(sizeof *gmtptr);
#endif
    if (gmtptr)
      gmtload(gmtptr);
    gmt_is_set = true;
  }
  unlock();
}

#if NETBSD_INSPIRED && !USE_TIMEX_T

timezone_t
tzalloc(char const *name)
{
  timezone_t sp = malloc(sizeof *sp);
  if (sp) {
    int err = zoneinit(sp, name);
    if (err != 0) {
      free(sp);
      errno = err;
      return NULL;
    }
  } else if (!HAVE_MALLOC_ERRNO)
    errno = ENOMEM;
  return sp;
}

void
tzfree(timezone_t sp)
{
  free(sp);
}

/*
** NetBSD 6.1.4 has ctime_rz, but omit it because C23 deprecates ctime and
** POSIX.1-2024 removes ctime_r.  Both have potential security problems that
** ctime_rz would share.  Callers can instead use localtime_rz + strftime.
**
** NetBSD 6.1.4 has tzgetname, but omit it because it doesn't work
** in zones with three or more time zone abbreviations.
** Callers can instead use localtime_rz + strftime.
*/

#endif

#if !USE_TIMEX_T || !defined TM_GMTOFF

/*
** The easy way to behave "as if no library function calls" localtime
** is to not call it, so we drop its guts into "localsub", which can be
** freely called. (And no, the PANS doesn't require the above behavior,
** but it *is* desirable.)
**
** If successful and SETNAME is nonzero,
** set the applicable parts of tzname, timezone and altzone;
** however, it's OK to omit this step for proleptic TZ strings
** since in that case tzset should have already done this step correctly.
** SETNAME's type is int_fast32_t for compatibility with gmtsub,
** but it is actually a boolean and its value should be 0 or 1.
*/

/*ARGSUSED*/
static struct tm *
localsub(struct state const *sp, time_t const *timep, int_fast32_t setname,
	 struct tm *const tmp)
{
	register const struct ttinfo *	ttisp;
	register int			i;
	register struct tm *		result;
	const time_t			t = *timep;

	if (sp == NULL) {
	  /* Don't bother to set tzname etc.; tzset has already done it.  */
	  return gmtsub(gmtptr, timep, 0, tmp);
	}
	if ((sp->goback && t < sp->ats[0]) ||
		(sp->goahead && t > sp->ats[sp->timecnt - 1])) {
			time_t newt;
			register time_t		seconds;
			register time_t		years;

			if (t < sp->ats[0])
				seconds = sp->ats[0] - t;
			else	seconds = t - sp->ats[sp->timecnt - 1];
			--seconds;

			/* Beware integer overflow, as SECONDS might
			   be close to the maximum time_t.  */
			years = seconds / SECSPERREPEAT * YEARSPERREPEAT;
			seconds = years * AVGSECSPERYEAR;
			years += YEARSPERREPEAT;
			if (t < sp->ats[0])
			  newt = t + seconds + SECSPERREPEAT;
			else
			  newt = t - seconds - SECSPERREPEAT;

			if (newt < sp->ats[0] ||
				newt > sp->ats[sp->timecnt - 1])
					return NULL;	/* "cannot happen" */
			result = localsub(sp, &newt, setname, tmp);
			if (result) {
# if defined ckd_add && defined ckd_sub
				if (t < sp->ats[0]
				    ? ckd_sub(&result->tm_year,
					      result->tm_year, years)
				    : ckd_add(&result->tm_year,
					      result->tm_year, years))
				  return NULL;
# else
				register int_fast64_t newy;

				newy = result->tm_year;
				if (t < sp->ats[0])
					newy -= years;
				else	newy += years;
				if (! (INT_MIN <= newy && newy <= INT_MAX))
					return NULL;
				result->tm_year = newy;
# endif
			}
			return result;
	}
	if (sp->timecnt == 0 || t < sp->ats[0]) {
		i = 0;
	} else {
		register int	lo = 1;
		register int	hi = sp->timecnt;

		while (lo < hi) {
			register int	mid = (lo + hi) >> 1;

			if (t < sp->ats[mid])
				hi = mid;
			else	lo = mid + 1;
		}
		i = sp->types[lo - 1];
	}
	ttisp = &sp->ttis[i];
	/*
	** To get (wrong) behavior that's compatible with System V Release 2.0
	** you'd replace the statement below with
	**	t += ttisp->tt_utoff;
	**	timesub(&t, 0L, sp, tmp);
	*/
	result = timesub(&t, ttisp->tt_utoff, sp, tmp);
	if (result) {
	  result->tm_isdst = ttisp->tt_isdst;
# ifdef TM_ZONE
	  result->TM_ZONE = (char *) &sp->chars[ttisp->tt_desigidx];
# endif
	  if (setname)
	    update_tzname_etc(sp, ttisp);
	}
	return result;
}
#endif

#if !USE_TIMEX_T

# if NETBSD_INSPIRED
struct tm *
localtime_rz(struct state *restrict sp, time_t const *restrict timep,
	     struct tm *restrict tmp)
{
  return localsub(sp, timep, 0, tmp);
}
# endif

static struct tm *
localtime_tzset(time_t const *timep, struct tm *tmp, bool setname)
{
  int err = lock();
  if (err) {
    errno = err;
    return NULL;
  }
  if (setname || !lcl_is_set)
    tzset_unlocked();
  tmp = localsub(lclptr, timep, setname, tmp);
  unlock();
  return tmp;
}

struct tm *
localtime(const time_t *timep)
{
# if !SUPPORT_C89
  static struct tm tm;
# endif
  return localtime_tzset(timep, &tm, true);
}

struct tm *
localtime_r(const time_t *restrict timep, struct tm *restrict tmp)
{
  return localtime_tzset(timep, tmp, false);
}
#endif

/*
** gmtsub is to gmtime as localsub is to localtime.
*/

static struct tm *
gmtsub(ATTRIBUTE_MAYBE_UNUSED struct state const *sp, time_t const *timep,
       int_fast32_t offset, struct tm *tmp)
{
	register struct tm *	result;

	result = timesub(timep, offset, gmtptr, tmp);
#ifdef TM_ZONE
	/*
	** Could get fancy here and deliver something such as
	** "+xx" or "-xx" if offset is non-zero,
	** but this is no time for a treasure hunt.
	*/
	tmp->TM_ZONE = ((char *)
			(offset ? wildabbr : gmtptr ? gmtptr->chars : utc));
#endif /* defined TM_ZONE */
	return result;
}

#if !USE_TIMEX_T

/*
* Re-entrant version of gmtime.
*/

struct tm *
gmtime_r(time_t const *restrict timep, struct tm *restrict tmp)
{
  gmtcheck();
  return gmtsub(gmtptr, timep, 0, tmp);
}

struct tm *
gmtime(const time_t *timep)
{
# if !SUPPORT_C89
  static struct tm tm;
# endif
  return gmtime_r(timep, &tm);
}

# if STD_INSPIRED

/* This function is obsolescent and may disappear in future releases.
   Callers can instead use localtime_rz with a fixed-offset zone.  */

struct tm *
offtime(const time_t *timep, long offset)
{
  gmtcheck();

#  if !SUPPORT_C89
  static struct tm tm;
#  endif
  return gmtsub(gmtptr, timep, offset, &tm);
}

# endif
#endif

/*
** Return the number of leap years through the end of the given year
** where, to make the math easy, the answer for year zero is defined as zero.
*/

static time_t
leaps_thru_end_of_nonneg(time_t y)
{
  return y / 4 - y / 100 + y / 400;
}

static time_t
leaps_thru_end_of(time_t y)
{
  return (y < 0
	  ? -1 - leaps_thru_end_of_nonneg(-1 - y)
	  : leaps_thru_end_of_nonneg(y));
}

static struct tm *
timesub(const time_t *timep, int_fast32_t offset,
	const struct state *sp, struct tm *tmp)
{
	register const struct lsinfo *	lp;
	register time_t			tdays;
	register const int *		ip;
	register int_fast32_t		corr;
	register int			i;
	int_fast32_t idays, rem, dayoff, dayrem;
	time_t y;

	/* If less than SECSPERMIN, the number of seconds since the
	   most recent positive leap second; otherwise, do not add 1
	   to localtime tm_sec because of leap seconds.  */
	time_t secs_since_posleap = SECSPERMIN;

	corr = 0;
	i = (sp == NULL) ? 0 : sp->leapcnt;
	while (--i >= 0) {
		lp = &sp->lsis[i];
		if (*timep >= lp->ls_trans) {
			corr = lp->ls_corr;
			if ((i == 0 ? 0 : lp[-1].ls_corr) < corr)
			  secs_since_posleap = *timep - lp->ls_trans;
			break;
		}
	}

	/* Calculate the year, avoiding integer overflow even if
	   time_t is unsigned.  */
	tdays = *timep / SECSPERDAY;
	rem = *timep % SECSPERDAY;
	rem += offset % SECSPERDAY - corr % SECSPERDAY + 3 * SECSPERDAY;
	dayoff = offset / SECSPERDAY - corr / SECSPERDAY + rem / SECSPERDAY - 3;
	rem %= SECSPERDAY;
	/* y = (EPOCH_YEAR
		+ floor((tdays + dayoff) / DAYSPERREPEAT) * YEARSPERREPEAT),
	   sans overflow.  But calculate against 1570 (EPOCH_YEAR -
	   YEARSPERREPEAT) instead of against 1970 so that things work
	   for localtime values before 1970 when time_t is unsigned.  */
	dayrem = tdays % DAYSPERREPEAT;
	dayrem += dayoff % DAYSPERREPEAT;
	y = (EPOCH_YEAR - YEARSPERREPEAT
	     + ((1 + dayoff / DAYSPERREPEAT + dayrem / DAYSPERREPEAT
		 - ((dayrem % DAYSPERREPEAT) < 0)
		 + tdays / DAYSPERREPEAT)
		* YEARSPERREPEAT));
	/* idays = (tdays + dayoff) mod DAYSPERREPEAT, sans overflow.  */
	idays = tdays % DAYSPERREPEAT;
	idays += dayoff % DAYSPERREPEAT + 2 * DAYSPERREPEAT;
	idays %= DAYSPERREPEAT;
	/* Increase Y and decrease IDAYS until IDAYS is in range for Y.  */
	while (year_lengths[isleap(y)] <= idays) {
		int tdelta = idays / DAYSPERLYEAR;
		int_fast32_t ydelta = tdelta + !tdelta;
		time_t newy = y + ydelta;
		register int	leapdays;
		leapdays = leaps_thru_end_of(newy - 1) -
			leaps_thru_end_of(y - 1);
		idays -= ydelta * DAYSPERNYEAR;
		idays -= leapdays;
		y = newy;
	}

#ifdef ckd_add
	if (ckd_add(&tmp->tm_year, y, -TM_YEAR_BASE)) {
	  errno = EOVERFLOW;
	  return NULL;
	}
#else
	if (!TYPE_SIGNED(time_t) && y < TM_YEAR_BASE) {
	  int signed_y = y;
	  tmp->tm_year = signed_y - TM_YEAR_BASE;
	} else if ((!TYPE_SIGNED(time_t) || INT_MIN + TM_YEAR_BASE <= y)
		   && y - TM_YEAR_BASE <= INT_MAX)
	  tmp->tm_year = y - TM_YEAR_BASE;
	else {
	  errno = EOVERFLOW;
	  return NULL;
	}
#endif
	tmp->tm_yday = idays;
	/*
	** The "extra" mods below avoid overflow problems.
	*/
	tmp->tm_wday = (TM_WDAY_BASE
			+ ((tmp->tm_year % DAYSPERWEEK)
			   * (DAYSPERNYEAR % DAYSPERWEEK))
			+ leaps_thru_end_of(y - 1)
			- leaps_thru_end_of(TM_YEAR_BASE - 1)
			+ idays);
	tmp->tm_wday %= DAYSPERWEEK;
	if (tmp->tm_wday < 0)
		tmp->tm_wday += DAYSPERWEEK;
	tmp->tm_hour = rem / SECSPERHOUR;
	rem %= SECSPERHOUR;
	tmp->tm_min = rem / SECSPERMIN;
	tmp->tm_sec = rem % SECSPERMIN;

	/* Use "... ??:??:60" at the end of the localtime minute containing
	   the second just before the positive leap second.  */
	tmp->tm_sec += secs_since_posleap <= tmp->tm_sec;

	ip = mon_lengths[isleap(y)];
	for (tmp->tm_mon = 0; idays >= ip[tmp->tm_mon]; ++(tmp->tm_mon))
		idays -= ip[tmp->tm_mon];
	tmp->tm_mday = idays + 1;
	tmp->tm_isdst = 0;
#ifdef TM_GMTOFF
	tmp->TM_GMTOFF = offset;
#endif /* defined TM_GMTOFF */
	return tmp;
}

/*
** Adapted from code provided by Robert Elz, who writes:
**	The "best" way to do mktime I think is based on an idea of Bob
**	Kridle's (so its said...) from a long time ago.
**	It does a binary search of the time_t space. Since time_t's are
**	just 32 bits, its a max of 32 iterations (even at 64 bits it
**	would still be very reasonable).
*/

#ifndef WRONG
# define WRONG (-1)
#endif /* !defined WRONG */

/*
** Normalize logic courtesy Paul Eggert.
*/

static bool
increment_overflow(int *ip, int j)
{
#ifdef ckd_add
	return ckd_add(ip, *ip, j);
#else
	register int const	i = *ip;

	/*
	** If i >= 0 there can only be overflow if i + j > INT_MAX
	** or if j > INT_MAX - i; given i >= 0, INT_MAX - i cannot overflow.
	** If i < 0 there can only be overflow if i + j < INT_MIN
	** or if j < INT_MIN - i; given i < 0, INT_MIN - i cannot overflow.
	*/
	if ((i >= 0) ? (j > INT_MAX - i) : (j < INT_MIN - i))
		return true;
	*ip += j;
	return false;
#endif
}

static bool
increment_overflow_time_iinntt(time_t *tp, iinntt j)
{
#ifdef ckd_add
  return ckd_add(tp, *tp, j);
#else
  if (j < 0
      ? (TYPE_SIGNED(time_t) ? *tp < TIME_T_MIN - j : *tp <= -1 - j)
      : TIME_T_MAX - j < *tp)
    return true;
  *tp += j;
  return false;
#endif
}

static bool
increment_overflow_time(time_t *tp, int_fast32_t j)
{
#ifdef ckd_add
	return ckd_add(tp, *tp, j);
#else
	/*
	** This is like
	** 'if (! (TIME_T_MIN <= *tp + j && *tp + j <= TIME_T_MAX)) ...',
	** except that it does the right thing even if *tp + j would overflow.
	*/
	if (! (j < 0
	       ? (TYPE_SIGNED(time_t) ? TIME_T_MIN - j <= *tp : -1 - j < *tp)
	       : *tp <= TIME_T_MAX - j))
		return true;
	*tp += j;
	return false;
#endif
}

static int
tmcomp(register const struct tm *const atmp,
       register const struct tm *const btmp)
{
	register int	result;

	if (atmp->tm_year != btmp->tm_year)
		return atmp->tm_year < btmp->tm_year ? -1 : 1;
	if ((result = (atmp->tm_mon - btmp->tm_mon)) == 0 &&
		(result = (atmp->tm_mday - btmp->tm_mday)) == 0 &&
		(result = (atmp->tm_hour - btmp->tm_hour)) == 0 &&
		(result = (atmp->tm_min - btmp->tm_min)) == 0)
			result = atmp->tm_sec - btmp->tm_sec;
	return result;
}

/* Copy to *DEST from *SRC.  Copy only the members needed for mktime,
   as other members might not be initialized.  */
static void
mktmcpy(struct tm *dest, struct tm const *src)
{
  dest->tm_sec = src->tm_sec;
  dest->tm_min = src->tm_min;
  dest->tm_hour = src->tm_hour;
  dest->tm_mday = src->tm_mday;
  dest->tm_mon = src->tm_mon;
  dest->tm_year = src->tm_year;
  dest->tm_isdst = src->tm_isdst;
#if defined TM_GMTOFF && ! UNINIT_TRAP
  dest->TM_GMTOFF = src->TM_GMTOFF;
#endif
}

static time_t
time2sub(struct tm *const tmp,
	 struct tm *(*funcp)(struct state const *, time_t const *,
			     int_fast32_t, struct tm *),
	 struct state const *sp,
	 const int_fast32_t offset,
	 bool *okayp,
	 bool do_norm_secs)
{
	register int			dir;
	register int			i, j;
	register time_t			lo;
	register time_t			hi;
	iinntt y, mday, hour, min, saved_seconds;
	time_t				newt;
	time_t				t;
	struct tm			yourtm, mytm;

	*okayp = false;
	mktmcpy(&yourtm, tmp);

	min = yourtm.tm_min;
	if (do_norm_secs) {
	  min += yourtm.tm_sec / SECSPERMIN;
	  yourtm.tm_sec %= SECSPERMIN;
	  if (yourtm.tm_sec < 0) {
	    yourtm.tm_sec += SECSPERMIN;
	    min--;
	  }
	}

	hour = yourtm.tm_hour;
	hour += min / MINSPERHOUR;
	yourtm.tm_min = min % MINSPERHOUR;
	if (yourtm.tm_min < 0) {
	  yourtm.tm_min += MINSPERHOUR;
	  hour--;
	}

	mday = yourtm.tm_mday;
	mday += hour / HOURSPERDAY;
	yourtm.tm_hour = hour % HOURSPERDAY;
	if (yourtm.tm_hour < 0) {
	  yourtm.tm_hour += HOURSPERDAY;
	  mday--;
	}

	y = yourtm.tm_year;
	y += yourtm.tm_mon / MONSPERYEAR;
	yourtm.tm_mon %= MONSPERYEAR;
	if (yourtm.tm_mon < 0) {
	  yourtm.tm_mon += MONSPERYEAR;
	  y--;
	}

	/*
	** Turn y into an actual year number for now.
	** It is converted back to an offset from TM_YEAR_BASE later.
	*/
	y += TM_YEAR_BASE;

	while (mday <= 0) {
	  iinntt li = y - (yourtm.tm_mon <= 1);
	  mday += year_lengths[isleap(li)];
	  y--;
	}
	while (DAYSPERLYEAR < mday) {
	  iinntt li = y + (1 < yourtm.tm_mon);
	  mday -= year_lengths[isleap(li)];
	  y++;
	}
	yourtm.tm_mday = mday;
	for ( ; ; ) {
		i = mon_lengths[isleap(y)][yourtm.tm_mon];
		if (yourtm.tm_mday <= i)
			break;
		yourtm.tm_mday -= i;
		if (++yourtm.tm_mon >= MONSPERYEAR) {
			yourtm.tm_mon = 0;
			y++;
		}
	}
#ifdef ckd_add
	if (ckd_add(&yourtm.tm_year, y, -TM_YEAR_BASE))
	  return WRONG;
#else
	y -= TM_YEAR_BASE;
	if (! (INT_MIN <= y && y <= INT_MAX))
		return WRONG;
	yourtm.tm_year = y;
#endif
	if (yourtm.tm_sec >= 0 && yourtm.tm_sec < SECSPERMIN)
		saved_seconds = 0;
	else if (yourtm.tm_year < EPOCH_YEAR - TM_YEAR_BASE) {
		/*
		** We can't set tm_sec to 0, because that might push the
		** time below the minimum representable time.
		** Set tm_sec to 59 instead.
		** This assumes that the minimum representable time is
		** not in the same minute that a leap second was deleted from,
		** which is a safer assumption than using 58 would be.
		*/
		saved_seconds = yourtm.tm_sec;
		saved_seconds -= SECSPERMIN - 1;
		yourtm.tm_sec = SECSPERMIN - 1;
	} else {
		saved_seconds = yourtm.tm_sec;
		yourtm.tm_sec = 0;
	}
	/*
	** Do a binary search (this works whatever time_t's type is).
	*/
	lo = TIME_T_MIN;
	hi = TIME_T_MAX;
	for ( ; ; ) {
		t = lo / 2 + hi / 2;
		if (t < lo)
			t = lo;
		else if (t > hi)
			t = hi;
		if (! funcp(sp, &t, offset, &mytm)) {
			/*
			** Assume that t is too extreme to be represented in
			** a struct tm; arrange things so that it is less
			** extreme on the next pass.
			*/
			dir = (t > 0) ? 1 : -1;
		} else	dir = tmcomp(&mytm, &yourtm);
		if (dir != 0) {
			if (t == lo) {
				if (t == TIME_T_MAX)
					return WRONG;
				++t;
				++lo;
			} else if (t == hi) {
				if (t == TIME_T_MIN)
					return WRONG;
				--t;
				--hi;
			}
			if (lo > hi)
				return WRONG;
			if (dir > 0)
				hi = t;
			else	lo = t;
			continue;
		}
#if defined TM_GMTOFF && ! UNINIT_TRAP
		if (mytm.TM_GMTOFF != yourtm.TM_GMTOFF
		    && (yourtm.TM_GMTOFF < 0
			? (-SECSPERDAY <= yourtm.TM_GMTOFF
			   && (mytm.TM_GMTOFF <=
			       (min(INT_FAST32_MAX, LONG_MAX)
				+ yourtm.TM_GMTOFF)))
			: (yourtm.TM_GMTOFF <= SECSPERDAY
			   && ((max(INT_FAST32_MIN, LONG_MIN)
				+ yourtm.TM_GMTOFF)
			       <= mytm.TM_GMTOFF)))) {
		  /* MYTM matches YOURTM except with the wrong UT offset.
		     YOURTM.TM_GMTOFF is plausible, so try it instead.
		     It's OK if YOURTM.TM_GMTOFF contains uninitialized data,
		     since the guess gets checked.  */
		  time_t altt = t;
		  int_fast32_t diff = mytm.TM_GMTOFF - yourtm.TM_GMTOFF;
		  if (!increment_overflow_time(&altt, diff)) {
		    struct tm alttm;
		    if (funcp(sp, &altt, offset, &alttm)
			&& alttm.tm_isdst == mytm.tm_isdst
			&& alttm.TM_GMTOFF == yourtm.TM_GMTOFF
			&& tmcomp(&alttm, &yourtm) == 0) {
		      t = altt;
		      mytm = alttm;
		    }
		  }
		}
#endif
		if (yourtm.tm_isdst < 0 || mytm.tm_isdst == yourtm.tm_isdst)
			break;
		/*
		** Right time, wrong type.
		** Hunt for right time, right type.
		** It's okay to guess wrong since the guess
		** gets checked.
		*/
		if (sp == NULL)
			return WRONG;
		for (i = sp->typecnt - 1; i >= 0; --i) {
			if (sp->ttis[i].tt_isdst != yourtm.tm_isdst)
				continue;
			for (j = sp->typecnt - 1; j >= 0; --j) {
				if (sp->ttis[j].tt_isdst == yourtm.tm_isdst)
					continue;
				if (ttunspecified(sp, j))
				  continue;
				newt = (t + sp->ttis[j].tt_utoff
					- sp->ttis[i].tt_utoff);
				if (! funcp(sp, &newt, offset, &mytm))
					continue;
				if (tmcomp(&mytm, &yourtm) != 0)
					continue;
				if (mytm.tm_isdst != yourtm.tm_isdst)
					continue;
				/*
				** We have a match.
				*/
				t = newt;
				goto label;
			}
		}
		return WRONG;
	}
label:
	if (increment_overflow_time_iinntt(&t, saved_seconds))
		return WRONG;
	if (funcp(sp, &t, offset, tmp))
		*okayp = true;
	return t;
}

static time_t
time2(struct tm * const	tmp,
      struct tm *(*funcp)(struct state const *, time_t const *,
			  int_fast32_t, struct tm *),
      struct state const *sp,
      const int_fast32_t offset,
      bool *okayp)
{
	time_t	t;

	/*
	** First try without normalization of seconds
	** (in case tm_sec contains a value associated with a leap second).
	** If that fails, try with normalization of seconds.
	*/
	t = time2sub(tmp, funcp, sp, offset, okayp, false);
	return *okayp ? t : time2sub(tmp, funcp, sp, offset, okayp, true);
}

static time_t
time1(struct tm *const tmp,
      struct tm *(*funcp)(struct state const *, time_t const *,
			  int_fast32_t, struct tm *),
      struct state const *sp,
      const int_fast32_t offset)
{
	register time_t			t;
	register int			samei, otheri;
	register int			sameind, otherind;
	register int			i;
	register int			nseen;
	char				seen[TZ_MAX_TYPES];
	unsigned char			types[TZ_MAX_TYPES];
	bool				okay;

	if (tmp == NULL) {
		errno = EINVAL;
		return WRONG;
	}
	if (tmp->tm_isdst > 1)
		tmp->tm_isdst = 1;
	t = time2(tmp, funcp, sp, offset, &okay);
	if (okay)
		return t;
	if (tmp->tm_isdst < 0)
#ifdef PCTS
		/*
		** POSIX Conformance Test Suite code courtesy Grant Sullivan.
		*/
		tmp->tm_isdst = 0;	/* reset to std and try again */
#else
		return t;
#endif /* !defined PCTS */
	/*
	** We're supposed to assume that somebody took a time of one type
	** and did some math on it that yielded a "struct tm" that's bad.
	** We try to divine the type they started from and adjust to the
	** type they need.
	*/
	if (sp == NULL)
		return WRONG;
	for (i = 0; i < sp->typecnt; ++i)
		seen[i] = false;
	nseen = 0;
	for (i = sp->timecnt - 1; i >= 0; --i)
		if (!seen[sp->types[i]] && !ttunspecified(sp, sp->types[i])) {
			seen[sp->types[i]] = true;
			types[nseen++] = sp->types[i];
		}
	for (sameind = 0; sameind < nseen; ++sameind) {
		samei = types[sameind];
		if (sp->ttis[samei].tt_isdst != tmp->tm_isdst)
			continue;
		for (otherind = 0; otherind < nseen; ++otherind) {
			otheri = types[otherind];
			if (sp->ttis[otheri].tt_isdst == tmp->tm_isdst)
				continue;
			tmp->tm_sec += (sp->ttis[otheri].tt_utoff
					- sp->ttis[samei].tt_utoff);
			tmp->tm_isdst = !tmp->tm_isdst;
			t = time2(tmp, funcp, sp, offset, &okay);
			if (okay)
				return t;
			tmp->tm_sec -= (sp->ttis[otheri].tt_utoff
					- sp->ttis[samei].tt_utoff);
			tmp->tm_isdst = !tmp->tm_isdst;
		}
	}
	return WRONG;
}

#if !defined TM_GMTOFF || !USE_TIMEX_T

static time_t
mktime_tzname(struct state *sp, struct tm *tmp, bool setname)
{
  if (sp)
    return time1(tmp, localsub, sp, setname);
  else {
    gmtcheck();
    return time1(tmp, gmtsub, gmtptr, 0);
  }
}

# if USE_TIMEX_T
static
# endif
time_t
mktime(struct tm *tmp)
{
  time_t t;
  int err = lock();
  if (err) {
    errno = err;
    return -1;
  }
  tzset_unlocked();
  t = mktime_tzname(lclptr, tmp, true);
  unlock();
  return t;
}

#endif

#if NETBSD_INSPIRED && !USE_TIMEX_T
time_t
mktime_z(struct state *restrict sp, struct tm *restrict tmp)
{
  return mktime_tzname(sp, tmp, false);
}
#endif

#if STD_INSPIRED && !USE_TIMEX_T
/* This function is obsolescent and may disappear in future releases.
   Callers can instead use mktime.  */
time_t
timelocal(struct tm *tmp)
{
	if (tmp != NULL)
		tmp->tm_isdst = -1;	/* in case it wasn't initialized */
	return mktime(tmp);
}
#endif

#if defined TM_GMTOFF || !USE_TIMEX_T

# ifndef EXTERN_TIMEOFF
#  ifndef timeoff
#   define timeoff my_timeoff /* Don't collide with OpenBSD 7.4 <time.h>.  */
#  endif
#  define EXTERN_TIMEOFF static
# endif

/* This function is obsolescent and may disappear in future releases.
   Callers can instead use mktime_z with a fixed-offset zone.  */
EXTERN_TIMEOFF time_t
timeoff(struct tm *tmp, long offset)
{
  if (tmp)
    tmp->tm_isdst = 0;
  gmtcheck();
  return time1(tmp, gmtsub, gmtptr, offset);
}
#endif

#if !USE_TIMEX_T
time_t
timegm(struct tm *tmp)
{
  time_t t;
  struct tm tmcpy;
  mktmcpy(&tmcpy, tmp);
  tmcpy.tm_wday = -1;
  t = timeoff(&tmcpy, 0);
  if (0 <= tmcpy.tm_wday)
    *tmp = tmcpy;
  return t;
}
#endif

static int_fast32_t
leapcorr(struct state const *sp, time_t t)
{
	register struct lsinfo const *	lp;
	register int			i;

	i = sp->leapcnt;
	while (--i >= 0) {
		lp = &sp->lsis[i];
		if (t >= lp->ls_trans)
			return lp->ls_corr;
	}
	return 0;
}

/*
** XXX--is the below the right way to conditionalize??
*/

#if !USE_TIMEX_T
# if STD_INSPIRED

/* NETBSD_INSPIRED_EXTERN functions are exported to callers if
   NETBSD_INSPIRED is defined, and are private otherwise.  */
#  if NETBSD_INSPIRED
#   define NETBSD_INSPIRED_EXTERN
#  else
#   define NETBSD_INSPIRED_EXTERN static
#  endif

/*
** IEEE Std 1003.1 (POSIX) says that 536457599
** shall correspond to "Wed Dec 31 23:59:59 UTC 1986", which
** is not the case if we are accounting for leap seconds.
** So, we provide the following conversion routines for use
** when exchanging timestamps with POSIX conforming systems.
*/

NETBSD_INSPIRED_EXTERN time_t
time2posix_z(struct state *sp, time_t t)
{
  return t - leapcorr(sp, t);
}

time_t
time2posix(time_t t)
{
  int err = lock();
  if (err) {
    errno = err;
    return -1;
  }
  if (!lcl_is_set)
    tzset_unlocked();
  if (lclptr)
    t = time2posix_z(lclptr, t);
  unlock();
  return t;
}

NETBSD_INSPIRED_EXTERN time_t
posix2time_z(struct state *sp, time_t t)
{
	time_t	x;
	time_t	y;
	/*
	** For a positive leap second hit, the result
	** is not unique. For a negative leap second
	** hit, the corresponding time doesn't exist,
	** so we return an adjacent second.
	*/
	x = t + leapcorr(sp, t);
	y = x - leapcorr(sp, x);
	if (y < t) {
		do {
			x++;
			y = x - leapcorr(sp, x);
		} while (y < t);
		x -= y != t;
	} else if (y > t) {
		do {
			--x;
			y = x - leapcorr(sp, x);
		} while (y > t);
		x += y != t;
	}
	return x;
}

time_t
posix2time(time_t t)
{
  int err = lock();
  if (err) {
    errno = err;
    return -1;
  }
  if (!lcl_is_set)
    tzset_unlocked();
  if (lclptr)
    t = posix2time_z(lclptr, t);
  unlock();
  return t;
}

# endif /* STD_INSPIRED */

# if TZ_TIME_T

#  if !USG_COMPAT
#   define timezone 0
#  endif

/* Convert from the underlying system's time_t to the ersatz time_tz,
   which is called 'time_t' in this file.  Typically, this merely
   converts the time's integer width.  On some platforms, the system
   time is local time not UT, or uses some epoch other than the POSIX
   epoch.

   Although this code appears to define a function named 'time' that
   returns time_t, the macros in private.h cause this code to actually
   define a function named 'tz_time' that returns tz_time_t.  The call
   to sys_time invokes the underlying system's 'time' function.  */

time_t
time(time_t *p)
{
  time_t r = sys_time(0);
  if (r != (time_t) -1) {
    iinntt offset = EPOCH_LOCAL ? timezone : 0;
    if (offset < IINNTT_MIN + EPOCH_OFFSET
	|| increment_overflow_time_iinntt(&r, offset - EPOCH_OFFSET)) {
      errno = EOVERFLOW;
      r = -1;
    }
  }
  if (p)
    *p = r;
  return r;
}

# endif
#endif
