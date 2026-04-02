/* Perform additional initialization for getopt functions in GNU libc.
   Copyright (C) 1997, 1998, 2001 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Ulrich Drepper <drepper@cygnus.com>, 1997.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

#ifdef USE_NONOPTION_FLAGS
/* Attention: this file is *not* necessary when the GNU getopt functions
   are used outside the GNU libc.  Some additional functionality of the
   getopt functions in GNU libc require this additional work.  */

#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include <stdio-common/_itoa.h>

/* Variable to synchronize work.  */
char *__getopt_nonoption_flags;


/* Remove the environment variable "_<PID>_GNU_nonoption_argv_flags_" if
   it is still available.  If the getopt functions are also used in the
   application it does not exist anymore since it was saved for the use
   in getopt.  */
void
__getopt_clean_environment (char **env)
{
  /* Bash 2.0 puts a special variable in the environment for each
     command it runs, specifying which ARGV elements are the results
     of file name wildcard expansion and therefore should not be
     considered as options.  */
  static const char envvar_tail[] = "_GNU_nonoption_argv_flags_=";
  char var[50];
  char *cp, **ep;
  size_t len;

  /* Construct the "_<PID>_GNU_nonoption_argv_flags_=" string.  We must
     not use `sprintf'.  */
  cp = memcpy (&var[sizeof (var) - sizeof (envvar_tail)], envvar_tail,
	       sizeof (envvar_tail));
  cp = _itoa_word (__getpid (), cp, 10, 0);
  /* Note: we omit adding the leading '_' since we explicitly test for
     it before calling strncmp.  */
  len = (var + sizeof (var) - 1) - cp;

  for (ep = env; *ep != NULL; ++ep)
    if ((*ep)[0] == '_'
	&& __builtin_expect (strncmp (*ep + 1, cp, len) == 0, 0))
      {
	/* Found it.  Store this pointer and move later ones back.  */
	char **dp = ep;
	__getopt_nonoption_flags = &(*ep)[len];
	do
	  dp[0] = dp[1];
	while (*dp++);
	/* Continue the loop in case the name appears again.  */
      }
}
#endif	/* USE_NONOPTION_FLAGS */
