/*
 * Copyright (C) 1999-2003, 2008 Free Software Foundation, Inc.
 * This file is part of the GNU LIBICONV Library.
 *
 * The GNU LIBICONV Library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * The GNU LIBICONV Library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with the GNU LIBICONV Library; see the file COPYING.LIB.
 * If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
 * Fifth Floor, Boston, MA 02110-1301, USA.
 */

#ifdef USE_AIX
# if defined _AIX
#  include "aliases_aix_sysaix.h"
# else
#  include "aliases_aix.h"
# endif
#endif
#ifdef USE_OSF1
# if defined __osf__
#  include "aliases_osf1_sysosf1.h"
# else
#  include "aliases_osf1.h"
# endif
#endif
#ifdef USE_DOS
# include "aliases_dos.h"
#endif
#ifdef USE_EXTRA
# include "aliases_extra.h"
#endif
