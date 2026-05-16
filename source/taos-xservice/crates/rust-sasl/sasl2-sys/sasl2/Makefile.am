AUTOMAKE_OPTIONS = 1.7
ACLOCAL_AMFLAGS = -I m4
# Top-level Makefile.am for SASL
# Rob Earhart
#
################################################################
# Copyright (c) 2000 Carnegie Mellon University.  All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer. 
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# 3. The name "Carnegie Mellon University" must not be used to
#    endorse or promote products derived from this software without
#    prior written permission. For permission or any other legal
#    details, please contact  
#      Office of Technology Transfer
#      Carnegie Mellon University
#      5000 Forbes Avenue
#      Pittsburgh, PA  15213-3890
#      (412) 268-4387, fax: (412) 268-7395
#      tech-transfer@andrew.cmu.edu
#
# 4. Redistributions of any form whatsoever must retain the following
#    acknowledgment:
#    "This product includes software developed by Computing Services
#     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
#
# CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
# THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
# FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
# AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
# OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
################################################################

if SASLAUTHD
SAD = saslauthd
else
SAD =
endif
if PWCHECK
PWC = pwcheck
else
PWC = 
endif
if SAMPLE
SAM = sample
else
SAM =
endif
if MACOSX
INSTALLOSX = install-exec-local-osx
else 
INSTALLOSX = 
endif

SUBDIRS=include sasldb common lib plugins utils $(PWC) $(SAM) $(SAD)
EXTRA_DIST=config doc docsrc win32 mac dlcompat-20010505 NTMakefile \
    INSTALL.TXT libsasl2.pc.in

dist_man3_MANS = \
    man/sasl.3 \
    man/sasl_authorize_t.3 \
    man/sasl_auxprop.3 \
    man/sasl_auxprop_getctx.3 \
    man/sasl_auxprop_request.3 \
    man/sasl_callbacks.3 \
    man/sasl_canon_user_t.3 \
    man/sasl_chalprompt_t.3 \
    man/sasl_checkapop.3 \
    man/sasl_checkpass.3 \
    man/sasl_client_init.3 \
    man/sasl_client_new.3 \
    man/sasl_client_start.3 \
    man/sasl_client_step.3 \
    man/sasl_decode.3 \
    man/sasl_dispose.3 \
    man/sasl_done.3 \
    man/sasl_encode.3 \
    man/sasl_encodev.3 \
    man/sasl_errdetail.3 \
    man/sasl_errors.3 \
    man/sasl_errstring.3 \
    man/sasl_getconfpath_t.3 \
    man/sasl_getopt_t.3 \
    man/sasl_getpath_t.3 \
    man/sasl_getprop.3 \
    man/sasl_getrealm_t.3 \
    man/sasl_getsecret_t.3 \
    man/sasl_getsimple_t.3 \
    man/sasl_global_listmech.3 \
    man/sasl_idle.3 \
    man/sasl_listmech.3 \
    man/sasl_log_t.3 \
    man/sasl_server_init.3 \
    man/sasl_server_new.3 \
    man/sasl_server_start.3 \
    man/sasl_server_step.3 \
    man/sasl_server_userdb_checkpass_t.3 \
    man/sasl_server_userdb_setpass_t.3 \
    man/sasl_setpass.3 \
    man/sasl_setprop.3 \
    man/sasl_user_exists.3 \
    man/sasl_verifyfile_t.3

%.1: man/.sphinx-build.stamp
	@$(MKDIR_P) $(@D)
	$(AM_V_GEN)touch $@

%.3: man/.sphinx-build.stamp
	@$(MKDIR_P) $(@D)
	$(AM_V_GEN)touch $@

%.5: man/.sphinx-build.stamp
	@$(MKDIR_P) $(@D)
	$(AM_V_GEN)touch $@

%.8: man/.sphinx-build.stamp
	@$(MKDIR_P) $(@D)
	$(AM_V_GEN)touch $@

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libsasl2.pc

## define this unconditionally because dist-hook references it
SPHINX_CACHE = docsrc/.doctrees

dist-hook:
	@find $(distdir) -exec chmod u+w {} ';'
	@find $(distdir) -name CVS -print | xargs -t rm -rf
	(cd $(distdir)/plugins && sh makeinit.sh)
	find $(top_distdir) -type f -name .sphinx-build.stamp -delete
	rm -rf $(top_distdir)/$(SPHINX_CACHE)

framedir = /Library/Frameworks/SASL2.framework
install-exec-local-osx:
	$(mkinstalldirs) $(framedir)/Versions/A/Headers
	$(mkinstalldirs) $(framedir)/Versions/A/Resources
	cd $(framedir)/Versions ; ln -fs A Current
	cd $(framedir) ; ln -fs Versions/A/Headers .
	cd $(framedir) ; ln -fs Versions/A/Resources .
	$(INSTALL_DATA) $(srcdir)/config/Info.plist $(framedir)/Versions/A/Resources

## documentation generation
.PHONY: clean-sphinx-cache clean-docsrc
clean-sphinx-cache:
	@$(RM) -r $(SPHINX_CACHE)

if HAVE_SPHINX_BUILD
GENERATED_RSTS =

SPHINX_OPTS = -d $(SPHINX_CACHE) -n -q

## detect when source directory is not build directory (i.e. VPATH
## build), and clone the docsrc tree into the build directory, so
## that we have a single "source directory" for sphinx-build to use.
docsrc/.sphinx-build.stamp:
	$(AM_V_GEN)test x"$(top_srcdir)" = x"$(top_builddir)" || \
        (cd $(top_srcdir) && tar cf - --mode=gu+w docsrc) | tar xf -
	@touch $@

clean-docsrc:
	test x"$(top_srcdir)" = x"$(top_builddir)" || \
		$(RM) -r $(top_builddir)/docsrc

## XXX doesn't detect if other rst sources are updated...
man/.sphinx-build.stamp: $(GENERATED_RSTS) docsrc/.sphinx-build.stamp
	$(AM_V_GEN)$(SPHINX_BUILD) $(SPHINX_OPTS) -b cyrman $(top_builddir)/docsrc $(top_builddir)/man
	@touch $@

clean-man:
	@$(RM) man/*.[1358]
# don't delete entire man/ directory while we still have man/legacy/
#	@$(RM) -r man

doc/html/.sphinx-build.stamp: $(GENERATED_RSTS) docsrc/.sphinx-build.stamp
	$(AM_V_GEN)$(SPHINX_BUILD) $(SPHINX_OPTS) -b html $(top_builddir)/docsrc $(top_builddir)/doc/html
	@touch $@

else
clean-docsrc:

man/.sphinx-build.stamp:
	@$(MKDIR_P) $(@D)
	$(AM_V_GEN)touch $@
	@echo "warning: missing documentation dependencies. man pages will be empty" 1>&2

clean-man:

doc/html/.sphinx-build.stamp:
	@echo "error: missing documentation dependencies. cannot build html documentation" 1>&2
	@false
endif

.PHONY: man man-force clean-man
man-force:
	## trash the timestamps, so we always re-run sphinx-build.
	## sphinx-build tracks source modifications itself, so forced
	## rebuilds will still be a lot quicker once it has run once.
	@$(RM) docsrc/.sphinx-build.stamp man/.sphinx-build.stamp

man: man-force $(dist_man3_MANS)

# clean-man is conditionally defined above. We do not want to remove
# man pages if the user is unable to rebuild them.

.PHONY: doc-html doc-html-force clean-doc-html
doc-html-force:
	@$(RM) docsrc/.sphinx-build.stamp doc/html/.sphinx-build.stamp

doc-html: doc-html-force doc/html/.sphinx-build.stamp

clean-doc-html:
	@$(RM) -r doc/html

.PHONY: doc clean-doc
doc: doc-html man

clean-doc: clean-doc-html clean-man

.PHONY: clean-local
clean-local: clean-doc clean-sphinx-cache clean-docsrc
