EXTRA_DIST = CHANGES LICENSE README.rst CMakeLists.txt cmake android examples scripts
SUBDIRS = doc src test

# "make distcheck" builds the dvi target, so use it to check that the
# documentation is built correctly.
dvi:
	$(MAKE) SPHINXOPTS_EXTRA=-W html

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = jansson.pc

TESTS = scripts/clang-format-check
