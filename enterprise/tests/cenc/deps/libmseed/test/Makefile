# This Makefile requires GNU make, sometimes available as gmake.
#
# A simple test suite for libmseed.
# See README for description.
#
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# Required compiler parameters
CFLAGS += -I..

LDFLAGS += -L..
LDLIBS := -lmseed $(LDLIBS)

SRCS := $(sort $(wildcard *.c))
BINS := $(SRCS:%.c=%)

TESTS := $(sort $(wildcard *.test))
TESTOUTS := $(TESTS:%.test=%.test.out)

# ASCII color coding for test results, green for PASSED and red for FAILED
PASSED := \033[0;32mPASSED\033[0m
FAILED := \033[0;31mFAILED\033[0m

TESTCOUNT := 0

test all: $(BINS) $(TESTOUTS)
	@printf '%d tests conducted\n' $(TESTCOUNT)

# Build programs and check for executable
$(BINS) : % : %.c
	@$(eval TESTCOUNT=$(shell echo $$(($(TESTCOUNT)+1))))
	@$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS); exit 0;
	@if test -x $@; \
	  then printf '$(PASSED) Building $<\n'; \
	  else printf '$(FAILED) Building $<\n'; exit 1; \
        fi

# Run test scripts, create %.test.out files and compare to %.test.ref references
$(TESTOUTS) : %.test.out : %.test $(BINS) FORCE
	@$(eval TESTCOUNT=$(shell echo $$(($(TESTCOUNT)+1))))
	@$(shell ./$< > $@ 2>&1)
	@diff $<.ref $@ >/dev/null; \
          if [ $$? -eq 0 ]; \
            then printf '$(PASSED) Test $<\n'; \
            else printf '$(FAILED) Test $<, Compare $<.ref $@\n'; \
	    exit 0; \
          fi

clean:
	@rm -rf $(BINS) $(TESTOUTS) *.dSYM

# Any targets using this empty FORCE rule as a prerequisite will always run
FORCE:
