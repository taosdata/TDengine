
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# Required compiler parameters
CFLAGS += -I..

LDFLAGS += -L..
LDLIBS := -lmseed $(LDLIBS)

# Build all *.c source as independent programs
SRCS := $(sort $(wildcard *.c))
BINS := $(SRCS:%.c=%)

all: $(BINS)

# Build programs and check for executable
$(BINS) : % : %.c
	@printf 'Building $<\n';
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS) 

clean:
	@rm -rf *.o $(BINS) *.dSYM

install:
	@echo
	@echo "No install target, copy the executable(s) as needed"
	@echo
