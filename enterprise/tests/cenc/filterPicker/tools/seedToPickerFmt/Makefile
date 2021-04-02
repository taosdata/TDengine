
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# Required compiler parameters
CFLAGS += -I/usr/local/include -g -Wall

LDFLAGS += -L/usr/local/lib
LDLIBS := -lmseed -ltaos -lcurl -lm $(LDLIBS)

# Build all *.c source as independent programs
SRCS := $(sort $(wildcard *.c))
BINS := $(SRCS:%.c=%)

all: $(BINS)

# Build programs and check for executable
$(BINS) : % : %.c
	@printf 'Building $<\r\n';
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS) 

clean:
	@rm -rf *.o $(BINS) *.dSYM

install:
	@echo
	@echo "No install target, copy the executable(s) as needed"
	@echo
