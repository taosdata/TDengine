# Makefile for building TDengine examples on TD Linux platform

INCLUDE_DIRS = 

TARGETS = connect_example       \
          create_db_demo        \
          insert_data_demo      \
          query_data_demo       \
          with_reqid_demo       \
          sml_insert_demo       \
          stmt_insert_demo      \
          tmq_demo

SOURCES = connect_example.c     \
          create_db_demo.c      \
          insert_data_demo.c    \
          query_data_demo.c     \
          with_reqid_demo.c     \
          sml_insert_demo.c     \
          stmt_insert_demo.c    \
          tmq_demo.c

LIBS = -ltaos -lpthread


CFLAGS = -g

all: $(TARGETS)

$(TARGETS):
	$(CC) $(CFLAGS) -o $@ $(wildcard $(@F).c) $(LIBS)

clean:
	rm -f $(TARGETS)