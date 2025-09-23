# Makefile for building TDengine examples on Linux

TARGETS = connect_example       \
          create_db_demo        \
          insert_data_demo      \
          query_data_demo       \
          with_reqid_demo       \
          sml_insert_demo       \
          stmt_insert_demo      \
          stmt2_insert_demo     \
          tmq_demo

LIBS = -ltaos -lpthread

CFLAGS = -g

all: $(TARGETS)

$(TARGETS):
	$(CC) $(CFLAGS) -o $@ $(wildcard $(@F).c) $(LIBS)

clean:
	rm -f $(TARGETS)
