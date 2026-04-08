target := all
buildDir=../../../build/bin
dependents := $(buildDir)/testTBase\
   	$(buildDir)/insertDataForPeriod\
	$(buildDir)/queryDataConsecutively\
	$(buildDir)/test_many_tables

CFLAGS := -g
TAOSCFLAG :=-g -Wall -fPIC -Wno-char-subscripts -malign-double -malign-stringops -D_REENTRANT -DLINUX -std=gnu99 -I../../inc -D__LOCAL__

TAOSLFLAG := -L../../../build/lib -lpthread ../../../build/lib/libtaos.a -lm -lrt

$(target): $(dependents)


$(buildDir)/testTBase : testTBase.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(TAOSCFLAG) $(TAOSLFLAG)

$(buildDir)/insertDataForPeriod : insertDataForPeriod.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(TAOSCFLAG) $(TAOSLFLAG)

$(buildDir)/queryDataConsecutively : queryDataConsecutively.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(TAOSCFLAG) $(TAOSLFLAG)

$(buildDir)/test_many_tables : test_many_tables.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(TAOSCFLAG) $(TAOSLFLAG)

clean:
	rm -rf $(dependents)
