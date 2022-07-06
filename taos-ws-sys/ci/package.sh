#!/bin/sh
set -ex
bin=$(realpath $(dirname $0))
wkdir=$bin/../../target/libtaosws
[ -d "$wkdir" ] || mkdir -p $wkdir
cp -f $bin/../../target/release/taosws.h $bin/../../target/release/libtaosws.so $bin/../../target/release/libtaosws.a $wkdir/
cp -f $bin/../examples/Makefile $bin/../examples/show-databases.c $wkdir/
cd $wkdir/../
tar cavf libtaosws.tar.gz libtaosws/
