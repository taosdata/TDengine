set +e

# NOTE: debug for the moment
date
pwd
ls -l /tmp/udf/*.so
ls -l ../../debug/build/bin/taosudf
ls -l ../../../debug/build/bin/taosudf

rm -rf /tmp/udf/libbitand.so /tmp/udf/libsqrsum.so /tmp/udf/libgpd.so
mkdir -p /tmp/udf
echo "compile udf bit_and and sqr_sum"
gcc -fPIC -shared sh/bit_and.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libbitand.so
gcc -fPIC -shared sh/l2norm.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libl2norm.so
gcc -fPIC -shared sh/gpd.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libgpd.so
echo "debug show /tmp/udf/*.so"
ls /tmp/udf/*.so

# NOTE: debug for the moment

date
ls -l /tmp/udf/*.so
ls -l ../../debug/build/bin/taosudf
ls -l ../../../debug/build/bin/taosudf
ldd /tmp/udf/libbitand.so
ldd ../../debug/build/bin/taosudf
ldd ../../../debug/build/bin/taosudf

