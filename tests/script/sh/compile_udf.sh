set +e

rm -rf /tmp/udf/libbitand.so /tmp/udf/libsqrsum.so
mkdir -p /tmp/udf
echo "compile udf bit_and and sqr_sum"
gcc -fPIC -shared sh/bit_and.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libbitand.so
gcc -fPIC -shared sh/l2norm.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libl2norm.so
echo "debug show /tmp/udf/*.so"
ls /tmp/udf/*.so

