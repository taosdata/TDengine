set +e

rm -rf /tmp/udf/libbitand.so /tmp/udf/libsqrsum.so
mkdir -p /tmp/udf
echo "compile udf bit_and and sqr_sum"
gcc -fPIC -shared sh/bit_and.c -o /tmp/udf/libbitand.so
gcc -fPIC -shared sh/sqr_sum.c -o /tmp/udf/libsqrsum.so
echo "debug show /tmp/udf/*.so"
ls /tmp/udf/*.so

