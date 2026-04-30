set +e

rm -rf /tmp/udf/libbitand.so /tmp/udf/libl2norm.so /tmp/udf/libgpd.so /tmp/udf/libperm_entropy.so
mkdir -p /tmp/udf
echo "compile udf bit_and and sqr_sum"
gcc -fPIC -shared cases/12-UDFs/sh/bit_and.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libbitand.so
gcc -fPIC -shared cases/12-UDFs/sh/l2norm.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libl2norm.so
gcc -fPIC -shared cases/12-UDFs/sh/gpd.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libgpd.so
# perm_entropy: aggregate UDF (accumulate-all-data-then-compute pattern)
gcc -fPIC -shared docs/examples/udf/perm_entropy.c -I../../include/libs/function/ -I../../include/client -I../../include/util -lm -o /tmp/udf/libperm_entropy.so
echo "debug show /tmp/udf/*.so"
ls /tmp/udf/*.so

