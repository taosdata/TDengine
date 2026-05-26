set +e

echo "compile udf bit_and and l2norm"

gcc -fPIC -shared c-udf/bit_and.c -o c-udf/libbitand.so

gcc -fPIC -shared c-udf/l2norm.c -o c-udf/libl2norm.so

