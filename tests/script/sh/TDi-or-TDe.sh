
tests_dir=`pwd`
IN_TDINTERNAL="community"
if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
    echo 1
else
    echo 0
fi