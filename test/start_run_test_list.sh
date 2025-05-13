if [ $# -ne 1 ]; then
    echo "Usage: $0 <test_case_list_file>"
    exit 1
fi

test_case_list=$1

nohup ./run_tests.sh $test_case_list > /dev/null 2>&1 &