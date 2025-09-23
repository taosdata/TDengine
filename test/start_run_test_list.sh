if [ $# -eq 1 ]; then
    test_case_list=$1
else
    test_case_list="test_list.txt"
fi
echo "Running command: nohup ./run_tests.sh $test_case_list > /dev/null 2>&1 &"
nohup ./run_tests.sh $test_case_list > /dev/null 2>&1 &
echo "Test has been started. See result:"
echo "        test_logs/case_result.txt   # case execute results"
echo "        test_logs/run_tests.log   # all cases outputs"
echo "        test_logs/xxx.log   # failed case outputs"
echo "        test_logs/allure-report   # allure report if allure command support"
echo "Usage: ./start_run_test_list.sh  # start test case list, use default file test_list.txt"
echo "      or ./start_run_test_list.sh path/to/case_list_file"
echo "       ./stop_run_test_list.sh # sttop test case list, will write stop.txt file to stop case run"
