import os
import subprocess
import time

def get_time_seconds():
    current_time = time.strftime("%H:%M:%S", time.localtime())
    hh, mm, ss = map(int, current_time.split(':'))
    return (hh * 60 + mm) * 60 + ss

def color_echo(color, message, time1, error_file=None):
    current_time = time.strftime("%H:%M:%S", time.localtime())
    time2 = get_time_seconds()
    inter_time = time2 - time1
    print(f"End at {current_time} , cast {inter_time}s")
    print(message)
    if error_file:
        with open(error_file, 'r') as file:
            print(file.read())

def check_skip_case(line):
    skip_case = False
    if line == "python3 ./test.py -f 1-insert/insertWithMoreVgroup.py":
        skip_case = False
    elif line == "python3 ./test.py -f 2-query/queryQnode.py":
        skip_case = False
    elif "-R" in line:
        skip_case = True
    return skip_case

def run_tests(case_file):
    exit_num = 0
    a = 0
    failed_tests = []

    with open(case_file, 'r') as file:
        for line in file:
            line = line.strip()
            if check_skip_case(line):
                continue

            if line.startswith("python3"):
                a += 1
                print(f"{a} Processing {line}")
                time1 = get_time_seconds()
                print(f"Start at {time.strftime('%H:%M:%S', time.localtime())}")

                result_file = f"result_{a}.txt"
                error_file = f"error_{a}.txt"
                with open(result_file, 'w') as result, open(error_file, 'w') as error:
                    process = subprocess.run(line.split(), stdout=result, stderr=error)
                    if process.returncode != 0:
                        color_echo("0c", "failed", time1, error_file)
                        exit_num = 8
                        failed_tests.append(line)
                    else:
                        color_echo("0a", "Success", time1)

    if failed_tests:
        with open("failed.txt", 'w') as file:
            for test in failed_tests:
                file.write(test + '\n')

    return exit_num

if __name__ == "__main__":
    case_file = "simpletest.bat"
    if len(os.sys.argv) > 1 and os.sys.argv[1] == "full":
        case_file = "fulltest.sh"
        if len(os.sys.argv) > 2:
            case_file = os.sys.argv[2]

    exit_code = run_tests(case_file)
    os.sys.exit(exit_code)