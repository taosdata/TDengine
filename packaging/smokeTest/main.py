import pytest

# python3 -m pytest -v -k linux --html=/var/www/html/report.html --json-report --json-report-file="report.json" --timeout=60

# pytest.main(["-s", "-v"])
import pytest

import subprocess


# define cmd function
def run_command(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, shell=True)

        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("Return Code:", result.returncode)

        return result
    except Exception as e:
        print(f"Error executing command '{command}': {str(e)}")
        return None


def main():
    pytest.main(['--html=report.html'])


if __name__ == '__main__':
    main()
