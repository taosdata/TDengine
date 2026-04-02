import pytest

# python3 -m pytest test_server.py -v --html=/var/www/html/report.html --json-report --json-report-file="/var/www/html/report.json" --timeout=60

# pytest.main(["-s", "-v"])
import pytest

import subprocess


# define cmd function




def main():
    pytest.main(['--html=report.html'])


if __name__ == '__main__':
    main()
