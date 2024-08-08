# conftest.py
import pytest

# Collect the setup and teardown of each test case and their std information
setup_stdout_info = {}
teardown_stdout_info = {}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    # Record the std of setup and teardown
    if call.when == 'setup':
        for i in rep.sections:
            if i[0] == "Captured stdout setup":
                if not setup_stdout_info:
                    setup_stdout_info[item.nodeid] = i[1]
    elif call.when == 'teardown':
        for i in rep.sections:
            if i[0] == "Captured stdout teardown":
                teardown_stdout_info[item.nodeid] = i[1]


# Insert setup and teardown's std in the summary section
def pytest_html_results_summary(prefix, summary, postfix):
    if setup_stdout_info or teardown_stdout_info:
        rows = []

        # Insert setup stdout
        if setup_stdout_info:
            for nodeid, stdout in setup_stdout_info.items():
                rows.append(
                    '<tr><td><b><span style="font-size: larger; color: black;">Setup:</b></span></td><td colspan="4"><pre>{}</pre></td></tr>'.format(
                        stdout.strip()))

        # Insert teardown stdout
        if teardown_stdout_info:
            for nodeid, stdout in teardown_stdout_info.items():
                rows.append(
                    '<tr><td><b><span style="font-size: larger; color: black;">Teardown:</b></td><td colspan="4"><pre>{}</pre></td></tr>'.format(stdout.strip()))

        prefix.extend(rows)
