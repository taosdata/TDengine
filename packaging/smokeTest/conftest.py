# conftest.py
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--verMode", default="enterprise", help="community or enterprise"
    )
    parser.addoption(
        "--tVersion", default="3.3.2.6", help="the version of taos"
    )
    parser.addoption(
        "--baseVersion", default="smoking", help="the path of nas"
    )
    parser.addoption(
        "--sourcePath", default="nas", help="only support nas currently"
    )




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
                html_content = '''
                <tr>
                    <td><b><span style="font-size: larger; color: black;">Setup:</span></b></td>
                    <td colspan="4">
                        <a href="#" id="toggleSetup">Show Setup</a>
                        <div id="setupContent" class="collapsible-content" style="display: none; white-space: pre-wrap; margin-top: 5px;">
                            <pre>{}</pre>
                        </div>
                    </td>
                </tr>
                '''.format(stdout.strip())

                # 如果需要在 Python 脚本中生成 HTML，并使用 JavaScript 控制折叠内容的显示，可以这样做：

                html_script = '''
                <script>
                document.getElementById('toggleSetup').addEventListener('click', function(event) {
                    event.preventDefault();
                    var setupContentDiv = document.getElementById('setupContent');
                    setupContentDiv.style.display = setupContentDiv.style.display === 'none' ? 'block' : 'none';
                    var buttonText = setupContentDiv.style.display === 'none' ? 'Show Setup' : 'Hide Setup';
                    this.textContent = buttonText;
                });
                </script>
                '''

                # 输出完整的 HTML 代码
                final_html = html_content + html_script
                rows.append(final_html)
                rows.append("<br>")
        # Insert teardown stdout
        if teardown_stdout_info:
            for nodeid, stdout in teardown_stdout_info.items():
                html_content = '''
                    <tr>
                        <td><b><span style="font-size: larger; color: black;">Teardown:</span></b></td>
                        <td colspan="4">
                            <a href="#" id="toggleTeardown">Show Teardown</a>
                            <div id="teardownContent" class="collapsible-content" style="display: none; white-space: pre-wrap; margin-top: 5px;">
                                <pre>{}</pre>
                            </div>
                        </td>
                    </tr>
                    '''.format(stdout.strip())

                # 如果需要在 Python 脚本中生成 HTML，并使用 JavaScript 控制折叠内容的显示，可以这样做：

                html_script = '''
                    <script>
                    document.getElementById('toggleTeardown').addEventListener('click', function(event) {
                        event.preventDefault();
                        var teardownContentDiv = document.getElementById('teardownContent');
                        teardownContentDiv.style.display = teardownContentDiv.style.display === 'none' ? 'block' : 'none';
                        var buttonText = teardownContentDiv.style.display === 'none' ? 'Show Teardown' : 'Hide Teardown';
                        this.textContent = buttonText;
                    });
                    </script>
                    '''

                # 输出完整的 HTML 代码
                final_html = html_content + html_script
                rows.append(final_html)

        prefix.extend(rows)
