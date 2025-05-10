from datetime import date
from datetime import timedelta
import os
import json
import re
import requests
import subprocess
from dotenv import load_dotenv

# load .env
# You should have a .env file in the same directory as this script
# You can exec: cp .env.example .env
load_dotenv()

# define version
version = os.getenv("VERSION")
version_list = os.getenv("VERSION_LIST").split(",")
version_pattern_str = version.replace('.', r'\.').replace('*', r'\d+')
version_pattern = re.compile(rf'^{version_pattern_str}$')
version_stack_list = list()

# define ip

ip = os.getenv("EXCLUDE_IP")
server_ip = os.getenv("SERVER_IP")
http_serv_ip = os.getenv("HTTP_SERV_IP")
http_serv_port = os.getenv("HTTP_SERV_PORT")
owner = os.getenv("OWNER")

# feishu-msg url
feishu_msg_url = os.getenv("FEISHU_MSG_URL")

# get today
today = date.today()

# Define the file and parameters
path="/data/telemetry/crash-report/"
trace_report_path = path + "trace_report"
os.makedirs(path, exist_ok=True)
os.makedirs(trace_report_path, exist_ok=True)

assert_script_path = path + "filter_assert.sh"
nassert_script_path = path + "filter_nassert.sh"

# get files for the past 7 days
def get_files():
    files = ""
    for i in range(1,8):
        #print ((today - timedelta(days=i)).strftime("%Y%m%d"))
        files = files + path + (today - timedelta(days=i)).strftime("%Y%m%d") + ".txt "
    return files.strip().split(" ")

# Define the AWK script as a string with proper escaping
def get_res(file_path):
    # Execute the script
    command = ['bash', file_path, version, ip] + get_files()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    # Capture the output and errors
    output, errors = process.communicate()

    # Check for errors
    if process.returncode != 0:
        return errors
    else:
        return output.rstrip()

def get_sum(output):
    # Split the output into lines
    lines = output.strip().split('\n')

    # Initialize the sum
    total_sum = 0

    # Iterate over each line
    for line in lines:
        # Split each line by space to separate the columns
        parts = line.split()

        # The first part of the line is the number, convert it to integer
        if parts:  # Check if there are any elements in the parts list
            number = int(parts[0])
            total_sum += number

    return total_sum

def get_crash_count_by_version(version_list):
    file_path = " ".join(get_files())
    results = {}
    sum = 0
    for version in version_list:
        pattern = f'"version":"{version}"'
        command = f"grep '{pattern}' {file_path} | grep -v '{ip}' | wc -l"
        count = int(subprocess.getoutput(command))
        sum += count
        results[version] = count
    output = "version\tcrash_count\n" + "\n".join([f"v{version}\t{count}" for version, count in results.items()])
    return output, sum

def convert_html(data):
    # convert data to json
    start_time = get_files()[6].split("/")[-1].split(".")[0]
    end_time = get_files()[0].split("/")[-1].split(".")[0]
    html_report_file = f'{start_time}_{end_time}.html'
    json_data = json.dumps(data)

    # Create HTML content
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stack Trace Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f0f0f5;
        }}
        h1 {{
            color: #2c3e50;
            text-align: center;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        th {{
            background-color: #3498db;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #ecf0f1;
        }}
        tr:hover {{
            background-color: #d1e7fd;
        }}
        pre {{
            background-color: #f7f7f7;
            padding: 10px;
            border: 1px solid #ddd;
            overflow-x: auto;
            white-space: pre-wrap;
            border-radius: 5px;
        }}
    </style>
</head>
<body>
    <h1>Stack Trace Report From {start_time} To {end_time} </h1>

    <table>
        <thead>
            <tr>
                <th>Key Stack Info</th>
                <th>Versions</th>
                <th>Num Of Crashes</th>
                <th>Full Stack Info</th>
            </tr>
        </thead>
        <tbody id="report">
        </tbody>
    </table>

    <script>
        const data = {json_data};

        const reportBody = document.getElementById('report');
        data.forEach(entry => {{
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${{entry.key_stack_info}}</td>
                <td>${{entry.version_list.join('<br>')}}</td>
                <td>${{entry.count}}</td>
                <td><pre>${{entry.full_stack_info}}</pre></td>
            `;
            reportBody.appendChild(row);
        }});
    </script>
</body>
</html>
'''
    # Write the HTML content to a file

    with open(f'{trace_report_path}/{html_report_file}', 'w') as f:
        f.write(html_content)
    return html_report_file

def get_version_stack_list(res):
    for line in res.strip().split('\n'):
        version_list = list()
        version_stack_dict = dict()
        count = line.split()[0]
        key_stack_info = line.split()[1]
        for file in get_files():
            with open(file, 'r') as infile:
                for line in infile:
                    line = line.strip()
                    data = json.loads(line)
                    # print(line)
                    if ip not in line and version_pattern.search(data["version"]) and key_stack_info in line:
                        if data["version"] not in version_list:
                            version_list.append(data["version"])
                            full_stack_info = data["stackInfo"]
        version_stack_dict["key_stack_info"] = key_stack_info
        version_stack_dict["full_stack_info"] = full_stack_info
        version_stack_dict["version_list"] = version_list
        version_stack_dict["count"] = count
        # print(version_stack_dict)
        version_stack_list.append(version_stack_dict)
    return version_stack_list

# get msg info
def get_msg(text, title="Telemetry Statistics"):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [
                        [{
                            "tag": "text",
                            "text": text
                        }
                        ]]
                }
            }
        }
    }

# post msg
def send_msg(json):
    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(url=feishu_msg_url, headers=headers, json=json)
    inf = req.json()
    if "StatusCode" in inf and inf["StatusCode"] == 0:
        pass
    else:
        print(inf)


def format_results(results):
    # Split the results into lines
    lines = results.strip().split('\n')

    # Parse lines into a list of tuples (number, rest_of_line)
    parsed_lines = []
    for line in lines:
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            number = int(parts[0])  # Convert the number part to an integer
            parsed_lines.append((number, parts[1]))

    # Sort the parsed lines by the first element (number) in descending order
    parsed_lines.sort(reverse=True, key=lambda x: x[0])

    # Determine the maximum width of the first column for alignment
    # max_width = max(len(str(item[0])) for item in parsed_lines)
    if parsed_lines:
        max_width = max(len(str(item[0])) for item in parsed_lines)
    else:
        max_width = 0

    # Format each line to align the numbers and function names with indentation
    formatted_lines = []
    for number, text in parsed_lines:
        formatted_line = f"       {str(number).rjust(max_width)} {text}"
        formatted_lines.append(formatted_line)

    # Join the formatted lines into a single string
    return '\n'.join(formatted_lines)

# # send report to feishu
def send_report(res, sum, html_report_file):
    content = f'''
    version: v{version}
    from: {get_files()[6].split("/")[-1].split(".")[0]}
    to: {get_files()[0].split("/")[-1].split(".")[0]}
    ip: {server_ip}
    owner: {owner}
    result: \n{format_results(res)}\n
    total crashes: {sum}\n
    details: http://{http_serv_ip}:{http_serv_port}/{html_report_file}
    '''
    print(get_msg(content))
    send_msg(get_msg(content, "Telemetry Crash Info Statistics"))

def send_crash_count_info_report(res, sum):
    content = f'''
    from: {get_files()[6].split("/")[-1].split(".")[0]}
    to: {get_files()[0].split("/")[-1].split(".")[0]}
    ip: {server_ip}
    owner: {owner}
    result: \n{res}\n
    total crashes: {sum}
    '''
    print(get_msg(content))
    send_msg(get_msg(content, "Telemetry Crash Count Statistics"))

# for none-taosAssertDebug
nassert_res = get_res(nassert_script_path)
# print(nassert_res)

# for taosAssertDebug
assert_res = get_res(assert_script_path)
# print(assert_res)

# combine the results
res = nassert_res + assert_res

# get version stack list
version_stack_list = get_version_stack_list(res) if len(res) > 0 else list()

# convert to html
html_report_file = convert_html(version_stack_list)

# get sum
sum = get_sum(res)

# send report
send_report(res, sum, html_report_file)

crash_count_info, crash_count_sum = get_crash_count_by_version(version_list)
send_crash_count_info_report(crash_count_info, crash_count_sum)