---
title: TDinsight - Grafana-based Zero-Dependency Monitoring Solution for TDengine
sidebar_label: TDinsight
---

TDinsight is a solution for monitoring TDengine using the builtin native monitoring database and [Grafana].

After TDengine starts, it will automatically create a monitoring database `log`. TDengine will automatically write many metrics in specific intervals into the `log` database. The metrics may include the server's CPU, memory, hard disk space, network bandwidth, number of requests, disk read/write speed, slow queries, other information like important system operations (user login, database creation, database deletion, etc.), and error alarms. With [Grafana] and [TDengine Data Source Plugin](https://github.com/taosdata/grafanaplugin/releases), TDinsight can visualize cluster status, node information, insertion and query requests, resource usage, etc., and also vnode, dnode, and mnode status, and exception alerts. Developers monitoring TDengine cluster operation status in real-time can be very convinient. This article will guide users to install the Grafana server, automatically install the TDengine data source plug-in, and deploy the TDinsight visualization panel through `TDinsight.sh` installation script.

## System Requirements

To deploy TDinsight, a single-node TDengine server or a multi-nodes TDengine cluster and a [Grafana] server are required. This dashboard requires TDengine 2.3.3.0 and above, with the `log` database enabled (`monitor = 1`).

## Installing Grafana

We recommend using the latest [Grafana] version 7 or 8 here. You can install Grafana on any [supported operating system](https://grafana.com/docs/grafana/latest/installation/requirements/#supported-operating-systems) by following the [official Grafana documentation Instructions](https://grafana.com/docs/grafana/latest/installation/) to install [Grafana].

### Installing Grafana on Debian or Ubuntu

For Debian or Ubuntu operating systems, we recommend the Grafana image repository and Use the following command to install from scratch.

```bash
sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key |\
  sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" |\
  sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana
```

### Install Grafana on CentOS / RHEL

You can install it from its official YUM repository.

```bash
sudo tee /etc/yum.repos.d/grafana.repo << EOF
[grafana]
name=grafana
baseurl=https://packages.grafana.com/oss/rpm
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://packages.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF
sudo yum install grafana
```

Or install it with RPM package.

```bash
wget https://dl.grafana.com/oss/release/grafana-7.5.11-1.x86_64.rpm
sudo yum install grafana-7.5.11-1.x86_64.rpm
# or
sudo yum install \
  https://dl.grafana.com/oss/release/grafana-7.5.11-1.x86_64.rpm
```

## Automated deployment of TDinsight

We provide an installation script [`TDinsight.sh`](https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh) script to allow users to configure the installation automatically and quickly.

You can download the script via `wget` or other tools:

```bash
wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh
chmod +x TDinsight.sh
./TDinsight.sh
```

This script will automatically download the latest [Grafana TDengine data source plugin](https://github.com/taosdata/grafanaplugin/releases/latest) and [TDinsight dashboard](https://grafana.com/grafana/dashboards/15167) with configurable parameters from the command-line options to the [Grafana Provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) configuration file to automate deployment and updates, etc. With the alert setting options provided by this script, you can also get built-in support for AliCloud SMS alert notifications.

Assume you use TDengine and Grafana's default services on the same host. Run `. /TDinsight.sh` and open the Grafana browser window to see the TDinsight dashboard.

The following is a description of TDinsight.sh usage.

```text
Usage:
   ./TDinsight.sh
   ./TDinsight.sh -h|--help
   ./TDinsight.sh -n <ds-name> -a <api-url> -u <user> -p <password>

Install and configure TDinsight dashboard in Grafana on Ubuntu 18.04/20.04 system.

-h, -help,          --help                  Display help

-V, -verbose,       --verbose               Run script in verbose mode. Will print out each step of execution.

-v, --plugin-version <version>              TDengine datasource plugin version, [default: latest]

-P, --grafana-provisioning-dir <dir>        Grafana provisioning directory, [default: /etc/grafana/provisioning/]
-G, --grafana-plugins-dir <dir>             Grafana plugins directory, [default: /var/lib/grafana/plugins]
-O, --grafana-org-id <number>               Grafana organization id. [default: 1]

-n, --tdengine-ds-name <string>             TDengine datasource name, no space. [default: TDengine]
-a, --tdengine-api <url>                    TDengine REST API endpoint. [default: http://127.0.0.1:6041]
-u, --tdengine-user <string>                TDengine user name. [default: root]
-p, --tdengine-password <string>            TDengine password. [default: taosdata]

-i, --tdinsight-uid <string>                Replace with a non-space ASCII code as the dashboard id. [default: tdinsight]
-t, --tdinsight-title <string>              Dashboard title. [default: TDinsight]
-e, --tdinsight-editable                    If the provisioning dashboard could be editable. [default: false]

-E, --external-notifier <string>            Apply external notifier uid to TDinsight dashboard.

Alibaba Cloud SMS as Notifier:
-s, --sms-enabled                           To enable tdengine-datasource plugin builtin Alibaba Cloud SMS webhook.
-N, --sms-notifier-name <string>            Provisioning notifier name.[default: TDinsight Builtin SMS]
-U, --sms-notifier-uid <string>             Provisioning notifier uid, use lowercase notifier name by default.
-D, --sms-notifier-is-default               Set notifier as default.
-I, --sms-access-key-id <string>            Alibaba Cloud SMS access key id
-K, --sms-access-key-secret <string>        Alibaba Cloud SMS access key secret
-S, --sms-sign-name <string>                Sign name
-C, --sms-template-code <string>            Template code
-T, --sms-template-param <string>           Template param, a escaped JSON string like '{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}'
-B, --sms-phone-numbers <string>            Comma-separated numbers list, eg "189xxxxxxxx,132xxxxxxxx"
-L, --sms-listen-addr <string>              [default: 127.0.0.1:9100]
```

Most command-line options can take effect the same as environment variables.

| Short Options | Long Options | Environment Variables | Description |
| ------ | -------------------------- | ---------------------------- | ------------------------------------------------------------------ --------- |
| -v | --plugin-version | TDENGINE_PLUGIN_VERSION | The TDengine data source plugin version, the latest version is used by default.                                   | -P
| -P | --grafana-provisioning-dir | GF_PROVISIONING_DIR | The Grafana configuration directory, defaults to `/etc/grafana/provisioning/` |
| -G | --grafana-plugins-dir | GF_PLUGINS_DIR | The Grafana plugin directory, defaults to `/var/lib/grafana/plugins`.                        | -O
| -O | --grafana-org-id | GF_ORG_ID | The Grafana organization ID, default is 1. |
| -n | --tdengine-ds-name | TDENGINE_DS_NAME | The name of the TDengine data source, defaults to TDengine. | -a | --tdengine-ds-name | The name of the TDengine data source, defaults to TDengine.
| -a | --tdengine-api | TDENGINE_API | The TDengine REST API endpoint. Defaults to `http://127.0.0.1:6041`.                     | -u
| -u | --tdengine-user | TDENGINE_USER | TDengine username. [default: root] |
| -p | --tdengine-password | TDENGINE_PASSWORD | TDengine password. [default: tadosdata] | -i | --tdengine-password
| -i | --tdinsight-uid | TDINSIGHT_DASHBOARD_UID | TDinsight `uid` of the dashboard. [default: tdinsight] |
| -t | --tdinsight-title | TDINSIGHT_DASHBOARD_TITLE | TDinsight dashboard title. [Default: TDinsight] | -e | -tdinsight-title
| -e | --tdinsight-editable | TDINSIGHT_DASHBOARD_EDITABLE | If the dashboard is configured to be editable. [Default: false] | -e | --external
| -E | --external-notifier | EXTERNAL_NOTIFIER | Apply the external notifier uid to the TDinsight dashboard.                                | -s
| -s | --sms-enabled | SMS_ENABLED | Enable the tdengine-datasource plugin built into Alibaba Cloud SMS webhook.                    | -s
| -N | --sms-notifier-name | SMS_NOTIFIER_NAME | The name of the provisioning notifier. [Default: `TDinsight Builtin SMS`] | -U
| -U | --sms-notifier-uid | SMS_NOTIFIER_UID | "Notification Channel" `uid`, lowercase of the program name is used by default, other characters are replaced by "-". |-sms
| -D | --sms-notifier-is-default | SMS_NOTIFIER_IS_DEFAULT | Set built-in SMS notification to default value.                                                |-sms-notifier-is-default
| -I | --sms-access-key-id | SMS_ACCESS_KEY_ID | Alibaba Cloud SMS access key id |
| -K | --sms-access-key-secret | SMS_ACCESS_KEY_SECRET | AliCloud SMS-access-secret-key |
| -S | --sms-sign-name | SMS_SIGN_NAME | Signature |
| -C | --sms-template-code | SMS_TEMPLATE_CODE | Template code |
| -T | --sms-template-param | SMS_TEMPLATE_PARAM | JSON template for template parameters |
| -B | --sms-phone-numbers | SMS_PHONE_NUMBERS | A comma-separated list of phone numbers, e.g. `"189xxxxxxxx,132xxxxxxxx"` |
| -L | --sms-listen-addr | SMS_LISTEN_ADDR | Built-in SMS webhook listener address, default is `127.0.0.1:9100` |

Suppose you start a TDengine database on host `tdengine` with HTTP API port `6041`, user `root1`, and password `pass5ord`. Execute the script.

```bash
sudo . /TDinsight.sh -a http://tdengine:6041 -u root1 -p pass5ord
```

We provide a "-E" option to configure TDinsight to use the existing Notification Channel from the command line. Assuming your Grafana user and password is `admin:admin`, use the following command to get the `uid` of an existing notification channel.

```bash
curl --no-progress-meter -u admin:admin http://localhost:3000/api/alert-notifications | jq
```

Use the `uid` value obtained above as `-E` input.

```bash
sudo ./TDinsight.sh -a http://tdengine:6041 -u root1 -p pass5ord -E existing-notifier
```

If you want to use the [Alibaba Cloud SMS](https://www.aliyun.com/product/sms) service as a notification channel, you should enable it with the `-s` flag add the following parameters.

- `-N`: Notification Channel name, default is `TDinsight Builtin SMS`.
- `-U`: Channel uid, default is lowercase of `name`, any other character is replaced with -, for the default `-N`, its uid is `tdinsight-builtin-sms`.
- `-I`: Alibaba Cloud SMS access key id.
- `-K`: Alibaba Cloud SMS access secret key.
- `-S`: Alibaba Cloud SMS signature.
- `-C`: Alibaba Cloud SMS template id.
- `-T`: Alibaba Cloud SMS template parameters, for JSON format template, example is as follows `'{"alarm_level":"%s", "time":"%s", "name":"%s", "content":"%s"}'`. There are four parameters: alarm level, time, name and alarm content.
- `-B`: a list of phone numbers, separated by a comma `,`.

If you want to monitor multiple TDengine clusters, you need to set up numerous TDinsight dashboards. Setting up non-default TDinsight requires some changes: the `-n` `-i` `-t` options need to be changed to non-default names, and `-N` and `-L` should also be changed if using the built-in SMS alerting feature.

```bash
sudo . /TDengine.sh -n TDengine-Env1 -a http://another:6041 -u root -p taosdata -i tdinsight-env1 -t 'TDinsight Env1'
# If using built-in SMS notifications
sudo . /TDengine.sh -n TDengine-Env1 -a http://another:6041 -u root -p taosdata -i tdinsight-env1 -t 'TDinsight Env1' \
  -s -N 'Env1 SMS' -I xx -K xx -S xx -C SMS_XX -T '' -B 00000000000 -L 127.0.0.01:10611
```

Please note that the configuration data source, notification channel, and dashboard are not changeable on the front end. You should update the configuration again via this script or manually change the configuration file in the `/etc/grafana/provisioning` directory (this is the default directory for Grafana, use the `-P` option to change it as needed).

Specifically, `-O` can be used to set the organization ID when you are using Grafana Cloud or another organization. `-G` specifies the Grafana plugin installation directory. The `-e` parameter sets the dashboard to be editable.

## Set up TDinsight manually

### Install the TDengine data source plugin

Install the latest version of the TDengine Data Source plugin from GitHub.

```bash
get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"v([^"]+)".*/\1/'
}
TDENGINE_PLUGIN_VERSION=$(get_latest_release)
sudo grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
  plugins install tdengine-datasource
```

:::note
The 3.1.6 and earlier version plugins require the following setting in the configuration file `/etc/grafana/grafana.ini` to enable unsigned plugins.

```ini
[plugins]
allow_loading_unsigned_plugins = tdengine-datasource
```
:::

### Start the Grafana service

```bash
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

### Logging into Grafana

Open the default Grafana URL in a web browser: ``http://localhost:3000``.
The default username/password is `admin`. Grafana will require a password change after the first login.

### Adding a TDengine Data Source

Point to the **Configurations** -> **Data Sources** menu, and click the **Add data source** button.

![TDengine Database TDinsight Add data source button](./assets/howto-add-datasource-button.webp)

Search for and select **TDengine**.

![TDengine Database TDinsight Add datasource](./assets/howto-add-datasource-tdengine.webp)

Configure the TDengine datasource.

![TDengine Database TDinsight Datasource Configuration](./assets/howto-add-datasource.webp)

Save and test. It will report 'TDengine Data source is working' under normal circumstances.

![TDengine Database TDinsight datasource test](./assets/howto-add-datasource-test.webp)

### Importing dashboards

Point to **+** / **Create** - **import** (or `/dashboard/import` url).

![TDengine Database TDinsight Import Dashboard and Configuration](./assets/import_dashboard.webp)

Type the dashboard ID `15167` in the **Import via grafana.com** location and **Load**.

![TDengine Database TDinsight Import via grafana.com](./assets/import-dashboard-15167.webp)

Once the import is complete, the full page view of TDinsight is shown below.

![TDengine Database TDinsight show](./assets/TDinsight-full.webp)

## TDinsight dashboard details

The TDinsight dashboard is designed to provide the usage and status of TDengine-related resources [dnodes, mnodes, vnodes](https://www.taosdata.com/cn/documentation/architecture#cluster) or databases.

Details of the metrics are as follows.

### Cluster Status

![TDengine Database TDinsight mnodes overview](./assets/TDinsight-1-cluster-status.webp)

This section contains the current information and status of the cluster, the alert information is also here (from left to right, top to bottom).

- **First EP**: the `firstEp` setting in the current TDengine cluster.
- **Version**: TDengine server version (master mnode).
- **Master Uptime**: The time elapsed since the current Master MNode was elected as Master.
- **Expire Time** - Enterprise version expiration time.
- **Used Measuring Points** - The number of measuring points used by the Enterprise Edition.
- **Databases** - The number of databases.
- **Connections** - The number of current connections.
- **DNodes/MNodes/VGroups/VNodes** - Total number of each resource and the number of survivors.
- **DNodes/MNodes/VGroups/VNodes Alive Percent**: The ratio of the number of alive/total for each resource, enabling the alert rule and triggering it when the resource liveness rate (the average percentage of healthy resources in 1 minute) is less than 100%.
- **Measuring Points Used**: The number of measuring points used to enable the alert rule (no data available in the community version, healthy by default).
- **Grants Expire Time**: the expiration time of the enterprise version of the enabled alert rule (no data available for the community version, healthy by default).
- **Error Rate**: Aggregate error rate (average number of errors per second) for alert-enabled clusters.
- **Variables**: `show variables` table display.

### DNodes Status

![TDengine Database TDinsight mnodes overview](./assets/TDinsight-2-dnodes.webp)

- **DNodes Status**: simple table view of `show dnodes`.
- **DNodes Lifetime**: the time elapsed since the dnode was created.
- **DNodes Number**: the number of DNodes changes.
- **Offline Reason**: if any dnode status is offline, the reason for offline is shown as a pie chart.

### MNode Overview

![TDengine Database TDinsight mnodes overview](./assets/TDinsight-3-mnodes.webp)

1. **MNodes Status**: a simple table view of `show mnodes`. 2.
2. **MNodes Number**: similar to `DNodes Number`, the number of MNodes changes.

### Request

![TDengine Database TDinsight tdinsight requests](./assets/TDinsight-4-requests.webp)

1. **Requests Rate(Inserts per Second)**: average number of inserts per second.
2. **Requests (Selects)**: number of query requests and change rate (count of second).
3. **Requests (HTTP)**: number of HTTP requests and request rate (count of second).

### Database

![TDengine Database TDinsight database](./assets/TDinsight-5-database.webp)

Database usage, repeated for each value of the variable `$database` i.e. multiple rows per database.

1. **STables**: number of super tables. 2.
2. **Total Tables**: number of all tables. 3.
3. **Sub Tables**: the number of all super table sub-tables. 4.
4. **Tables**: graph of all normal table numbers over time.
5. **Tables Number Foreach VGroups**: The number of tables contained in each VGroups.

### DNode Resource Usage

![TDengine Database TDinsight dnode usage](./assets/TDinsight-6-dnode-usage.webp)

Data node resource usage display with repeated multiple rows for the variable `$fqdn` i.e., each data node. Includes.

1. **Uptime**: the time elapsed since the dnode was created.
2. **Has MNodes?**: whether the current dnode is a mnode. 3.
3. **CPU Cores**: the number of CPU cores. 4.
4. **VNodes Number**: the number of VNodes in the current dnode. 5.
5. **VNodes Masters**: the number of vnodes in the master role. 6.
6. **Current CPU Usage of taosd**: CPU usage rate of taosd processes.
7. **Current Memory Usage of taosd**: memory usage of taosd processes.
8. **Disk Used**: The total disk usage percentage of the taosd data directory.
9. **CPU Usage**: Process and system CPU usage. 10.
10. **RAM Usage**: Time series view of RAM usage metrics.
11. **Disk Used**: Disks used at each level of multi-level storage (default is level0).
12. **Disk Increasing Rate per Minute**: Percentage increase or decrease in disk usage per minute.
13. **Disk IO**: Disk IO rate. 14.
14. **Net IO**: Network IO, the aggregate network IO rate in addition to the local network.

### Login History

![TDengine Database TDinsight Login History](./assets/TDinsight-7-login-history.webp)

Currently, only the number of logins per minute is reported.

### Monitoring taosAdapter

![TDengine Database TDinsight monitor taosadapter](./assets/TDinsight-8-taosadapter.webp)

Support monitoring taosAdapter request statistics and status details. Includes.

1. **http_request**: contains the total number of requests, the number of failed requests, and the number of requests being processed
2. **top 3 request endpoint**: data of the top 3 requests by endpoint group
3. **Memory Used**: taosAdapter memory usage
4. **latency_quantile(ms)**: quantile of (1, 2, 5, 9, 99) stages
5. **top 3 failed request endpoint**: data of the top 3 failed requests by endpoint grouping
6. **CPU Used**: taosAdapter CPU usage

## Upgrade

TDinsight installed via the `TDinsight.sh` script can be upgraded to the latest Grafana plugin and TDinsight Dashboard by re-running the script.

In the case of a manual installation, follow the steps above to install the new Grafana plugin and Dashboard yourself.

## Uninstall

TDinsight installed via the `TDinsight.sh` script can be cleaned up using the command line `TDinsight.sh -R` to clean up the associated resources.

To completely uninstall TDinsight during a manual installation, you need to clean up the following.

1. the TDinsight Dashboard in Grafana.
2. the Data Source in Grafana. 3.
3. remove the `tdengine-datasource` plugin from the plugin installation directory.

## Integrated Docker Example

```bash
git clone --depth 1 https://github.com/taosdata/grafanaplugin.git
cd grafanaplugin
```

Change as needed in the ``docker-compose.yml`` file to

```yaml
version: '3.7'

services:
  grafana:
    image: grafana/grafana:7.5.10
    volumes:
      - . /dist:/var/lib/grafana/plugins/tdengine-datasource
      - . /grafana/grafana.ini:/etc/grafana/grafana.ini
      - . /grafana/provisioning/:/etc/grafana/provisioning/
      - grafana-data:/var/lib/grafana
    environment:
      TDENGINE_API: ${TDENGINE_API}
      TDENGINE_USER: ${TDENGINE_USER}
      TDENGINE_PASS: ${TDENGINE_PASS}
      SMS_ACCESS_KEY_ID: ${SMS_ACCESS_KEY_ID}
      SMS_ACCESS_KEY_SECRET: ${SMS_ACCESS_KEY_SECRET}
      SMS_SIGN_NAME: ${SMS_SIGN_NAME}
      SMS_TEMPLATE_CODE: ${SMS_TEMPLATE_CODE}
      SMS_TEMPLATE_PARAM: '${SMS_TEMPLATE_PARAM}'
      SMS_PHONE_NUMBERS: $SMS_PHONE_NUMBERS
      SMS_LISTEN_ADDR: ${SMS_LISTEN_ADDR}
    ports:
      - 3000:3000
volumes:
  grafana-data:
```

Replace the environment variables in `docker-compose.yml` or save the environment variables to the `.env` file, then start Grafana with `docker-compose up`. See [Docker Compose Reference](https://docs.docker.com/compose/)

```bash
docker-compose up -d
```

Then the TDinsight was deployed via Provisioning. Go to http://localhost:3000/d/tdinsight/ to view the dashboard.

[grafana]: https://grafana.com
[tdengine]: https://tdengine.com
