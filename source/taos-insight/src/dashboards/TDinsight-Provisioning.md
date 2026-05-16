# TDinsight Provisioning

If you've Grafana server installed, you could follow the steps to setup TDinsight dashboard to monitor TDengine cluster.

tl;dr. Suppose you have a TDengine cluster, we use RESTful API url to access it, eg. `http://localhost:6041`, with default user `root` and password `taosdata`, and run grafana-server on Ubuntu 18.04/20.04 or CentOS 7.8/8.x, the following line will setup TDinsight dashboard for you without any options.

```bash
./TDinsight.sh
```

## Long Usage

```bash
Usage: ./TDinsight.sh

Install and configure TDinsight dashboard in Grafana on ubuntu 18.04/20.04 system.

-h, -help,          --help                  Display help

-V, -verbose,       --verbose               Run script in verbose mode. Will print out each step of execution.

-v, --plugin-version <version>              TDengine datasource plugin version, [default: latest]

-P, --grafana-provisioning-dir <dir>        Grafana provisioning directory, [default: /etc/grafana/provisioning/]
-G, --grafana-plugins-dir <dir>             Grafana plugins directory, [default: /var/lib/grafana/plugins]
-O, --grafana-org-id <number>               Grafana orgnization id. [default: 1]

-n, --tdengine-ds-name <string>             TDengine datasource name, no space. [default: TDengine]
-a, --tdengine-api <url>                    TDengine REST API endpoint. [default: http://127.0.0.1:6041]
-u, --tdengine-user <string>                TDengine user name. [default: root]
-p, --tdengine-password <string>            TDengine password. [default: taosdata]

-i, --tdinsight-uid <string>                Replace with a non-space ascii code as the dashboard id. [default: tdinsight]
-t, --tdinsight-title <string>              Dashboard title. [default: TDinsight]
-e, --tdinsight-editable                    If the provisioning dashboard could be editable. [default: false]

Aliyun SMS as Notifier:
-s, --sms-enabled                           To enable tdengine-datasource plugin builtin aliyun sms webhook
-N, --sms-notifier-name <string>            [default: sms]
-I, --sms-access-key-id <string>            Aliyun sms access key id
-K, --sms-access-key-secret <string>        Aliyun sms access key secret
-S, --sms-sign-name <string>                Sign name
-C, --sms-template-code <string>            Template code
-T, --sms-template-param <string>           Template param, a escaped json string like "\\\"a\\\":\\\"hi\\\""
-B, --sms-phone-numbers <string>            Comma-separated numbers list, eg "189xxxxxxxx,132xxxxxxxx"
-L, --sms-listen-addr <string>              [default: 127.0.0.1:9100]
```

## Use Environment Variables

| Options                    | Environment                            | Demo                                                                                                            |
| -------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| --plugin-version           | TDENGINE_PLUGIN_VERSION                | latest                                                                                                          |
| --grafana-provisioning-dir | GF_PROVISIONING_DIR                    | /etc/grafana/provisioning/                                                                                      |
| --grafana-plugins-dir      | GF_PLUGINS_DIR                         | /var/lib/grafana/plugins                                                                                        |
| --grafana-org-id           | GF_ORG_ID                              | 1                                                                                                               |
| --tdengine-ds-name         | TDENGINE_DS_NAME                       | TDengine                                                                                                        |
| --tdengine-api             | TDENGINE_API                           | http://localhost:6041                                                                                           |
| --tdengine-user            | TDENGINE_USER                          | root                                                                                                            |
| --tdengine-password        | TDENGINE_PASSWORD                      | taosdata                                                                                                        |
| --tdinsight-uid            | TDINSIGHT_DASHBOARD_UID                | tdinsight                                                                                                       |
| --tdinsight-title          | TDINSIGHT_DASHBOARD_TITLE              | TDinsight                                                                                                       |
| --tdinsight-editable       | TDINSIGHT_DASHBOARD_EDITABLE           | false                                                                                                           |
| --sms-enabled              | SMS_ENABLED                            | false                                                                                                           |
| --sms-notifier-name        | SMS_NOTIFIER_NAME                      | sms                                                                                                             |
| --sms-access-key-id        | SMS_ACCESS_KEY_ID=${SMS_ACCESS_KEY_ID} | LTAI4GHZmsHnaMSh6P6tE5i                                                                                         |
| --sms-access-key-secret    | SMS_ACCESS_KEY_SECRET                  | QqA7JZrwN7nDYMbZZMazCsfjTSfjNU                                                                                  |
| --sms-sign-name            | SMS_SIGN_NAME                          | taosdata                                                                                                        |
| --sms-template-code        | SMS_TEMPLATE_CODE                      | SMS_227010280                                                                                                   |
| --sms-template-param       | SMS_TEMPLATE_PARAM                     | `"{\\\"alarm_level\\\":\\\"%s\\\",\\\"time\\\":\\\"%s\\\",\\\"name\\\":\\\"%s\\\",\\\"content\\\":\\\"%s\\\"}"` |
| --sms-phone-numbers        | SMS_PHONE_NUMBERS                      | `18900000000`                                                                                                   |
| --sms-listen-addr          | SMS_LISTEN_ADDR                        | `127.0.0.1:9100`                                                                                                |