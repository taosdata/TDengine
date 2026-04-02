#!/usr/bin/env bash
if [ -e "$0" ]; then
  BIN=$(realpath $(dirname $0))
else
  BIN=./
fi
set -e
verbose=0
REMOVE=0
OFFLINE=0
DOWNLOAD_ONLY=0
INSTALL_FROM_GRAFANA=0
SUDO=$(command -v sudo)
if [ "$SUDO" != "" ]; then
  SUDO_GRAFANA="$SUDO -u grafana"
fi

[ -f .env ] && source .env

TDENGINE_PLUGIN_VERSION=${TDENGINE_PLUGIN_VERSION:-latest}

# Grafana configurations
GF_PROVISIONING_DIR=${GF_PROVISIONING_DIR:-/etc/grafana/provisioning}
GF_PLUGINS_DIR=${GF_PLUGINS_DIR:-/var/lib/grafana/plugins}

GF_PROVISIONING_DATASOURCES_DIR=${GF_PROVISIONING_DIR}/datasources
GF_PROVISIONING_DASHBOARDS_DIR=${GF_PROVISIONING_DIR}/dashboards
GF_PROVISIONING_NOTIFIERS_DIR=${GF_PROVISIONING_DIR}/notifiers
GF_ORG_ID=${GF_ORG_ID:-1}

# TDengine configurations
TDENGINE_DS_ENABLED=true
TDENGINE_DS_NAME=${TDENGINE_DS_NAME:-TDengine}
if [ "$TDENGINE_CLOUD_URL" = "" ]; then
  TDENGINE_API=${TDENGINE_API:-http://127.0.0.1:6041}
else
  TDENGINE_API=${TDENGINE_CLOUD_URL}
fi
TDENGINE_USER=${TDENGINE_USER:-root}
TDENGINE_PASSWORD=${TDENGINE_PASSWORD:-taosdata}

LOG_REPLICA=${LOG_REPLICA:1}

SMS_ENABLED=${SMS_ENABLED:-false}
SMS_NOTIFIER_NAME=${SMS_NOTIFIER_NAME:-TDinsight Builtin SMS}
SMS_NOTIFIER_IS_DEFAULT=${SMS_NOTIFIER_IS_DEFAULT:-false}
SMS_ACCESS_KEY_ID=${SMS_ACCESS_KEY_ID}
SMS_ACCESS_KEY_SECRET=${SMS_ACCESS_KEY_SECRET}
SMS_SIGN_NAME=${SMS_SIGN_NAME}
SMS_TEMPLATE_CODE=$SMS_TEMPLATE_CODE
SMS_TEMPLATE_PARAM=$SMS_TEMPLATE_PARAM
SMS_PHONE_NUMBERS=${SMS_PHONE_NUMBERS}
SMS_LISTEN_ADDR=${SMS_LISTEN_ADDR:-127.0.0.1:9100}

options=$(getopt -l "help,verbose,remove,offline,download-only,\
plugin-version:,from-grafana,\
grafana-provisioning-dir:,grafana-plugins-dir:,grafana-org-id:,\
tdengine-ds-name:,tdengine-api:,tdengine-user:,tdengine-password:,tdengine-cloud-token:,\
editable,\
sms-enabled,sms-notifier-name:,sms-notifier-uid:,sms-notifier-is-default,\
sms-access-key-id:,sms-access-key-secret:,\
sms-sign-name:,sms-template-code:,sms-template-param:,sms-phone-numbers:,\
ms-listen-addr:" \
-o "hVRFv:P:G:O:n:a:u:p:t:esN:U:DI:K:S:C:T:B:L:y" -a -- "$@")

usage() { # Function: Print a help message.
  cat << EOF
Usage:
   $0
   $0 -h|--help
   $0 -n <ds-name> -a <api-url> -u <user> -p <password>
   $0 -n <ds-name> -a <api-url> -t <token>

Install and configure TDinsight dashboard in Grafana on Ubuntu 18.04/20.04 system.

-h, --help                                  Display help

-V, --verbose                               Run script in verbose mode. Will print out each step of execution.
-R, --remove                                Remove TDengine data source and the plugin.
-F, --offline                               Use file in local path (the directory this script located in or just current work directory).
    --download-only                         Download plugin and dashboard json file into current directory.
    --from-grafana                          Do not download plugin, use grafana-cli latest plugin version.

-v, --plugin-version <version>              TDengine datasource plugin version, [default: $TDENGINE_PLUGIN_VERSION]

-P, --grafana-provisioning-dir <dir>        Grafana provisioning directory, [default: $GF_PROVISIONING_DIR]
-G, --grafana-plugins-dir <dir>             Grafana plugins directory, [default: $GF_PLUGINS_DIR]
-O, --grafana-org-id <number>               Grafana organization id. [default: $GF_ORG_ID]

-n, --tdengine-ds-name <string>             TDengine datasource name, no space. [default: $TDENGINE_DS_NAME]
-a, --tdengine-api <url>                    TDengine REST API endpoint. [default: $TDENGINE_API]
-u, --tdengine-user <string>                TDengine user name. [default: $TDENGINE_USER]
-p, --tdengine-password <string>            TDengine password. [default: $TDENGINE_PASSWORD]
-t, --tdengine-cloud-token <string>         TDengine cloud token. [default: $TDENGINE_CLOUD_TOKEN]
-e, --editable                              TDengine datasource is editable or not [default: $TDENGINE_EDITABLE]

Aliyun SMS for datasource configuration:
-s, --sms-enabled                           To enable tdengine-datasource plugin builtin Aliyun sms webhook.
-N, --sms-notifier-name <string>            Provisioning notifier name.[default: $SMS_NOTIFIER_NAME]
-U, --sms-notifier-uid <string>             Provisioning notifier uid, use lowercase notifier name by default.
-D, --sms-notifier-is-default               Set notifier as default.
-I, --sms-access-key-id <string>            Aliyun sms access key id
-K, --sms-access-key-secret <string>        Aliyun sms access key secret
-S, --sms-sign-name <string>                Sign name
-C, --sms-template-code <string>            Template code
-T, --sms-template-param <string>           Template params in json format like '{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}'
-B, --sms-phone-numbers <string>            Comma-separated numbers list, eg "189xxxxxxxx,132xxxxxxxx"
-L, --sms-listen-addr <string>              [default: $SMS_LISTEN_ADDR]
EOF
}

eval set -- "$options"

while true; do
  case $1 in
  -h | --help)
    usage
    exit 0
    ;;
  -V | --verbose)
    export verbose=1
    set -xv # Set xtrace and verbose mode.
    ;;
  -R | --remove)
    export REMOVE=1
    ;;
  -F | --offline)
    export OFFLINE=1
    ;;
  --download-only)
    export DOWNLOAD_ONLY=1
    ;;
  --from-grafana)
    export INSTALL_FROM_GRAFANA=1
    ;;
  -v | --plugin-version)
    shift
    export TDENGINE_PLUGIN_VERSION=$2
    ;;
  -P | --grafana-provisioning-dir)
    shift
    export GF_PROVISIONING_DIR=$1
    ;;
  -G | --grafana-plugins-dir)
    shift
    export GF_PLUGINS_DIR=$1
    ;;
  -O | --grafana-org-id)
    shift
    export GF_ORG_ID=$1
    ;;
  -n | --tdengine-ds-name)
    shift
    export TDENGINE_DS_NAME=$1
    ;;
  -a | --tdengine-api)
    shift
    export TDENGINE_API=$1
    ;;
  -u | --tdengine-user)
    shift
    export TDENGINE_USER=$1
    ;;
  -p | --tdengine-password)
    shift
    export TDENGINE_PASSWORD=$1
    ;;
  -t | --tdengine-cloud-token)
    shift
    export TDENGINE_CLOUD_TOKEN=$1
    ;;
  -e | --editable)
    shift
    export TDENGINE_EDITABLE=true
    ;;
  -E | --external-notifier)
    shift
    export EXTERNAL_NOTIFIER=$1
    ;;
  -s | --sms-enabled)
    export SMS_ENABLED=true
    ;;
  -N | --sms-notifier-name)
    shift
    export SMS_NOTIFIER_NAME=$1
    ;;
  -U | --sms-notifier-uid)
    shift
    export SMS_NOTIFIER_UID=$1
    ;;
  -D | --sms-notifier-is-default)
    export SMS_NOTIFIER_IS_DEFAULT=true
    ;;
  -I | --sms-access-key-id)
    shift
    export SMS_ACCESS_KEY_ID=$1
    ;;
  -K | --sms-access-key-secret)
    shift
    export SMS_ACCESS_KEY_SECRET=$1
    ;;
  -S | --sms-sign-name)
    shift
    export SMS_SIGN_NAME=$1
    ;;
  -C | --sms-template-code)
    shift
    export SMS_TEMPLATE_CODE=$1
    ;;
  -T | --sms-template-param)
    shift
    export SMS_TEMPLATE_PARAM=$1
    ;;
  -B | --sms-phone-numbers)
    shift
    export SMS_PHONE_NUMBERS=$1
    ;;
  -L | --sms-listen-addr)
    shift
    export SMS_LISTEN_ADDR=$1
    ;;
  --)
    shift
    break
    ;;
  esac
  shift
done

if [ "$SMS_NOTIFIER_UID" == "" ]; then
  SMS_NOTIFIER_UID=$(echo $SMS_NOTIFIER_NAME | tr 'A-Z' 'a-z' | tr ' ' '-')
fi

if [ "$EXTERNAL_NOTIFIER" != "" ]; then
  echo "TDinsight builtin sms is disabled by external notifier settings."
  export SMS_ENABLED=false
fi

_assert_dir() {
  [ -d "$1" ] || $SUDO mkdir -p "$1"
}


get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" | # Get latest release from GitHub api
    grep '"tag_name":' |                                                                # Get tag line
    sed -E 's/.*"v([^"]+)".*/\1/'                                                       # Pluck JSON value
}

get_dashboard_by_id() {
  [ "$verbose" = "0" ] || echo "** Get dashboard json file by id $1 to $2"
  id=$1
  file=$2
  wget -qO $file https://grafana.com/api/dashboards/${id}/revisions/latest/download
}

download_plugin() {
  [ "$verbose" = "0" ] || echo "** download plugin version $TDENGINE_PLUGIN_VERSION"
  [ -s tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip ] || \
    wget -c https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip
  
}

install_plugin() {
  which grafana-cli > /dev/null || (echo "Grafana has not been installed in the server, install it first."; exit 1)
  set +e
  py=$(command -v python3)
  set -e
  if [ "$py" = "" ]; then
    echo "install plugin from github"
    $SUDO_GRAFANA grafana-cli --pluginUrl \
      https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
      plugins install tdengine-datasource
  else
    if [ "$OFFLINE" = 0 ]; then
      download_plugin
    else
      [ -s tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip ] || (echo "use offline cache, but plugin not exist"; exit 1)
    fi
    # open a simple server for local url
    port=$(shuf -i 2000-65000 -n 1)
    if [ "$(command -v python3)" == "" ]; then
      if [ "$(command -v python2)" == "" ]; then
        echo "Grafana plugin installation requires python2 or python3, please install one first!"
        exit 1
      fi
      python2 -m SimpleHTTPServer $port &
    else
      python3 -m http.server $port &
    fi
    pid=$!
    sleep 1
    set +e
    $SUDO_GRAFANA grafana-cli --pluginUrl http://localhost:$port/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip plugins install tdengine-datasource
    status=$?
    kill $pid
    set -e
    if [ "$status" != "0" ]; then
      exit $status
    fi
  fi
}

install_plugin_from_grafana() {
  which grafana-cli > /dev/null || (echo "Grafana has not been installed in the server, install it first."; exit 1)
  echo "* install tdengine-datasource plugin from Grafana"
  $SUDO_GRAFANA grafana-cli plugins install tdengine-datasource
}

remove_plugin() {
  $SUDO rm -rf $GF_PLUGINS_DIR/tdengine-datasource
}

provisioning_datasource() {
  [ -d $GF_PROVISIONING_DATASOURCES_DIR ] || mkdir $GF_PROVISIONING_DATASOURCES_DIR
  echo "* Provisioning $GF_PROVISIONING_DATASOURCES_DIR/$TDENGINE_DS_NAME.yaml"
  TDENGINE_BASIC_AUTH=$(printf "$TDENGINE_USER:$TDENGINE_PASSWORD" | base64)
  tee $TDENGINE_DS_NAME.yaml > /dev/null  <<EOF
# config file version
apiVersion: 1

# list of datasources to insert/update depending
# whats available in the database
datasources:
  # <string, required> name of the datasource. Required
- name: $TDENGINE_DS_NAME
  # <string, required> datasource type. Required
  type: tdengine-datasource
  # <string, required> access mode. direct or proxy. Required
  # <int> org id. will be set to  orgId 1 if not specified
  orgId: $GF_ORG_ID
  # <string> url
  url: $TDENGINE_API
  # <map> fields that will be converted to json and stored in json_data
  secureJsonData:
    url: $TDENGINE_API
    basicAuth: $TDENGINE_BASIC_AUTH
    # <string> cloud service token of TDengine,  optional.
    token: $TDENGINE_CLOUD_TOKEN

    # aliSms* is configuration options for builtin sms notifier powered by Aliyun Cloud SMS

    # <string> the key id from Aliyun.
    aliSmsAccessKeyId: "$SMS_ACCESS_KEY_ID"
    # <string> key secret paired to key id.
    aliSmsAccessKeySecret: "$SMS_ACCESS_KEY_SECRET"
    aliSmsSignName: "$SMS_SIGN_NAME"
    # <string> sms template code from Aliyun. eg. SMS_123010240
    aliSmsTemplateCode: "$SMS_TEMPLATE_CODE"
    # <string> serialized json string for sms template parameters. eg.
    #  '{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}'
    aliSmsTemplateParam: '$SMS_TEMPLATE_PARAM'
    # <string> phone number list, separated by comma ,
    aliSmsPhoneNumbersList: "$SMS_PHONE_NUMBERS"
    # <string> builtin sms notifier webhook address.
    aliSmsListenAddr: "$SMS_LISTEN_ADDR"
  version: 1
  # <bool> allow users to edit datasources from the UI.
  editable: $TDENGINE_EDITABLE
EOF
   $SUDO cp -f $TDENGINE_DS_NAME.yaml $GF_PROVISIONING_DATASOURCES_DIR/
   rm $TDENGINE_DS_NAME.yaml
}

remove_datasource() {
  $SUDO rm -f $GF_PROVISIONING_DATASOURCES_DIR/$TDENGINE_DS_NAME.yaml
}

provisioning_notifiers() {
  if [ "$SMS_ENABLED" == "true" ]; then
    _assert_dir $GF_PROVISIONING_NOTIFIERS_DIR
    echo "* Provisioning $GF_PROVISIONING_NOTIFIERS_DIR/${SMS_NOTIFIER_UID}.yaml"
    tee ${SMS_NOTIFIER_UID}.yaml > /dev/null <<EOF
# config file version
apiVersion: 1

notifiers:
  - name: ${SMS_NOTIFIER_NAME}
    type: webhook
    uid: ${SMS_NOTIFIER_UID}
    is_default: ${SMS_NOTIFIER_IS_DEFAULT}
    settings:
      url: http://${SMS_LISTEN_ADDR}/sms
      httpMethod: POST
EOF
    $SUDO cp -f ${SMS_NOTIFIER_UID}.yaml $GF_PROVISIONING_NOTIFIERS_DIR/
    rm ${SMS_NOTIFIER_UID}.yaml
  fi
}

remove_notifier() {
  set +e
  [ -e "$GF_PROVISIONING_NOTIFIERS_DIR/${SMS_NOTIFIER_UID}.yaml" ] && $SUDO rm -f "$GF_PROVISIONING_NOTIFIERS_DIR/${SMS_NOTIFIER_UID}.yaml"
  set -e
}

if [ "$REMOVE" == "1" ]; then
  remove_notifier
  remove_datasource
  remove_plugin
  exit 0
fi

####################################
# main scripts
####################################
if [ "$DOWNLOAD_ONLY" = "1" ]; then
  if [ "$TDENGINE_PLUGIN_VERSION" = "latest" ]; then
    TDENGINE_PLUGIN_VERSION=$(get_latest_release)
  fi
  download_plugin
  download_dashboard
  echo "TDENGINE_PLUGIN_VERSION=$TDENGINE_PLUGIN_VERSION" > .tdinsight.cache

  echo .tdinsight.cache
  echo TDinsight-$TDINSIGHT_DASHBOARD_ID.json
  echo tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip
  exit 0
fi

# Install tdengine-datasource plugin
if [ "$INSTALL_FROM_GRAFANA" = "1" ]; then
  install_plugin_from_grafana
else

  if [ "$OFFLINE" = "1" ]; then
    [ -e "$BIN/.tdinsight.cache" ] && source $BIN/.tdinsight.cache
    [ -e ".tdinsight.cache" ] && source .tdinsight.cache
  fi

  if [ "$TDENGINE_PLUGIN_VERSION" = "latest" ]; then
    TDENGINE_PLUGIN_VERSION=$(get_latest_release)
    echo using tdengine-datasource plugin $TDENGINE_PLUGIN_VERSION
  fi

  install_plugin

fi

# Provisioning TDengine data source
provisioning_datasource

# Provisioning TDengine notification channels
provisioning_notifiers
