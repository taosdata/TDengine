#!/usr/bin/env bash
BIN=$(realpath $(dirname $0))
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
TDENGINE_API=${TDENGINE_API:-http://127.0.0.1:6041}
TDENGINE_USER=${TDENGINE_USER:-root}
TDENGINE_PASSWORD=${TDENGINE_PASSWORD:-taosdata}

LOG_REPLICA=${LOG_REPLICA:1}

# TDinsight dashboard configurations
TDINSIGHT_DASHBOARD_ID=15167
TDINSIGHT_DASHBOARD_UID=${TDINSIGHT_DASHBOARD_UID:-tdinsight}
TDINSIGHT_DASHBOARD_TITLE=${TDINSIGHT_DASHBOARD_TITLE:-TDinsight}
TDENGINE_EDITABLE=${TDENGINE_EDITABLE:-false}

options=$(getopt -l "help,verbose,remove,offline,download-only,\
plugin-version:,from-grafana,\
grafana-provisioning-dir:,grafana-plugins-dir:,grafana-org-id:,\
tdengine-ds-name:,tdengine-api:,tdengine-user:,tdengine-password:,tdengine-cloud-token:\
tdinsight-uid:,tdinsight-title:,tdinsight-editable," \
-o "hVRFv:P:G:O:n:a:u:p:i:t:e" -a -- "$@")

usage() { # Function: Print a help message.
  cat << EOF
Usage:
   $0
   $0 -h|--help
   $0 -n <ds-name> -a <api-url> -u <user> -p <password>

Install and configure TDinsight dashboard in Grafana on Ubuntu 18.04/20.04 system.

-h, --help                                  Display help

-V, --verbose                               Run script in verbose mode. Will print out each step of execution.
-R, --remove                                Remove TDinsight dashboard, TDengine data source and the plugin.
-F, --offline                               Use file in local path (the directory this script located in or just current work directory).
    --download-only                         Download plugin and dashboard json file into current directory.

-v, --plugin-version <version>              TDengine datasource plugin version, [default: $TDENGINE_PLUGIN_VERSION]

-P, --grafana-provisioning-dir <dir>        Grafana provisioning directory, [default: $GF_PROVISIONING_DIR]
-G, --grafana-plugins-dir <dir>             Grafana plugins directory, [default: $GF_PLUGINS_DIR]
-O, --grafana-org-id <number>               Grafana organization id. [default: $GF_ORG_ID]

-n, --tdengine-ds-name <string>             TDengine datasource name, no space. [default: $TDENGINE_DS_NAME]
-a, --tdengine-api <url>                    TDengine REST API endpoint. [default: $TDENGINE_API]
-u, --tdengine-user <string>                TDengine user name. [default: $TDENGINE_USER]
-p, --tdengine-password <string>            TDengine password. [default: $TDENGINE_PASSWORD]
    --tdengine-cloud-token <string>         TDengine cloud token. [env: \$TDENGINE_CLOUD_TOKEN]

-i, --tdinsight-uid <string>                Replace with a non-space ASCII code as the dashboard id. [default: $TDINSIGHT_DASHBOARD_UID]
-t, --tdinsight-title <string>              Dashboard title. [default: $TDINSIGHT_DASHBOARD_TITLE]
-e, --tdinsight-editable                    If the provisioning dashboard could be editable. [default: false]
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
    export TDENGINE_PLUGIN_VERSION=$1
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
    if [[ $1 == "http"* || $1 == "https"* ]]; then
      export TDENGINE_API=$1
    else
      echo "TDengine API endpoint must start with http or https"
      exit 1
    fi
    ;;
  -u | --tdengine-user)
    shift
    export TDENGINE_USER=$1
    ;;
  -p | --tdengine-password)
    shift
    export TDENGINE_PASSWORD=$1
    ;;
  --tdengine-cloud-token)
    shift
    export TDENGINE_CLOUD_TOKEN=$1
    ;;
  -i | --tdinsight-uid)
    shift
    export TDINSIGHT_DASHBOARD_UID=$1
    ;;
  -t | --tdinsight-title)
    shift
    export TDINSIGHT_DASHBOARD_TITLE=$1
    ;;
  -e | --tdinsight-editable)
    export TDENGINE_EDITABLE=true
    ;;
  --)
    shift
    break
    ;;
  esac
  shift
done

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

echoerr() {
  echo "$@" >&2
}

download_plugin() {
  [ "$verbose" = "0" ] || echoerr "** download plugin version $TDENGINE_PLUGIN_VERSION"
  if [ -s tdengine-datasource.zip.md5 ] && md5sum -c tdengine-datasource.zip.md5 >/dev/null; then
    echoerr "** plugin has been downloaded"
  else
    echoerr "** downloading from github"
    wget -c -o /dev/stderr https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
      -O tdengine-datasource.zip
    md5sum tdengine-datasource.zip > tdengine-datasource.zip.md5
  fi
}

install_plugin() {
  which grafana-cli > /dev/null || (echo "Grafana has not been installed in the server, install it first."; exit 1)
  if [ "$OFFLINE" = 0 ]; then
    download_plugin
  else
    [ -s tdengine-datasource.zip ] || (echo "use offline cache, but plugin not exist"; exit 1)
  fi
  # open a simple server for local url
  port=$(shuf -i 2000-65000 -n 1)

  if [ "$(command -v python3)" = "" ]; then
    if [ "$(command -v python2)" = "" ]; then
      echo "Grafana plugin installation requires python2 or python3, please install one first!"; exit 1
    fi
    python2 -m SimpleHTTPServer $port &
    pid=$!
  else
    python3 -m http.server $port &
    pid=$!
  fi
  sleep 1
  set +e
  $SUDO_GRAFANA grafana-cli --pluginUrl http://localhost:$port/tdengine-datasource.zip plugins install tdengine-datasource
  status=$?
  kill $pid
  set -e
  if [ "$status" != "0" ]; then
    exit $status
  fi
}

remove_plugin() {
  set +e
  $SUDO sed -i "s/tdengine-datasource//g" /etc/grafana/grafana.ini
  $SUDO rm -rf $GF_PLUGINS_DIR/tdengine-datasource
  set -e
}

provisioning_datasource() {
  [ -d $GF_PROVISIONING_DATASOURCES_DIR ] || $SUDO mkdir $GF_PROVISIONING_DATASOURCES_DIR
  echo "* Provisioning $GF_PROVISIONING_DATASOURCES_DIR/$TDENGINE_DS_NAME.yaml"
  TDENGINE_BASIC_AUTH=$(printf "$TDENGINE_USER:$TDENGINE_PASSWORD" | base64)
  $SUDO tee $GF_PROVISIONING_DATASOURCES_DIR/$TDENGINE_DS_NAME.yaml >/dev/null <<EOF
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
  # <int> org id. will be set to 1 if not specified
  orgId: $GF_ORG_ID
  # <string> url
  url: $TDENGINE_API
  # <map> fields that will be converted to json and stored in json_data
  secureJsonData:
    url: $TDENGINE_API
    basicAuth: $TDENGINE_BASIC_AUTH
    # <string> cloud service token of TDengine,  optional.
    token: $TDENGINE_CLOUD_TOKEN

  version: 1
  # <bool> allow users to edit datasources from the UI.
  editable: $TDENGINE_EDITABLE
EOF
}

remove_datasource() {
  $SUDO rm $GF_PROVISIONING_DATASOURCES_DIR/$TDENGINE_DS_NAME.yaml
}

download_dashboard() {
  #get_dashboard_by_id $TDINSIGHT_DASHBOARD_ID TDinsight-$TDINSIGHT_DASHBOARD_ID.json
  echo "**deprecated: 3.2.6 and above will not download dashboard from grafana, use builtin dashboards via grafana datasource settings.**" >&2
  true
}

provisioning_dashboard() {
  echo "* Provisioning dashboard $TDINSIGHT_DASHBOARD_ID as $TDINSIGHT_DASHBOARD_UID"
  # 1. Download latest dashboard json file or use cached file
  if [ "$OFFLINE" = "0" ]; then
    get_dashboard_by_id $TDINSIGHT_DASHBOARD_ID $TDINSIGHT_DASHBOARD_UID.json
  else
    if [ -e "TDinsight-$TDINSIGHT_DASHBOARD_ID.json" ]; then
      $SUDO cp "TDinsight-$TDINSIGHT_DASHBOARD_ID.json" $TDINSIGHT_DASHBOARD_UID.json
    elif [ -e "$BIN/TDinsight-$TDINSIGHT_DASHBOARD_ID.json" ]; then
      $SUDO cp "$BIN/TDinsight-$TDINSIGHT_DASHBOARD_ID.json" $TDINSIGHT_DASHBOARD_UID.json
    else
      echo "** in offline mode bug no cached files there"
      exit 1
    fi
  fi

  # 2. Replace with TDengine data source name
  sed 's#"datasource": "${DS_TDENGINE}"#"datasource": "'$TDENGINE_DS_NAME'"#g' -i $TDINSIGHT_DASHBOARD_UID.json
  sed 's/"tdinsight"/"'$TDINSIGHT_DASHBOARD_UID'"/' -i $TDINSIGHT_DASHBOARD_UID.json
  sed 's/"TDinsight"/"'"$TDINSIGHT_DASHBOARD_TITLE"'"/' -i $TDINSIGHT_DASHBOARD_UID.json
  sed 's/"gnetId": 15167/"gnetId": null/' -i $TDINSIGHT_DASHBOARD_UID.json


  # 4. Add dashboard config
  cat > $TDINSIGHT_DASHBOARD_UID.yaml <<EOF
apiVersion: 1
providers:
  - name: ${TDINSIGHT_DASHBOARD_UID}
    type: file
    updateIntervalSeconds: 60
    options:
      path: /etc/grafana/provisioning/dashboards/$TDINSIGHT_DASHBOARD_UID.json
EOF

  # 3. Add this to provisioning directory
  [ -d $GF_PROVISIONING_DASHBOARDS_DIR ] || $SUDO mkdir -p $GF_PROVISIONING_DASHBOARDS_DIR
  echo "** Provisioning $GF_PROVISIONING_DASHBOARDS_DIR/$TDINSIGHT_DASHBOARD_UID.{json,yaml}"
  mv $TDINSIGHT_DASHBOARD_UID.{json,yaml} $GF_PROVISIONING_DASHBOARDS_DIR
  echo "** Provisioning done."
}

remove_dashboard() {
  set +e
  $SUDO rm -f $GF_PROVISIONING_DASHBOARDS_DIR/$TDINSIGHT_DASHBOARD_UID.{json,yaml}
  set -e
}

if [ "$REMOVE" == "1" ]; then
  remove_dashboard
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
  cd $BIN
  download_plugin
  echo tdengine-datasource.zip
  echo tdengine-datasource.zip.md5
  exit 0
fi

# Install tdengine-datasource plugin
if [ "$INSTALL_FROM_GRAFANA" = "1" ]; then
  install_plugin_from_grafana
else

  if [ "$OFFLINE" = "1" ] && [ -f "$BIN/tdengine-datasource.zip.md5" ]; then
    echoerr using offline plugin
    cd $BIN
  elif [ "$TDENGINE_PLUGIN_VERSION" = "latest" ]; then
    TDENGINE_PLUGIN_VERSION=$(get_latest_release)
    echoerr using tdengine-datasource plugin $TDENGINE_PLUGIN_VERSION
  else
    echoerr using tdengine-datasource plugin $TDENGINE_PLUGIN_VERSION
  fi

  install_plugin

fi

# Provisioning TDengine data source
provisioning_datasource
