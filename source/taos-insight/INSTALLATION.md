# Install the Grafana Plugin for TDengine

- [Install the Grafana Plugin for TDengine](#install-the-grafana-plugin-for-tdengine)
  - [Compatible with TDengine version](#compatible-with-tdengine-version)
  - [One-line installer](#one-line-installer)
  - [Install with GUI](#install-with-gui)
  - [Install Manually](#install-manually)
  - [Installation on macOS](#installation-on-macos)

[TDengine] is open-sourced big data platform under GNU AGPL v3.0, designed and optimized for the Internet of Things (
IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring, developed
by [TDengine](https://tdengine.com/).

[TDengine] data source plugin is developed for [Grafana]. This document explains how to install and configure the data
source plugin, and use it as a time-series database. We'll take a look at the data source options, variables, querying,
and other options specific to this data source.

At first, please refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/)
for instructions on how to add a data source to Grafana. Note that, only users with the organization admin role can add
data sources.

## Compatible with TDengine version
| TDengine grafana plugin version | major changes                                                                                                            | TDengine version |
| :-----------------------------: | ------------------------------------------------------------------------------------------------------------------------ | :--------------: |
|              4.0.0              | 1. Simplified backend data processing by aligning with Grafana SDK's DataFrame best practices. <br> 2. Fixed alert tag extraction for queries with grouped dimensions. <br> 3. Breaking: removed deprecated query fields (`alias`, `colNameFormatStr`, `colNameToGroup`, plugin `timeShift*`) and removed TDinsightV2/Monitor dashboards (see README/CHANGELOG migration notes). | 3.3.7.0 or later |
|              3.8.0              | Compatible Grafana version changed from 7.5 to 8.0.                                                                      | 3.3.7.0 or later |
|              3.7.6              | Fix password changes not updating authentication credentials.                                                            | 3.3.7.0 or later |
|              3.7.5              | Resolve dependency library security issues.                                                                              | 3.3.7.0 or later |
|              3.7.4              | Fix alert configuration issue in Grafana 12.                                                                             | 3.3.7.0 or later |
|              3.7.3              | Updated the TDinsightV3 dashboard.                                                                                       | 3.3.7.0 or later |
|              3.7.2              | Updated the taosX dashboard.                                                                                             | 3.3.0.0 or later |
|              3.7.1              | 1. Supports multi-line SQL editor. </br> 2. Added the 'Classified Connection Counts' table to the TDinsightV3 dashboard. | 3.3.0.0 or later |
|              3.7.0              | Migrate TDengine Grafana plugin from Angular to React.                                                                   | 3.3.0.0 or later |
|              3.6.3              | Fix TDinsight Request data issue.                                                                                        | 3.3.0.0 or later |
|              3.6.2              | Improve alert ui.                                                                                                        | 3.3.0.0 or later |
|              3.6.1              | Improve alert ui.                                                                                                        | 3.3.0.0 or later |
|              3.6.0              | Support taosd alert configuration for grafana 11 and 7.5 version.                                                        | 3.3.0.0 or later |
|              3.5.2              | Update TDinsightV3 dashboard.                                                                                            | 3.3.0.0 or later |
|              3.5.1              | Update TDinsightV3 and taosX dashboard.                                                                                  | 3.3.0.0 or later |
|              3.5.0              | New TDinsightV3 and taosX dashboard.                                                                                     | 3.2.3.0 or later |
|              3.4.0              | New adapter dashboard.                                                                                                   | 3.2.0.1 or later |
|              3.2.7              | Support TDengine 3.0.                                                                                                    | 3.0.0.0 or later |


## One-line installer

```bash
TDENGINE_DS_NAME=TDengine
TDENGINE_CLOUD_URL=
TDENGINE_CLOUD_TOKEN=
bash -c "$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)"
```

- `TDengine_DS_NAME`: The data source name to create
- `TDENGINE_CLOUD_URL`: The TDengine url, eg. `http://localhost:6041`
- `TDENGINE_CLOUD_TOKEN`: The TDengine cloud service token, optional.
- `TDENGINE_USER`/`TDENGINE_PASSWORD`: The TDengine username/password, optional.

Type `install.sh --help` for the full usage of the script.

## Install with GUI

The TDengine data source plugin is already published as a signed Grafana plugin. You can easily install it from Grafana
Configuration GUI. In any platform you already installed Grafana, you can open the URL http://localhost:3000 then click
plugin menu from left panel.

![click plugin menu](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/click-plugin-menu-from-config.png)

Then key in `TDengine` to search.

![search TDengine](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/search-tdengine-from-config.png)

## Install Manually

We recommend you use the prebuilt package of the plugin
in [the latest release download page](https://github.com/taosdata/grafanaplugin/releases/latest).

Option 1, you can install the TDengine data source plugin with `grafana-cli`.

```bash
sudo -u grafana grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip \
  plugins install tdengine-datasource
```

Option 2, you can install the plugin manually.

```bash
# make sure to use the right plugins directory
GF_PLUGINS_DIR=/var/lib/grafana/plugins
# install plugin
V=3.1.7
wget -c https://github.com/taosdata/grafanaplugin/releases/download/v$V/tdengine-datasource-$V.zip
sudo -u grafana unzip -oq tdengine-datasource-$V.zip -d $GF_PLUGINS_DIR
sudo -u grafana sh -c "chmod +x $GF_PLUGINS_DIR/tdengine-datasource/tdengine-datasource*"
```

Here is a unified shell script to automatically download and install the latest plugin in a Grafana server.

```bash
# make sure to use the right plugins directory
GF_PLUGINS_DIR=/var/lib/grafana/plugins

get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"v([^"]+)".*/\1/'
}
install_plugin() {
  V=$1
  [ -f tdengine-datasource-$V.zip ] ||
    wget -c https://github.com/taosdata/grafanaplugin/releases/download/v$V/tdengine-datasource-$V.zip
  sudo -u grafana unzip -oq tdengine-datasource-$V.zip -d $GF_PLUGINS_DIR
  sudo -u grafana sh -c "chmod +x $GF_PLUGINS_DIR/tdengine-datasource/tdengine-datasource*"
}

install_plugin $(get_latest_release)
```

Option 3, if you use the plugin in docker, just set the environment `GF_INSTALL_PLUGINS`
like `GF_INSTALL_PLUGINS=https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip;tdengine-datasource`,
or refer
to [Build and run a Docker image with pre-installed plugins](https://grafana.com/docs/grafana/latest/installation/docker/#build-and-run-a-docker-image-with-pre-installed-plugins)
to pre-build a custom Grafana image with the TDengine data source plugin.

Option 4, build and install this plugin by yourself. You should follow
the [CONTRIBUTING](https://github.com/taosdata/grafanaplugin/blob/master/CONTRIBUTING.md) steps to set up develop
environment and build the plugin.

```bash
yarn build:all
```

Then copy the `dist/` directory to Grafana plugin directory.

```bash
sudo -u grafana rsync -rlzP dist/ /var/lib/grafana/plugins/tdengine-datasource
```

It will also generate a zipped package named `tdengine-datasource-<version>.zip` for distribution.

Note that the plugin has not been published to <https://grafana.com> officially (the plugin is under review, as if you
want to know) - that means the plugin is unsigned for public use. So if you use it in 7.x and 8.x of grafana, you should
configure in `/etc/grafana/grafana.ini` by add these lines to allow unsigned plugin before start the Grafana server.

```ini
[plugins]
allow_loading_unsigned_plugins = tdengine-datasource
```

Or use environment variable `GF_ALLOW_LOADING_UNSIGNED_PLUGINS=tdengine-datasource` in docker and other container-based
environments.

In any particular scenario, if you need a signed plugin, please follow
the [Sign a plugin](https://grafana.com/docs/grafana/latest/developers/plugins/sign-a-plugin) instructions to sign a
private plugin. Commands may be like this:

```sh
export GRAFANA_API_KEY=<YOUR_API_KEY>
yarn run grafana-toolkit plugin:sign \
  --signatureType private \
  --rootUrls 'http://localhost:3000'
```

`rootUrls` is your local grafana server url.

The next step is restart the Grafana server. The new data source should now be available in the data source type
dropdown in the **Add Data Source** view. For systemd based system, the restart command is:

```sh
sudo systemctl restart grafana-server
```

For SysVInit based system (CentOS 6 etc.):

```sh
sudo service grafana-server restart
```

## Installation on macOS

The best way to install Grafana is to use [Homebrew](https://brew.sh/) with which to install the most recent released
version of Grafana using Homebrew package.

Open a terminal and enter:

```sh
brew update
brew install grafana
```

Next, add TDengine’s plugin with the following command:

```sh
sudo grafana-cli --pluginsDir /opt/homebrew/var/lib/grafana/plugins --pluginUrl\
  https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip \
  plugins install tdengine-datasource
```

The `pluginsDir` option `/opt/homebrew/var/lib/grafana/plugins` should be overridden if the directory not exist(that
will depend on how you install the Grafana service).

You can check the plugins directory to see if tdengine-datasource plugin installed:

```sh
$ ls /opt/homebrew/var/lib/grafana/plugins
tdengine-datasource
```

TDengine plugin has not been signed, so you should change the grafana configuration to allow unsigned plugin.

Since you are using macOS with Homebrew, Grafana instances installed using Homebrew require you to edit
the `grafana.ini` file directly. The way to change the `grafana.ini` file is to run the following command:

```sh
open -a textedit /opt/homebrew/etc/grafana/grafana.ini
```

Note that, this plugin is signed since v3.1.7. If you are using an older version, please configure to allow loading
unsigned plugins:

First, open the file, search for

```ini
;allow_loading_unsigned_plugins
```

Then, delete the semicolon to uncomment it, and change the line to:

```ini
allow_loading_unsigned_plugins = tdengine-datasource
```

After that, go ahead and save the configuration file.

(Re-)start Grafana service:

```sh
# brew services start grafana
brew services restart grafana
```

Point to plugins pages to search and check TDengine plugin is in the list.
