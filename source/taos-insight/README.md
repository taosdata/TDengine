# Grafana Plugin for TDengine

- [Grafana Plugin for TDengine](#grafana-plugin-for-tdengine)
  - [Usage](#usage)
    - [Add Data Source](#add-data-source)
    - [Import Dashboard](#import-dashboard)
  - [Important changes](#important-changes)
    - [v4.0.0 - **Breaking Changes Release**](#v400---breaking-changes-release)
      - [Breaking Changes](#breaking-changes)
      - [Migration Guide](#migration-guide)
        - [Dashboard Queries](#dashboard-queries)
        - [SQL Query Updates](#sql-query-updates)
        - [Alert Rules](#alert-rules)
        - [Rollback Plan](#rollback-plan)
      - [New Features](#new-features)
      - [Bug Fixes](#bug-fixes)
    - [v3.6.0](#v360)
    - [v3.2.0](#v320)
  - [Monitor TDengine Database with TDinsight Dashboard](#monitor-tdengine-database-with-tdinsight-dashboard)
  - [Docker Stack](#docker-stack)
  - [Dashboards](#dashboards)

[TDengine] is open-sourced big data platform under GNU AGPL v3.0, designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring, developed by [TDengine](https://tdengine.com/).

[TDengine] data source plugin is developed for [Grafana]. This document explains how to install and configure the data source plugin, and use it as a time-series database. We'll take a look at the data source options, variables, querying, and other options specific to this data source. 

At first, please refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/) for instructions on how to add a data source to Grafana. Note that, only users with the organization admin role can add data sources.

To install this plugin, please refer to [Install the Grafana Plugin for TDengine](https://github.com/taosdata/grafanaplugin/blob/master/INSTALLATION.md)

## Usage

Now it's ready for you to add your own TDengine data source and use it in a dashboard. Refer to [Grafana Datasource documentations topic - Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/) for a quick view. Please make sure the TDengine backend daemon `taosd` and TDengine RESTful service backend daemon `taosadapter` already launched.

### Add Data Source

Point to **Configurations** -> **Data Sources** menu and then **Add data source** button.

![add data source button](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource-button.png)

Search and choose **TDengine**.
![add data source](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource-tdengine.png)

If TDengine is not in the list, please check the installation instructions for allowing loading unsigned plugins.

**Configure TDengine data source for Grafana version 11.**
![data source configuration](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource-11v.png)
Note:
1. Close the Load TDengine Alert button to prevent automatic import of alert rules when adding data sources.
2. When deleting a data source, it is necessary to first clear the imported alert rules

**Configure TDengine data source for Grafana with version lower than 11.**

![data source configuration](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource.png)


Save and test it, it should say 'TDengine Data source is working'.

![data source test](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource-test.png)

### Import Dashboard

Point to **+** / **Create** - **import** (or `/assets/import` url).

![import dashboard and config](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/import_dashboard.png)

Now you can import dashboard with JSON file or grafana dashboard id (please make sure your network is public to <https://grafana.com>).

Here is the first grafana dashboard you want to use for TDengine, the grafana dashboard id is [`18180`](https://grafana.com/grafana/dashboards/18180-tdinsight-for-3-x/).

![import via grafana.com](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/import-dashboard-18180.png)

After import:

![dashboard display](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/TDinsight-v3-full.png)

## Important changes

### [v4.0.0](https://github.com/taosdata/grafanaplugin/releases/tag/v4.0.0) - **Breaking Changes Release**

This version includes **breaking changes** that require action from users. Please read the migration guide carefully before upgrading.

#### Breaking Changes

1. **Removed deprecated query fields**:
   - `alias`: Use SQL `AS` clause instead
   - `colNameFormatStr`: Use Grafana's field display name feature
   - `colNameToGroup`: Use SQL `GROUP BY` with proper column aliases
   - `timeShift`, `timeShiftPeriod`, `timeShiftUnit`: Use Grafana's time range overrides in panel options

2. **QueryEditor simplified**: Removed "Alias By", "Group By Column(s)", and "Group By Format" UI inputs. Query editor now supports SQL-only flow.

3. **Legacy dashboards removed**: TDinsight V2 and Monitor dashboard are removed. Use TDinsightV3 or taosX dashboards instead.

4. **Minimum Grafana version**: Now requires Grafana 8.0+ (previously 7.5+)

#### Migration Guide

##### Dashboard Queries

If your existing dashboards use the removed fields, you need to update them:

**OLD (v3.x)**:
```json
{
  "alias": "My Metric",
  "colNameFormatStr": "location_{{value}}",
  "colNameToGroup": "location",
  "formatType": "Time series"
}
```

**NEW (v4.0.0)**:
```json
{
  "formatType": "Time series"
}
```

Use SQL `AS` clause for aliasing:
```sql
SELECT value AS my_metric FROM ...
```

Use Grafana's field display name override for grouped data:
```json
"fieldConfig": {
  "defaults": {
    "displayName": "prefix_${__field.labels.label_name}",
  },
  "overrides": []
}
```

##### SQL Query Updates

Use the new time macros for better time filtering:

```sql
-- Time filter macro
SELECT * FROM sensors
WHERE $__timeFilter(ts)
GROUP BY tbname;

-- Explicit time range macros
SELECT * FROM sensors
WHERE ts >= $__timeFrom AND ts < $__timeTo;
```

##### Alert Rules

If you have alert rules using the removed fields:
1. Export your alert rules before upgrading (Grafana UI → Alerting → New alert rule → Export)
2. Update queries to remove deprecated fields
3. Re-import alerts using Grafana's alert provisioning API

##### Rollback Plan

If you encounter issues after upgrading:
1. Uninstall plugin v4.0.0
2. Reinstall plugin v3.8.0
3. Your dashboards will work again (but still need migration for future upgrades)

#### New Features

- **Enhanced SQL macros**: Added `$__timeFrom`, `$__timeTo`, `$__timeFilter(column)` for better time range handling

#### Bug Fixes

- Fixed label parsing issue when dimension values contain spaces
- Fixed alert tag extraction for queries with grouped dimensions

---

### [v3.6.0](https://github.com/taosdata/grafanaplugin/releases/tag/v3.6.0)
1. Grafana  11 versions

    The TDengine data source plugin has added functionality for Grafana  11 versions, which can automatically import and clear alerts for basic metrics of the TDengine cluster (such as CPU, memory, dnode, vnode, etc.) when adding data sources.
    ![data source configuration](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/howto-add-datasource-11v.png)
    Note:

    （1）Close the Load TDengine Alert button to prevent automatic import of alert rules when adding data sources.

    （2）When deleting a data source, it is necessary to first clear the imported alarm rules.

    After adding the data source, you will see the automatically imported alert configuration in the alert management menu.
    ![data source configuration](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/alert-rule.png)

2. Grafana 8.0 versions

    The TDengine data source plugin has added functionality for Grafana 8.0 versions, which can automatically import and clear alerts for basic metrics of the TDengine cluster (such as CPU, memory, dnode, vnode, etc.) when adding data sources.

    To import the Dashboard, enter "TDinsight for 3.x Dashboard" and click save. Subsequently, the loaded alert rules will appear in the alert menu as shown below.
    ![Grafana 8.0](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/alert8.png)
    


### [v3.2.0](https://github.com/taosdata/grafanaplugin/releases/tag/v3.2.0)

1. TDengine data source plugin uses secureJsonData to store sensitive data. It will cause a breaking change when you're upgrading from an older version:

    The simple way to migrate from older version is to reconfigure the data source like adding a data source from scratch.

    If you're using Grafana provisioning configurations, you should change the data source provisioning configuration file to use `secureJsonData`:

    ```yaml
    apiVersion: 1
    datasources:
      # <string, required> name of the datasource. Required
    - name: TDengine
      # <string, required> datasource type. Required
      type: tdengine-datasource
      # <string, required> access mode. direct or proxy. Required
      # <int> org id. will default to orgId 1 if not specified
      orgId: 1
      
      # <string> url to TDengine rest api, eg. http://td1:6041
      url: "$TDENGINE_API"

      # <bool> mark as default datasource. Max one per org
      isDefault: true

      # <map> 
      secureJsonData:
        # <string> a redundant url configuration. Required.
        url: "$TDENGINE_API"
        # <string> basic authorization token. Required, can be build like
        #   `printf root:taosdata|base64`
        basicAuth: "${TDENGINE_BASIC_AUTH}"
        # <string> cloud service token of TDengine,  optional.
        token: "$TDENGINE_CLOUD_TOKEN"
   
      version: 1
      # <bool> allow users to edit datasources from the UI.
      editable: true
    ```

2. Now users can quickly import TDinsight dashboard in **Dashboards** tab in a datasource configuration page.

    ![import-tdinsight-from-tdengine-ds](https://raw.githubusercontent.com/taosdata/grafanaplugin/master/assets/import_dashboard-on-datasource.png)

## Monitor TDengine Database with TDinsight Dashboard

TDinsight is a simple monitoring solution for TDengine database. See [TDinsight README](https://github.com/taosdata/grafanaplugin/blob/master/src/dashboards/TDinsightV3.md) for the details.

## Docker Stack

For a quick look and test, you can use `docker-compose` to start a full Grafana + AlertManager + Alert Webhook stack:

```sh
docker-compose up -d
```

Services:

- Grafana: <http://localhost:3000>
- AlertManager: <http://localhost:9093>, in docker it's <http://alertmanager:9010/sms>
- Webhook: <http://localhost:9010/sms>, in docker it's <http://webhook:9010/sms>

## Dashboards

You can get other dashboards in the examples' directory, or search in grafana with TDengine datasource <https://grafana.com/grafana/dashboards/?orderBy=downloads&direction=desc&dataSource=tdengine-datasource> .

Here is a short list:

- [18180](https://grafana.com/grafana/dashboards/18180): TDinsightV3
- [19910](https://grafana.com/grafana/dashboards/19910): TDsmeters
- [20631](https://grafana.com/grafana/dashboards/20631): taosX
  
You could open a pr to add one if you want to share your dashboard with TDengine community, we appreciate your contribution!

[TDengine]: https://github.com/taosdata/TDengine
[Grafana]: https://grafana.com
