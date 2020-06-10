import _ from "lodash";

export class GenericDatasource {

  constructor(instanceSettings, $q, backendSrv, templateSrv) {
    this.type = instanceSettings.type;
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.q = $q;
    this.backendSrv = backendSrv;
    this.templateSrv = templateSrv;
    this.headers = {'Content-Type': 'application/json'};
    this.headers.Authorization = this.getAuthorization(instanceSettings.jsonData);
  }

  query(options) {
    var targets = this.buildQueryParameters(options);

    if (targets.length <= 0) {
      return this.q.when({data: []});
    }

    return this.doRequest({
      url: this.url + '/grafana/query',
      data: targets,
      method: 'POST'
    });
  }

  testDatasource() {
    return this.doRequest({
      url: this.url + '/grafana/heartbeat',
      method: 'GET',
    }).then(response => {
      if (response.status === 200) {
        return { status: "success", message: "TDengine Data source is working", title: "Success" };
      }
    });
  }

  doRequest(options) {
    options.headers = this.headers;

    return this.backendSrv.datasourceRequest(options);
  }

  buildQueryParameters(options) {

    var targets = _.map(options.targets, target => {
      return {
        refId: target.refId,
        alias: this.generateAlias(options, target),
        sql: this.generateSql(options, target)
      };
    });

    return targets;
  }

  encode(input) {
    var _keyStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    var output = "";
    var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    var i = 0;
    while (i < input.length) {
      chr1 = input.charCodeAt(i++);
      chr2 = input.charCodeAt(i++);
      chr3 = input.charCodeAt(i++);
      enc1 = chr1 >> 2;
      enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
      enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
      enc4 = chr3 & 63;
      if (isNaN(chr2)) {
        enc3 = enc4 = 64;
      } else if (isNaN(chr3)) {
        enc4 = 64;
      }
      output = output + _keyStr.charAt(enc1) + _keyStr.charAt(enc2) + _keyStr.charAt(enc3) + _keyStr.charAt(enc4);
    }

    return output;
  }

  getAuthorization(jsonData){
    jsonData = jsonData || {};
    var defaultUser = jsonData.user || "root";
    var defaultPassword = jsonData.password || "taosdata";

    return "Basic " + this.encode(defaultUser + ":" + defaultPassword);
  }

  generateAlias(options, target){
    var alias = target.alias || "";
    alias = this.templateSrv.replace(alias, options.scopedVars, 'csv');
    return alias;
  }

  generateSql(options, target) {
    var sql = target.sql;
    if (sql == null || sql == ""){
      return sql;
    }

    var queryStart = "now-1h";
    if (options != null && options.range != null && options.range.from != null){
      queryStart = options.range.from.toISOString();
    }

    var queryEnd = "now";
    if (options != null && options.range != null && options.range.to != null){
      queryEnd = options.range.to.toISOString();
    }
    var intervalMs = Math.max(options.intervalMs,15000);
    if (isNaN(intervalMs)) {
      intervalMs = 15000;
    }
    intervalMs += "a";
    sql = sql.replace(/^\s+|\s+$/gm, '');
    sql = sql.replace("$from", "'" + queryStart + "'");
    sql = sql.replace("$begin", "'" + queryStart + "'");
    sql = sql.replace("$to", "'" + queryEnd + "'");
    sql = sql.replace("$end", "'" + queryEnd + "'");
    sql = sql.replace("$interval", intervalMs);

    sql = this.templateSrv.replace(sql, options.scopedVars, 'csv');
    return sql;
  }

}