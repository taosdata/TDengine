'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.GenericDatasource = undefined;

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var GenericDatasource = exports.GenericDatasource = function () {
  function GenericDatasource(instanceSettings, $q, backendSrv, templateSrv) {
    _classCallCheck(this, GenericDatasource);

    this.type = instanceSettings.type;
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.q = $q;
    this.backendSrv = backendSrv;
    this.templateSrv = templateSrv;
    this.headers = { 'Content-Type': 'application/json' };
    this.headers.Authorization = this.getAuthorization(instanceSettings.jsonData);
  }

  _createClass(GenericDatasource, [{
    key: 'query',
    value: function query(options) {
      var targets = this.buildQueryParameters(options);

      if (targets.length <= 0) {
        return this.q.when({ data: [] });
      }

      return this.doRequest({
        url: this.url + '/grafana/query',
        data: targets,
        method: 'POST'
      });
    }
  }, {
    key: 'testDatasource',
    value: function testDatasource() {
      return this.doRequest({
        url: this.url + '/grafana/heartbeat',
        method: 'GET'
      }).then(function (response) {
        if (response.status === 200) {
          return { status: "success", message: "TDengine Data source is working", title: "Success" };
        }
      });
    }
  }, {
    key: 'doRequest',
    value: function doRequest(options) {
      options.headers = this.headers;

      return this.backendSrv.datasourceRequest(options);
    }
  }, {
    key: 'buildQueryParameters',
    value: function buildQueryParameters(options) {
      var _this = this;

      var targets = _lodash2.default.map(options.targets, function (target) {
        return {
          refId: target.refId,
          alias: _this.generateAlias(options, target),
          sql: _this.generateSql(options, target)
        };
      });

      return targets;
    }
  }, {
    key: 'encode',
    value: function encode(input) {
      var _keyStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
      var output = "";
      var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
      var i = 0;
      while (i < input.length) {
        chr1 = input.charCodeAt(i++);
        chr2 = input.charCodeAt(i++);
        chr3 = input.charCodeAt(i++);
        enc1 = chr1 >> 2;
        enc2 = (chr1 & 3) << 4 | chr2 >> 4;
        enc3 = (chr2 & 15) << 2 | chr3 >> 6;
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
  }, {
    key: 'getAuthorization',
    value: function getAuthorization(jsonData) {
      jsonData = jsonData || {};
      var defaultUser = jsonData.user || "root";
      var defaultPassword = jsonData.password || "taosdata";

      return "Basic " + this.encode(defaultUser + ":" + defaultPassword);
    }
  }, {
    key: 'generateAlias',
    value: function generateAlias(options, target) {
      var alias = target.alias || "";
      alias = this.templateSrv.replace(alias, options.scopedVars, 'csv');
      return alias;
    }
  }, {
    key: 'generateSql',
    value: function generateSql(options, target) {
      var sql = target.sql;
      if (sql == null || sql == "") {
        return sql;
      }

      var queryStart = "now-1h";
      if (options != null && options.range != null && options.range.from != null) {
        queryStart = options.range.from.toISOString();
      }

      var queryEnd = "now";
      if (options != null && options.range != null && options.range.to != null) {
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
  }]);

  return GenericDatasource;
}();
//# sourceMappingURL=datasource.js.map
