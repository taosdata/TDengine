'use strict';

System.register(['lodash'], function (_export, _context) {
  "use strict";
  var _, _createClass, GenericDatasource;
  
  function strTrim(str) {
     return str.replace(/^\s+|\s+$/gm,'');
  }

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }],
    execute: function () {
      _createClass = function () {
        function defineProperties(target, props) {
          for (var i = 0; i < props.length; i++) {
            var descriptor = props[i];
            descriptor.enumerable = descriptor.enumerable || false;
            descriptor.configurable = true;
            if ("value" in descriptor) descriptor.writable = true;
            Object.defineProperty(target, descriptor.key, descriptor);
          }
        }

        return function (Constructor, protoProps, staticProps) {
          if (protoProps) defineProperties(Constructor.prototype, protoProps);
          if (staticProps) defineProperties(Constructor, staticProps);
          return Constructor;
        };
      }();

      _export('GenericDatasource', GenericDatasource = function () {
        function GenericDatasource(instanceSettings, $q, backendSrv, templateSrv) {
          _classCallCheck(this, GenericDatasource);
          
          this.type = instanceSettings.type;
          this.url = instanceSettings.url;
          this.name = instanceSettings.name;
          this.q = $q;
          this.backendSrv = backendSrv;
          this.templateSrv = templateSrv;
          //this.withCredentials = instanceSettings.withCredentials;
          this.headers = { 'Content-Type': 'application/json' };
          var taosuser = instanceSettings.jsonData.user;
          var taospwd = instanceSettings.jsonData.password;
          if (taosuser == null || taosuser == undefined || taosuser == "") {
            taosuser = "root";
          }
          if (taospwd == null || taospwd == undefined || taospwd == "") {
            taospwd = "taosdata";
          }

          this.headers.Authorization = "Basic " + this.encode(taosuser + ":" + taospwd);
        }

        _createClass(GenericDatasource, [{
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
        }, {
          key: 'generateSql',
          value: function generateSql(sql, queryStart, queryEnd, intervalMs) {
            if (queryStart == undefined || queryStart == null) {
                queryStart = "now-1h";
            }
            if (queryEnd == undefined || queryEnd == null) {
                queryEnd = "now";
            }
            if (intervalMs == undefined || intervalMs == null) {
                intervalMs = "20000";
            }
            
            intervalMs += "a";
            sql = sql.replace(/^\s+|\s+$/gm, '');
            sql = sql.replace("$from", "'" + queryStart + "'");
            sql = sql.replace("$begin", "'" + queryStart + "'");
            sql = sql.replace("$to", "'" + queryEnd + "'");
            sql = sql.replace("$end", "'" + queryEnd + "'");
            sql = sql.replace("$interval", intervalMs);
            
            return sql;
          }
        }, {
          key: 'query',
          value: function query(options) {
            var querys = new Array;
            for (var i = 0; i < options.targets.length; ++i) {
                var query = new Object;
                
                query.refId = options.targets[i].refId;
                query.alias = options.targets[i].alias;
                if (query.alias == null || query.alias == undefined) {
                    query.alias = "";
                }
                
                //query.sql = this.generateSql(options.targets[i].sql, options.range.raw.from, options.range.raw.to, options.intervalMs);
                query.sql = this.generateSql(options.targets[i].sql, options.range.from.toISOString(), options.range.to.toISOString(), options.intervalMs);
                console.log(query.sql);
                
                querys.push(query);
            }
            
            if (querys.length <= 0) {
              return this.q.when({ data: [] });
            }

            return this.doRequest({
              url: this.url + '/grafana/query',
              data: querys,
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
            //console.log(options);
            return this.backendSrv.datasourceRequest(options);
          }
        }]);

        return GenericDatasource;
      }());

      _export('GenericDatasource', GenericDatasource);
    }
  };
});
//# sourceMappingURL=datasource.js.map
