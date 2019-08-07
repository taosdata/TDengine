/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

char* httpMsg[] = {
    "success",                                 // 0
    "http server is not online",               // 1
    "http url is not support",                 // 2
    "http method parse error",                 // 3
    "http version should be 1.0, 1.1 or 1.2",  // 4
    "http head parse error",                   // 5
    "request size is too big",
    "http body size invalid",
    "http chunked body parse error",           // 8
    "http url parse error",                    // 9
    "invalid type of Authorization",
    "no auth info input",
    "no sql input",
    "session list was full",
    "no enough memory to alloc sqls",
    "generate taosd token error",
    "db and table can not be null",
    "no need to execute use db cmd",
    "parse grafana json error",
    "size of multi request is 0",              // 19
    "request is empty",                        // 20
    "no enough connections for http",          // 21

    // telegraf
    "database name can not be null",           // 22
    "database name too long",
    "invalid telegraf json fromat",
    "metrics size is 0",
    "metrics size can not more than 1K",       // 26
    "metric name not find",
    "metric name type should be string",
    "metric name length is 0",
    "metric name length too long",
    "timestamp not find",                      // 31
    "timestamp type should be integer",
    "timestamp value smaller than 0",
    "tags not find",
    "tags size is 0",
    "tags size too long",                      // 36
    "tag is null",
    "tag name is null",
    "tag name length too long",                // 39
    "tag value type should be number or string",
    "tag value is null",
    "table is null",                           // 42
    "table name length too long",
    "fields not find",                         // 44
    "fields size is 0",
    "fields size too long",
    "field is null",                           // 47
    "field name is null",
    "field name length too long",              // 49
    "field value type should be number or string",
    "field value is null",                     // 51
    "parse basic auth token error",
    "parse taosd auth token error",
    "host type should be string",

    // grafana
    "query size is 0",                        // 55
    "query size can not more than 100",

    // opentsdb
    "database name can not be null",          // 57
    "database name too long",
    "invalid opentsdb json fromat",           // 59
    "metrics size is 0",
    "metrics size can not more than 10K",     // 61
    "metric name not find",
    "metric name type should be string",
    "metric name length is 0",
    "metric name length can not more than 22",
    "timestamp not find",
    "timestamp type should be integer",
    "timestamp value smaller than 0",
    "tags not find",
    "tags size is 0",
    "tags size too long",                    // 71
    "tag is null",
    "tag name is null",
    "tag name length too long",              // 74
    "tag value type should be boolean, number or string",
    "tag value is null",
    "tag value can not more than 64",        // 77
    "value not find",
    "value type should be boolean, number or string",
    "stable not exist",

};
