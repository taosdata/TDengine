"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9354],{

/***/ 3905:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Zo: () => (/* binding */ MDXProvider),
/* harmony export */   kt: () => (/* binding */ createElement)
/* harmony export */ });
/* unused harmony exports MDXContext, useMDXComponents, withMDXComponents */
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);


function _defineProperty(obj, key, value) {
  if (key in obj) {
    Object.defineProperty(obj, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  } else {
    obj[key] = value;
  }

  return obj;
}

function _extends() {
  _extends = Object.assign || function (target) {
    for (var i = 1; i < arguments.length; i++) {
      var source = arguments[i];

      for (var key in source) {
        if (Object.prototype.hasOwnProperty.call(source, key)) {
          target[key] = source[key];
        }
      }
    }

    return target;
  };

  return _extends.apply(this, arguments);
}

function ownKeys(object, enumerableOnly) {
  var keys = Object.keys(object);

  if (Object.getOwnPropertySymbols) {
    var symbols = Object.getOwnPropertySymbols(object);
    if (enumerableOnly) symbols = symbols.filter(function (sym) {
      return Object.getOwnPropertyDescriptor(object, sym).enumerable;
    });
    keys.push.apply(keys, symbols);
  }

  return keys;
}

function _objectSpread2(target) {
  for (var i = 1; i < arguments.length; i++) {
    var source = arguments[i] != null ? arguments[i] : {};

    if (i % 2) {
      ownKeys(Object(source), true).forEach(function (key) {
        _defineProperty(target, key, source[key]);
      });
    } else if (Object.getOwnPropertyDescriptors) {
      Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
    } else {
      ownKeys(Object(source)).forEach(function (key) {
        Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key));
      });
    }
  }

  return target;
}

function _objectWithoutPropertiesLoose(source, excluded) {
  if (source == null) return {};
  var target = {};
  var sourceKeys = Object.keys(source);
  var key, i;

  for (i = 0; i < sourceKeys.length; i++) {
    key = sourceKeys[i];
    if (excluded.indexOf(key) >= 0) continue;
    target[key] = source[key];
  }

  return target;
}

function _objectWithoutProperties(source, excluded) {
  if (source == null) return {};

  var target = _objectWithoutPropertiesLoose(source, excluded);

  var key, i;

  if (Object.getOwnPropertySymbols) {
    var sourceSymbolKeys = Object.getOwnPropertySymbols(source);

    for (i = 0; i < sourceSymbolKeys.length; i++) {
      key = sourceSymbolKeys[i];
      if (excluded.indexOf(key) >= 0) continue;
      if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue;
      target[key] = source[key];
    }
  }

  return target;
}

var isFunction = function isFunction(obj) {
  return typeof obj === 'function';
};

var MDXContext = /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createContext({});
var withMDXComponents = function withMDXComponents(Component) {
  return function (props) {
    var allComponents = useMDXComponents(props.components);
    return /*#__PURE__*/React.createElement(Component, _extends({}, props, {
      components: allComponents
    }));
  };
};
var useMDXComponents = function useMDXComponents(components) {
  var contextComponents = react__WEBPACK_IMPORTED_MODULE_0__.useContext(MDXContext);
  var allComponents = contextComponents;

  if (components) {
    allComponents = isFunction(components) ? components(contextComponents) : _objectSpread2(_objectSpread2({}, contextComponents), components);
  }

  return allComponents;
};
var MDXProvider = function MDXProvider(props) {
  var allComponents = useMDXComponents(props.components);
  return /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createElement(MDXContext.Provider, {
    value: allComponents
  }, props.children);
};

var TYPE_PROP_NAME = 'mdxType';
var DEFAULTS = {
  inlineCode: 'code',
  wrapper: function wrapper(_ref) {
    var children = _ref.children;
    return /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createElement(react__WEBPACK_IMPORTED_MODULE_0__.Fragment, {}, children);
  }
};
var MDXCreateElement = /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.forwardRef(function (props, ref) {
  var propComponents = props.components,
      mdxType = props.mdxType,
      originalType = props.originalType,
      parentName = props.parentName,
      etc = _objectWithoutProperties(props, ["components", "mdxType", "originalType", "parentName"]);

  var components = useMDXComponents(propComponents);
  var type = mdxType;
  var Component = components["".concat(parentName, ".").concat(type)] || components[type] || DEFAULTS[type] || originalType;

  if (propComponents) {
    return /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createElement(Component, _objectSpread2(_objectSpread2({
      ref: ref
    }, etc), {}, {
      components: propComponents
    }));
  }

  return /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createElement(Component, _objectSpread2({
    ref: ref
  }, etc));
});
MDXCreateElement.displayName = 'MDXCreateElement';
function createElement (type, props) {
  var args = arguments;
  var mdxType = props && props.mdxType;

  if (typeof type === 'string' || mdxType) {
    var argsLength = args.length;
    var createElementArgArray = new Array(argsLength);
    createElementArgArray[0] = MDXCreateElement;
    var newProps = {};

    for (var key in props) {
      if (hasOwnProperty.call(props, key)) {
        newProps[key] = props[key];
      }
    }

    newProps.originalType = type;
    newProps[TYPE_PROP_NAME] = typeof type === 'string' ? type : mdxType;
    createElementArgArray[1] = newProps;

    for (var i = 2; i < argsLength; i++) {
      createElementArgArray[i] = args[i];
    }

    return react__WEBPACK_IMPORTED_MODULE_0__.createElement.apply(null, createElementArgArray);
  }

  return react__WEBPACK_IMPORTED_MODULE_0__.createElement.apply(null, args);
}




/***/ }),

/***/ 8692:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   assets: () => (/* binding */ assets),
/* harmony export */   contentTitle: () => (/* binding */ contentTitle),
/* harmony export */   "default": () => (/* binding */ MDXContent),
/* harmony export */   frontMatter: () => (/* binding */ frontMatter),
/* harmony export */   metadata: () => (/* binding */ metadata),
/* harmony export */   toc: () => (/* binding */ toc)
/* harmony export */ });
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,title:'Data Ingestion'};const contentTitle=undefined;const metadata={"unversionedId":"enterprise/data-in","id":"enterprise/data-in","title":"Data Ingestion","description":"This section describes how to access data from various data sources to TDengine using taosX's command line mode. For command line arguments to taosX, see taosX. You can also use taos-explorer's visual interface for data ingestion, please refer to Visual Management. For service installation and deployment, please refer to Installation and Deployment.","source":"@site/docs/30-enterprise/08-data-in.md","sourceDirName":"30-enterprise","slug":"/enterprise/data-in","permalink":"/docs-en/enterprise/data-in","draft":false,"tags":[],"version":"current","sidebarPosition":8,"frontMatter":{"toc_max_heading_level":4,"title":"Data Ingestion"},"sidebar":"defaultSidebar","previous":{"title":"Permissions Management","permalink":"/docs-en/enterprise/grant"},"next":{"title":"Data Replication","permalink":"/docs-en/enterprise/replication"}};const assets={};const toc=[{value:'OPC-UA',id:'opc-ua',level:2},{value:'Configuration Parameters',id:'configuration-parameters',level:3},{value:'Examples',id:'examples',level:3},{value:'CSV configuration file template',id:'csv-configuration-file-template',level:3},{value:'OPC-DA',id:'opc-da',level:2},{value:'Configuration Parameters',id:'configuration-parameters-1',level:3},{value:'Usage examples',id:'usage-examples',level:3},{value:'Troubleshooting common errors',id:'troubleshooting-common-errors',level:3},{value:'PI',id:'pi',level:2},{value:'PI DSN Configuration',id:'pi-dsn-configuration',level:3},{value:'Usage examples',id:'usage-examples-1',level:3},{value:'Troubleshooting common errors',id:'troubleshooting-common-errors-1',level:3},{value:'InfluxDB',id:'influxdb',level:2},{value:'Command Line Parameters',id:'command-line-parameters',level:3},{value:'Examples',id:'examples-1',level:3},{value:'OpenTSDB',id:'opentsdb',level:2},{value:'Command Line Parameters',id:'command-line-parameters-1',level:3},{value:'Examples',id:'examples-2',level:3},{value:'MQTT',id:'mqtt',level:2},{value:'MQTT DSN Configuration',id:'mqtt-dsn-configuration',level:3},{value:'Interpreter configuration for MQTT connectors',id:'interpreter-configuration-for-mqtt-connectors',level:3},{value:'Example',id:'example',level:3},{value:'Kafka',id:'kafka',level:2},{value:'Command Line Parameters',id:'command-line-parameters-2',level:3},{value:'Kafka DSN configuration',id:'kafka-dsn-configuration',level:3},{value:'Example 1',id:'example-1',level:3},{value:'Example 2',id:'example-2',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This section describes how to access data from various data sources to TDengine using taosX's command line mode. For command line arguments to taosX, see `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../reference/taosx"},`taosX`),`. You can also use taos-explorer's visual interface for data ingestion, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../explorer"},`Visual Management`),`. For service installation and deployment, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../get-started"},`Installation and Deployment`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"opc-ua"},`OPC-UA`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"configuration-parameters"},`Configuration Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Name`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Type`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Description`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`interval`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection interval (seconds), default 1s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`concurrent`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection concurrency, default 1`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`batch_size`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection data points per batch, default 100`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`batch_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for collectors (seconds), default 20s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`connect_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for connections (seconds), default 10s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`request_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for requests (seconds), default 10s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`security_policy`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPC-UA connection security policy (None/Basic128Rsa15/Basic256/Basic256Sha256)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`security_mode`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPC-UA connection mode (None/Sign/SignAndEncrypt)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`certificate`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`cert.pem path Takes effect when connection mode and policy are not none`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`private_key`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`key.pem path Takes effect when connection mode and policy are not none`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`csv_config_file`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Contains OPC UA data point and table configuration. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`ua.nodes`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPC-UA node NodeID. Used in conjunction with opc_table_config configuration, both need to be configured at the same time. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence. Configuration format is <nodeid`,`>`,`::<code`,`>`,`, code is used to build sub-tables.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`opc_table_config`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPCUA single-column table configuration. Must be used with ua.nodes.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`debug`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Whether to enable OPC connector debug logs. The default value is false.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`enable`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Whether to store raw data. The default value is false`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Raw data storage path. Must be configured when enable is set to true.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`keep`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Days to store raw data. Must be configured when enable is set to true.`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Notes:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`opc_table_config:`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`{
    "stable_prefix": "meters", // Supertable prefix
    "column_configs":
    [
        {
            "column_name": "received_time", // Storage received time
            "column_type": "timestamp",
            "column_alias": "ts", // Receive the time to build the table column with the column name ts
            "is_primary_key": true // Receive time timestamp as primary key
        },
        {
            "column_name": "original_time",
            "column_type": "timestamp",
            "column_alias": "ts_2",
            "is_primary_key": false
        },
        {
            "column_name": "value", // Data column
            "column_alias": "valueaa", // Data column alias
            "is_primary_key": false
        },
        {
            "column_name": "quality", // Quality column
            "column_type": "int",
            "column_alias": "quality11", // Quality column alias
            "is_primary_key": false
        }
    ]
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"examples"},`Examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Example configuration using ua.nodes and opc_table_config:
Capture the points with nodeid ns=2;i=2 and ns=2;i=3, write them to the opc library of cluster tdengine with supertable prefixed with meters, and create a supertable of meters_float if the points with ns=2;i=2 are of type float. The supertable uses the data received by opc as the timestamp index column and The super table uses the data received by opc as the timestamp index column and keeps the original timestamp column, the original timestamp column is named ts_2, the data column is stored as valueaa, and the quality data is stored in the quality11 column.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run \\
    -f "opcua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::DSF1312,ns=2;i=3::DSF1313&opc_table_config={\\"stable_prefix\\": \\"meters\\", \\"column_configs\\": [{\\"column_name\\": \\"received_time\\", \\"column_type\\": \\"timestamp\\", \\"column_alias\\": \\"ts\\", \\"is_primary_key\\": true }, {\\"column_name\\": \\"original_time\\", \\"column_type\\": \\"timestamp\\", \\"column_alias\\": \\"ts_2\\", \\"is_primary_key\\": false }, {\\"column_name\\": \\"value\\", \\"column_alias\\": \\"valueaa\\", \\"is_primary_key\\": false }, {\\"column_name\\": \\"quality\\", \\"column_type\\": \\"int\\", \\"column_alias\\": \\"quality11\\", \\"is_primary_key\\": false } ] }" \\
    -t "taos://tdengine:6030/opc"
 


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Using CSV configuration file`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run -f "opcua://<server-info>?csv_config_file=@<file_path>" -t "taos+ws://tdengine:6041/opc"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"csv-configuration-file-template"},`CSV configuration file template`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"opc-da"},`OPC-DA`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"configuration-parameters-1"},`Configuration Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Name`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Type`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Description`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`interval`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection interval (seconds), default 1s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`concurrent`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection concurrency, default 1`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`batch_size`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Collection data points per batch, default 100`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`batch_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for collectors (seconds), default 20s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`connect_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for connections (seconds), default 10s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`request_timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Timeout for requests (seconds), default 10s`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`csv_config_file`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Contains OPC UA data point and table configuration. Either csv_config_file or ua.nodes must be used. CSV Configuration Template Reference: OPC Requirements Summary and Status of Completion`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`da.tags`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPC-UA node NodeID. Used in conjunction with opc_table_config configuration, both need to be configured at the same time. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`opc_table_config`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`OPCUA single-column table configuration. Must be used with da.tags`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`debug`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Whether to enable OPC connector debug logs. The default value is false.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`enable`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Whether to store raw data. The default value is false`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Raw data storage path. Must be configured when enable is set to true.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`keep`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Days to store raw data. Must be configured when enable is set to true.`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"usage-examples"},`Usage examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run \\
    -f "opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int"
    -t "taos://tdengine:6030/opc"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The result of the above example execution:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Capture data from the OPC DA on the Matrikon.OPC.Simulation.1 server with da.tags of Random.Real8 and a data type of int, which corresponds to the creation of a table in TDengine with the table name tb3, the column name c1, and a schema of type int (if the corresponding table already exists, the data is captured directly and written to the table). (if the corresponding table already exists, the data will be collected and written directly).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"troubleshooting-common-errors"},`Troubleshooting common errors`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`(1) If a native connection is used, the task fails to start and reports the following error:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Solution:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Check whether the FQDN of the target TDengine is connected and whether port 6030 can be accessed normally.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`(2) If you use a WebSocket connection the task fails to start and reports the following error:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are several types of errors that can be encountered when connecting using a WebSocket. The error message can be viewed after "Caused by", the following are a few possible errors:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN of the target TDengine cluster is accessible.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"IO error: Connection refused (os error 111)": Port access failed, check if the target port (typically 6041) is configured correctly or is enabled and accessible.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"HTTP error: *": Possible connection to wrong taosAdapter port or LSB/Nginx/Proxy configuration error.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrectly configured port.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"pi"},`PI`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"pi-dsn-configuration"},`PI DSN Configuration`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The PI DSN configuration is as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`pi://[<username>:<password>@]PIServerName/AFDatabaseName?[TemplateForPIPoint][&TemplateForAFElement][&PointList][&<PISystemName=pisys>][&<MaxWaitLen>][&UpdateInterval]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following parameters are supported in the taosX CLI runtime, with at least one of the TemplateForPIPoint, TemplateForAFElement, and PointList parameters configured:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`PISystemName: optional, connection configuration PI system service name, the default value is the same as PIServerName.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`MaxWaitLen: optional, the maximum number of data buffer bars, the default value is 1000, the valid range is `,`[1,10000]`,`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`UpdateInterval: optional, the frequency of data retrieval by PI System, the default value is 10000 (milliseconds: ms), the valid range is `,`[10,600000]`,`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TemplateForPIPoint: optional, use PI Point mode to import templates into TDengine according to each Arrtribution of an element as a sub-table `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TemplateForAFElement: optional, use AF Point mode to import the template into TDengine as a sub-table according to the element's Attribution collection `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`PointList: optional, use PointList mode to import the point information described in the specified csv file in the PI database to TDengine.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"usage-examples-1"},`Usage examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Configure the PI database Met1, template template1, template2 as TemplateForPIPoint mode, template template3, template4 as TemplateForAFElement mode, and the points file points.csv under the path of server WIN-2OA23UM12TN as PointList mode, connect and configure the PI System service name as PI, the maximum buffer bar of data as 1000, and the data fetching frequency of PI System as 10000ms. points.csv under the path of home/ is configured as PointList mode, the connection is configured with the PI System service name PI, the maximum number of data buffer entries is 1000, and the frequency of data fetching by the PI System is 10000ms, and the data in the library is synchronized to the pi library of the server tdengine. The complete example is as follows.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run \\
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \\
    -t "taos://tdengine:6030/pi"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"troubleshooting-common-errors-1"},`Troubleshooting common errors`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`(1) If a native connection is used, the task fails to start and reports the following error:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Solution:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Check whether the FQDN of the target TDengine is connected and whether port 6030 can be accessed normally.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`(2) If you use a WebSocket connection the task fails to start and reports the following error:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are several types of errors that can be encountered when connecting using a WebSocket. The error message can be viewed after "Caused by", the following are a few possible errors:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN of the target TDengine cluster is accessible.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"IO error: Connection refused (os error 111)": Port access failed, check if the target port (typically 6041) is configured correctly or is enabled and accessible.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"HTTP error: *": Possible connection to wrong taosAdapter port or LSB/Nginx/Proxy configuration error.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`"WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrectly configured port.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"influxdb"},`InfluxDB`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"command-line-parameters"},`Command Line Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The command to synchronize data from InfluxDB to TDengine is shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The InfluxDB DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`version: Required, the version of InfluxDB, mainly used to distinguish between 1.x and 2.x versions, which use different authentication parameters;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`version = 1.x`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`username: Required, InfluxDB user that has read access at least in this organization;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`password: Required, the login password for the InfluxDB user;`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`version = 2.x`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`orgId: Required, the Organization ID in InfluxDB;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`token: Mandatory, the API token generated in InfluxDB, this token must have at least the Read permission of the above Bucket;`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`bucket: Required, the name of the Bucket in InfluxDB, only one Bucket can be synchronized at a time;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`measurements: Non-required, you can specify multiple Measurements to be synchronized (English comma-separated), unspecified synchronizes all;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`beginTime: Required, format: YYYY-MM-DD'T'HH:MM:SS'Z', time zone adopts UTC time zone, e.g.: 2023-06-01T00:00:00+0800, i.e. Beijing time 2023-06-01 00:00:00 (East 8 time zone);`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`endTime: Non-required, the field can be left unspecified or the value can be empty, in the same format as beginTime; if unspecified, the data synchronization will continue after the task is submitted;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`readWindow: Non-required, the field can be left unspecified or the value is empty, the options are D, H, M (days, hours, minutes); if not specified, the default is to split the read window by M.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"examples-1"},`Examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Synchronize the data from InfluxDB located at 192.168.1.10, Bucket name test_bucket, starting at 00:00:00 UTC on June 01, 2023, to TDengine's test_db via taoskeeper running on 192.168.1.20 database of TDengine, the complete command is shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`# version = 1.x
taosx run \\
  --from "influxdb+http://192.168.1.10:8086/?version=1.7&username=test&password=123456&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \\
  --to "taos+http://192.168.1.20:6041/test_db" \\
  -vv

# version = 2.x
taosx run \\
  --from "influxdb+http://192.168.1.10:8086/?version=2.7&orgId=3233855dc7e37d8d&token=OZ2sB6Ie6qcKcYAmcHnL-i3STfLVg_IRPQjPIzjsAQ4aUxCWzYhDesNape1tp8IsX9AH0ld41C-clTgo08CGYA==&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \\
  --to "taos+http://192.168.1.20:6041/test_db" \\
  -vv
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In this command, endTime is not specified, so the task will run for a long time, continuously synchronizing the latest data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"opentsdb"},`OpenTSDB`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"command-line-parameters-1"},`Command Line Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The command to synchronize data from OpenTSDB to TDengine is shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`taosx run --from "<OpenTSDB-DSN>" --to "<TDengine-DSN>"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The OpenTSDB DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`metrics: Non-required, you can specify multiple Metrics to be synchronized (English comma-separated), unspecified synchronizes all;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`beginTime: Required, format: YYYY-MM-DD'T'HH:MM:SS'Z', time zone adopts UTC time zone, e.g.: 2023-06-01T00:00:00+0800, i.e. Beijing time 2023-06-01 00:00:00 (East 8 time zone);`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`endTime: Non-required, the field can be left unspecified or the value can be empty, in the same format as beginTime; if unspecified, the data synchronization will continue after the task is submitted;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`readWindow: Non-required, the field can be left unspecified or the value is empty, the options are D, H, M (days, hours, minutes); if not specified, the default is to split the read window by minutes.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"examples-2"},`Examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Synchronize the data from two data sources with metric names test_metric1 and test_metric2 in OpenTSDB located at 192.168.1.10 from 00:00:00 UTC on June 01, 2023 to TDengine's test_db database using taoskeeper running on 192.168.1.20 with the following commands The complete command to synchronize to the test_db database of TDengine is shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`taosx run \\
  --from "opentsdb+http://192.168.1.10:4242/?metrics=test_metric1,test_metric2&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \\
  --to "taos+http://192.168.1.20:6041/test_db" \\
  -vv
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In this command, endTime is not specified, so the task will run for a long time, continuously synchronizing the latest data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"mqtt"},`MQTT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Currently, the MQTT Connector only supports consuming JSON-formatted messages from the MQTT server and synchronizing them to the TDengine. The commands are shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`taosx run --from "<MQTT-DSN>" --to "<TDengine-DSN>" --parser "@<parser-config-file-path>"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The parameters are:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`--from`),` specifies the DSN of the MQTT data source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`--to`),` specifies the DSN of the TDengine cluster`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`--parser`),` specifies a JSON-formatted configuration file that determines how to parse JSON-formatted MQTT messages, as well as the super table names, sub-table names, field names and types, and label names and types when writing to TDengine.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"mqtt-dsn-configuration"},`MQTT DSN Configuration`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`MQTT DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`topics: Mandatory, used to configure the name of the MQTT topic to listen on and the maximum QoS supported by the connector, in the form of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`<topic>::<max-Qos>`),`; multiple topics can be configured, separated by commas; when configuring a topic, you can also use the wildcard characters # and + supported by the MQTT protocol.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`version: Non-required, used to configure the version of MQTT protocol, supported versions include: 3.1/3.1.1/5.0, the default value is 3.1;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`clean_session: Non-required, used to configure the connector as an MQTT client to connect to the MQTT server, the server whether to save the session information, the default value is true, that is, does not save the session information;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`client_id: Mandatory, used to configure the client id of the connector when it connects to the MQTT server as an MQTT client.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`keep_alive: Non-required, used to configure the waiting time after the connector, as an MQTT client, sends a PINGREG message to the MQTT server, if the connector does not receive a PINGREQ message from the MQTT server within this time, the connector will actively disconnect; the unit of this configuration is seconds, and the default value is 60.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`ca: Non-required, used to specify the CA certificate to be used when the connector establishes an SSL/TLS connection with the MQTT server, and its value is @ in front of the absolute path of the certificate file, for example: @/home/admin/certs/ca.crt.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`cert: Non-required, used to specify the client certificate to be used when the connector establishes an SSL/TLS connection with the MQTT server, whose value is @ in front of the absolute path of the certificate file, for example: @/home/admin/certs/client.crt.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`cert_key: Non-required, used to specify the client's private key to be used when the connector establishes an SSL/TLS connection with the MQTT server, whose value is @ in front of the absolute path of the private key file, for example: @/home/admin/certs/client.key.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`log_level: Non-required, used to configure the logging level of the connector, the connector supports error/warn/info/debug/trace 5 logging levels, the default value is info.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A sample MQTT description string is as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`mqtt://<username>:<password>@<mqtt-broker-ip>:8883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=taosdata_1234&keep_alive=60&ca=@/home/admin/certs/ca.crt&cert=@/home/admin/certs/client.crt&cert_key=@/home/admin/certs/client.key
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"interpreter-configuration-for-mqtt-connectors"},`Interpreter configuration for MQTT connectors`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The connector's interpreter configuration file, the parameter to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`--parser`),` configuration item, which takes the value of a JSON file, can be configured in two parts, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`parse`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`model`),`, as shown in the template below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`{
  "parse": {
    "payload": {
      "json": [
        {
          "name": "ts",
          "alias": "ts",
          "cast": "TIMESTAMP"
        },
        ...
      ]
    }
  },
  "model": {
    "using": "<stable-name>",
    "name": "<subtable-prefix>{alias}",
    "columns": [ ... ],
    "tags": [ ... ]
  }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The fields are described below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The parse section currently supports only one type of payload, json, where the value of the json field is a JSON Array consisting of a JSON Object.`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Each JSON Object consists of three fields: name, alias, cast;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The name field specifies how to extract the field from the MQTT message. If the MQTT message is a simple JSON Object, you can set the field name here; if the MQTT message is a complex JSON Object, you can use a JSON Path to extract the field, e.g. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`$.data.city`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The alias field is used to name the name that will be used after the fields in the MQTT message are synchronized to the TDengine;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The cast field is used to specify the type of field used in the MQTT message after it is synchronized to the TDengine.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The model section is used to set up information about the TDengine super table, sub-tables, columns and labels:`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The using field is used to specify the super table name;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The name field is used to specify the name of the sub-table, and its value can be divided into two parts: the prefix and the variable, the variable is the value of the alias set in the parse part, and you need to use {}, for example: d{id};`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The columns field is used to set which fields in the MQTT message are to be used as columns in the TDengine super table, and takes the value of the alias set in the parse section; note that the order here determines the order of the columns in the TDengine super table, and therefore the first column must be of type TIMESTAMP;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The tags field is used to set which fields in the MQTT message are used as tags in the TDengine super table, and takes the value of the alias set in the parse section.`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"example"},`Example`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There is an MQTT broker running on port 1883 of 192.168.1.10, with usernames and passwords admin, 123456; we want to synchronize the messages from it to TDengine's test database via taosadapter running on 192.168.1.20. The MQTT message format is:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`{
  "id": 1,
  "current": 10.77,
  "voltage": 222,
  "phase": 0.77,
  "groupid": 7,
  "location": "California.SanDiego"
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`When MQTT messages are synchronized to TDengine, if you use meters as the name of the super table, prefix "d" to concatenate the value of id field as the name of the sub-table, ts, id, current, voltage, phase as the columns of the super table, and groupid, location as the labels of the super table, the interpreter is configured as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`{
  "parse": {
    "payload": {
      "json": [
        {
          "name": "ts",
          "alias": "ts",
          "cast": "TIMESTAMP"
        },
        {
          "name": "id",
          "alias": "id",
          "cast": "INT"
        },
        {
          "name": "voltage",
          "alias": "voltage",
          "cast": "INT"
        },
        {
          "name": "phase",
          "alias": "phase",
          "cast": "FLOAT"
        },
        {
          "name": "current",
          "alias": "current",
          "cast": "FLOAT"
        },
        {
          "name": "groupid",
          "alias": "groupid",
          "cast": "INT"
        },
        {
          "name": "location",
          "alias": "location",
          "cast": "VARCHAR(20)"
        }
      ]
    }
  },
  "model": {
    "name": "d{id}",
    "using": "meters",
    "columns": [
      "ts",
      "id",
      "current",
      "voltage",
      "phase"
    ],
    "tags": [
      "groupid",
      "location"
    ]
  }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the above parser configuration is located in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/home/admin/parser.json`),`, then the full command is shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`taosx run \\
  -f "mqtt://admin:123456@192.168.1.10:1883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=1234&keep_alive=60" \\
  -t "taos+ws://192.168.1.20:6041/test"
  --parser "@/home/admin/parser.json"
  --verbose
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"kafka"},`Kafka`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"command-line-parameters-2"},`Command Line Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`taosx supports consuming data from Kafka and writing to TDengine. The configuration file is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`or`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>" --parser "@<parser-config-file-path>"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The parameters are:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`-f or --from: Kafka DSN`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`-t or --to: TDengine DSN`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`--parser: JSON configuration file or string`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"kafka-dsn-configuration"},`Kafka DSN configuration`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Parameter`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Description`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Mandatory`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Default`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Used On`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Example`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`group`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Consumer group. Can be an empty string; in this case the consumer generated has no group`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`""`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`topics`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Specify the topic to consume. All available partitions for the specified topic will be used unless overridden when topic_partitions is specified.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`This parameter or topic_partitions must specify at least one in order for topics to be assigned to consumers.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`None`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`topics=tp1,tp2`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`topic_partitions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Explicitly specify the topic partition to be used. Use only the specified partition with the topic identified.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`This parameter or topics must specify at least one in order for the topic to be assigned to the consumer.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`None`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`topic_partitions=tp1:0..2,tp2:1`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`fallback_offset`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Possible values at topic offset: - Earliest: receive the earliest available offset; - Latest: receive the most recent offset; - ByTime(i64): used to request all messages up to a specific time (ms); Unix timestamps (milliseconds)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Earliest`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`fallback_offset=Earliest`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`offset_storage`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Defines the available storage to use when fetching or committing group offsets: - Zookeeper: Zookeeper-based storage (available since kafka 0.8.1); - Kafka: Kafka-based storage (available since Kafka 0.8.2). This is the preferred method for groups to store their offsets.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Kafka`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`offset_storage=Kafka`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`timeout`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`When subscribing to data from kafka, if no valid data is fetched after the timeout, exit`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`500`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`timeout=never`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`use_ssl`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Whether to use SSL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`cert`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SSL certificate path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`cert_key`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SSL certificate key path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`No`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null})))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"example-1"},`Example 1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Consume data from a Kafka instance on the 192.168.1.92 server and synchronize it to the TDengine on 192.168.1.92 without using a parser.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`kafka`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`#!/bin/bash
KAFKA_HOME=/root/zyyang/kafka_2.13-3.1.0
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --delete
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --delete
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --partitions 5 --replication-factor 1 --create
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --partitions 1 --replication-factor 1 --create
$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 << EOF
{"id": 1, "message": "hello"}
{"id": 2, "message": "hello"}
{"id": 3, "message": "hello"}
{"id": 4, "message": "hello"}
{"id": 5, "message": "hello"}
EOF
$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 << EOF
{"id": 1, "message": "aaa"}
{"id": 2, "message": "aaa"}
{"id": 3, "message": "aaa"}
{"id": 4, "message": "aaa"}
{"id": 5, "message": "aaa"}
EOF
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --describe
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --describe
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`TDengine`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`drop database if exists kafka_to_taos;
create database if not exists kafka_to_taos precision 'ms';
use kafka_to_taos;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`taosx`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.1.92:6030/kafka_to_taos" --parser "{\\"parse\\":{\\"ts\\":{\\"as\\":\\"timestamp(ms)\\"},\\"topic\\":{\\"as\\":\\"varchar\\",\\"alias\\":\\"t\\"},\\"partition\\":{\\"as\\":\\"int\\",\\"alias\\":\\"p\\"},\\"offset\\":{\\"as\\":\\"bigint\\",\\"alias\\":\\"o\\"},\\"key\\":{\\"as\\":\\"binary\\",\\"alias\\":\\"k\\"},\\"value\\":{\\"as\\":\\"binary\\",\\"alias\\":\\"v\\"}},\\"model\\":[{\\"name\\":\\"t_{t}\\",\\"using\\":\\"kafka_data\\",\\"tags\\":[\\"t\\",\\"p\\"],\\"columns\\":[\\"ts\\",\\"o\\",\\"k\\",\\"v\\"]}]}"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"example-2"},`Example 2`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Consume data from the Kafka instance on the 192.168.1.92 server, synchronize it to the TDengine on 192.168.1.92, and use parser to parse the JSON data in the value.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`kafka, same as example 1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`TDengine, same as example 1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Taosx`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.0.201:6030/kafka_to_taos" --parser "{\\"parse\\":{\\"ts\\":{\\"as\\":\\"timestamp(ms)\\"},\\"topic\\":{\\"as\\":\\"varchar\\",\\"alias\\":\\"t\\"},\\"partition\\":{\\"as\\":\\"int\\",\\"alias\\":\\"p\\"},\\"offset\\":{\\"as\\":\\"bigint\\",\\"alias\\":\\"o\\"},\\"value\\":{\\"json\\":[\\"id::int\\",\\"message::binary\\"]}},\\"model\\":[{\\"name\\":\\"t_{t}\\",\\"using\\":\\"kafka_data\\",\\"tags\\":[\\"t\\",\\"p\\"],\\"columns\\":[\\"ts\\",\\"o\\",\\"id\\",\\"message\\"]}]}"
`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);