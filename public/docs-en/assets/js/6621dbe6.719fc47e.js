"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1713],{

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

/***/ 7492:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  assets: () => (/* binding */ assets),
  contentTitle: () => (/* binding */ _03_telegraf_contentTitle),
  "default": () => (/* binding */ _03_telegraf_MDXContent),
  frontMatter: () => (/* binding */ _03_telegraf_frontMatter),
  metadata: () => (/* binding */ metadata),
  toc: () => (/* binding */ _03_telegraf_toc)
});

// EXTERNAL MODULE: ./node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(7462);
// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/@mdx-js/react/dist/esm.js
var esm = __webpack_require__(3905);
;// CONCATENATED MODULE: ./docs/14-reference/_telegraf.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`In the Telegraf configuration file (default location `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`/etc/telegraf/telegraf.conf`),`) add an `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`outputs.http`),` section.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`[[outputs.http]]
  url = "http://<taosAdapter's host>:<REST service port>/influxdb/v1/write?db=<database name>"
  ...
  username = "<TDengine's username>"
  password = "<TDengine's password>"
  ...
`)),(0,esm/* mdx */.kt)("p",null,`Where <taosAdapter's host`,`>`,` please fill in the server's domain name or IP address running the taosAdapter service. <REST service port`,`>`,` please fill in the port of the REST service (default is 6041). <TDengine's username`,`>`,` and <TDengine's password`,`>`,` please fill in the actual configuration of the currently running TDengine. And <database name`,`>`,` please fill in the database name where you want to store Telegraf data in TDengine.`),(0,esm/* mdx */.kt)("p",null,`An example is as follows.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`[[outputs.http]]
  url = "http://127.0.0.1:6041/influxdb/v1/write?db=telegraf"
  method = "POST"
  timeout = "5s"
  username = "root"
  password = "taosdata"
  data_format = "influx"
`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/20-third-party/03-telegraf.md
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _03_telegraf_frontMatter={title:'Telegraf writing',sidebar_label:'Telegraf',description:'This document describes how to integrate TDengine with Telegraf.'};const _03_telegraf_contentTitle=undefined;const metadata={"unversionedId":"third-party/telegraf","id":"third-party/telegraf","title":"Telegraf writing","description":"This document describes how to integrate TDengine with Telegraf.","source":"@site/docs/20-third-party/03-telegraf.md","sourceDirName":"20-third-party","slug":"/third-party/telegraf","permalink":"/docs-en/third-party/telegraf","draft":false,"tags":[],"version":"current","sidebarPosition":3,"frontMatter":{"title":"Telegraf writing","sidebar_label":"Telegraf","description":"This document describes how to integrate TDengine with Telegraf."},"sidebar":"defaultSidebar","previous":{"title":"Prometheus","permalink":"/docs-en/third-party/prometheus"},"next":{"title":"collectd","permalink":"/docs-en/third-party/collectd"}};const assets={};const _03_telegraf_toc=[{value:'Prerequisites',id:'prerequisites',level:2},{value:'Configuration steps',id:'configuration-steps',level:2},{value:'Verification method',id:'verification-method',level:2}];const _03_telegraf_layoutProps={toc: _03_telegraf_toc};const _03_telegraf_MDXLayout="wrapper";function _03_telegraf_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_03_telegraf_MDXLayout,(0,esm_extends/* default */.Z)({},_03_telegraf_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`Telegraf is a viral, open-source, metrics collection software. Telegraf can collect the operation information of various components without having to write any scripts to collect regularly, reducing the difficulty of data acquisition.`),(0,esm/* mdx */.kt)("p",null,`Telegraf's data can be written to TDengine by simply adding the output configuration of Telegraf to the URL corresponding to taosAdapter and modifying several configuration items. The presence of Telegraf data in TDengine can take advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.`),(0,esm/* mdx */.kt)("h2",{"id":"prerequisites"},`Prerequisites`),(0,esm/* mdx */.kt)("p",null,`To write Telegraf data to TDengine requires the following preparations.`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`The TDengine cluster is deployed and functioning properly`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`taosAdapter is installed and running properly. Please refer to the `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/taosadapter"},`taosAdapter manual`),` for details.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Telegraf has been installed. Please refer to the `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.22/install/"},`official documentation`),` for Telegraf installation.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Telegraf collects the running status measurements of current system. You can enable `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.22/plugins/"},`input plugins`),` to insert `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.24/data_formats/input/"},`other formats`),` data to Telegraf then forward to TDengine.`)),(0,esm/* mdx */.kt)("h2",{"id":"configuration-steps"},`Configuration steps`),(0,esm/* mdx */.kt)(MDXContent,{mdxType:"Telegraf"}),(0,esm/* mdx */.kt)("h2",{"id":"verification-method"},`Verification method`),(0,esm/* mdx */.kt)("p",null,`Restart Telegraf service:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`sudo systemctl restart telegraf
`)),(0,esm/* mdx */.kt)("p",null,`Use TDengine CLI to verify Telegraf correctly writing data to TDengine and read out:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 telegraf                       |
Query OK, 3 rows in database (0.010568s)

taos> use telegraf;
Database changed.

taos> show stables;
              name              |
=================================
 swap                           |
 cpu                            |
 system                         |
 diskio                         |
 kernel                         |
 mem                            |
 processes                      |
 disk                           |
Query OK, 8 row(s) in set (0.000521s)

taos> select * from telegraf.system limit 10;
              ts               |           load1           |           load5           |          load15           |        n_cpus         |        n_users        |        uptime         | uptime_format |              host
|
=============================================================================================================================================================================================================================================
 2022-04-20 08:47:50.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5533 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:00.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5543 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:10.000000000 |               0.000000000 |               0.040000000 |               0.070000000 |                     4 |                     1 |                  5553 |  1:32         | shuduo-1804
|
Query OK, 3 row(s) in set (0.013269s)
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("ul",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},`TDengine take influxdb format data and create unique ID for table names by the rule.
The user can configure `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`smlChildTableName`),` parameter to generate specified table names if he/she needs. And he/she also need to insert data with specified data format.
For example, Add `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`smlChildTableName=tname`),` in the taos.cfg file. Insert data `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`st,tname=cpu1,t1=4 c1=3 1626006833639000000`),` then the table name will be cpu1. If there are multiple lines has same tname but different tag_set, the first line's tag_set will be used to automatically creating table and ignore other lines. Please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/schemaless/#Schemaless-Line-Protocol"},`TDengine Schemaless`)))));};_03_telegraf_MDXContent.isMDXComponent=true;

/***/ })

}]);