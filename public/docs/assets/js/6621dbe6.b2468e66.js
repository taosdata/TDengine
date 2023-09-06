"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1713],{

/***/ 3905:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Zo": () => (/* binding */ MDXProvider),
/* harmony export */   "kt": () => (/* binding */ createElement)
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
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _03_telegraf_contentTitle),
  "default": () => (/* binding */ _03_telegraf_MDXContent),
  "frontMatter": () => (/* binding */ _03_telegraf_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _03_telegraf_toc)
});

// EXTERNAL MODULE: ./node_modules/@docusaurus/core/node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(3117);
// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/@mdx-js/react/dist/esm.js
var esm = __webpack_require__(3905);
;// CONCATENATED MODULE: ./docs/14-reference/_telegraf.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`在 Telegraf 配置文件（默认位置 /etc/telegraf/telegraf.conf） 增加 outputs.http 输出模块配置：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`[[outputs.http]]
  url = "http://<taosAdapter's host>:<REST service port>/influxdb/v1/write?db=<database name>"
  ...
  username = "<TDengine's username>"
  password = "<TDengine's password>"
  ...
`)),(0,esm/* mdx */.kt)("p",null,`其中 <taosAdapter's host`,`>`,` 请填写运行 taosAdapter 服务的服务器域名或 IP 地址，<REST service port`,`>`,` 请填写 REST 服务的端口（默认为 6041），<TDengine's username`,`>`,` 和 <TDengine's password`,`>`,` 请填写当前运行的 TDengine 实际配置，<database name`,`>`,` 请填写希望在 TDengine 保存 Telegraf 数据的数据库名。`),(0,esm/* mdx */.kt)("p",null,`示例如下：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`[[outputs.http]]
  url = "http://127.0.0.1:6041/influxdb/v1/write?db=telegraf"
  method = "POST"
  timeout = "5s"
  username = "root"
  password = "taosdata"
  data_format = "influx"
`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/20-third-party/03-telegraf.md
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _03_telegraf_frontMatter={sidebar_label:'Telegraf',title:'Telegraf 写入',description:'使用 Telegraf 向 TDengine 写入数据'};const _03_telegraf_contentTitle=undefined;const metadata={"unversionedId":"third-party/telegraf","id":"third-party/telegraf","title":"Telegraf 写入","description":"使用 Telegraf 向 TDengine 写入数据","source":"@site/docs/20-third-party/03-telegraf.md","sourceDirName":"20-third-party","slug":"/third-party/telegraf","permalink":"/docs/third-party/telegraf","draft":false,"tags":[],"version":"current","sidebarPosition":3,"frontMatter":{"sidebar_label":"Telegraf","title":"Telegraf 写入","description":"使用 Telegraf 向 TDengine 写入数据"},"sidebar":"defaultSidebar","previous":{"title":"Prometheus","permalink":"/docs/third-party/prometheus"},"next":{"title":"collectd","permalink":"/docs/third-party/collectd"}};const assets={};const _03_telegraf_toc=[{value:'前置条件',id:'前置条件',level:2},{value:'配置步骤',id:'配置步骤',level:2},{value:'验证方法',id:'验证方法',level:2}];const _03_telegraf_layoutProps={toc: _03_telegraf_toc};const _03_telegraf_MDXLayout="wrapper";function _03_telegraf_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_03_telegraf_MDXLayout,(0,esm_extends/* default */.Z)({},_03_telegraf_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。`),(0,esm/* mdx */.kt)("p",null,`只需要将 Telegraf 的输出配置增加指向 taosAdapter 对应的 url 并修改若干配置项即可将 Telegraf 的数据写入到 TDengine 中。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。`),(0,esm/* mdx */.kt)("h2",{"id":"前置条件"},`前置条件`),(0,esm/* mdx */.kt)("p",null,`要将 Telegraf 数据写入 TDengine 需要以下几方面的准备工作。`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`TDengine 集群已经部署并正常运行`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`taosAdapter 已经安装并正常运行。具体细节请参考 `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/taosadapter"},`taosAdapter 的使用手册`)),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Telegraf 已经安装。安装 Telegraf 请参考`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.22/install/"},`官方文档`)),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Telegraf 默认采集系统运行状态数据。通过使能`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.22/plugins/"},`输入插件`),`方式可以输出`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://docs.influxdata.com/telegraf/v1.24/data_formats/input/"},`其他格式`),`的数据到 Telegraf 再写入到 TDengine中。`)),(0,esm/* mdx */.kt)("h2",{"id":"配置步骤"},`配置步骤`),(0,esm/* mdx */.kt)(MDXContent,{mdxType:"Telegraf"}),(0,esm/* mdx */.kt)("h2",{"id":"验证方法"},`验证方法`),(0,esm/* mdx */.kt)("p",null,`重启 Telegraf 服务：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`sudo systemctl restart telegraf
`)),(0,esm/* mdx */.kt)("p",null,`使用 TDengine CLI 验证从 Telegraf 向 TDengine 写入数据并能够正确读出：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> show databases;
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
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("ul",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},`TDengine 接收 influxdb 格式数据默认生成的子表名是根据规则生成的唯一 ID 值。
用户如需指定生成的表名，可以通过在 taos.cfg 里配置 smlChildTableName 参数来指定。如果通过控制输入数据格式，即可利用 TDengine 这个功能指定生成的表名。
举例如下：配置 smlChildTableName=tname 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的表名为 cpu1。如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/schemaless/#%E6%97%A0%E6%A8%A1%E5%BC%8F%E5%86%99%E5%85%A5%E8%A1%8C%E5%8D%8F%E8%AE%AE"},`TDengine 无模式写入参考指南`)))));};_03_telegraf_MDXContent.isMDXComponent=true;

/***/ })

}]);