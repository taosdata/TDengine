"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9115],{

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

/***/ 5189:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "assets": () => (/* binding */ assets),
/* harmony export */   "contentTitle": () => (/* binding */ contentTitle),
/* harmony export */   "default": () => (/* binding */ MDXContent),
/* harmony export */   "frontMatter": () => (/* binding */ frontMatter),
/* harmony export */   "metadata": () => (/* binding */ metadata),
/* harmony export */   "toc": () => (/* binding */ toc)
/* harmony export */ });
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={sidebar_label:'流式计算',description:'TDengine 流式计算将数据的写入、预处理、复杂分析、实时计算、报警触发等功能融为一体，是一个能够降低用户部署成本、存储成本和运维成本的计算引擎。',title:'流式计算'};const contentTitle=undefined;const metadata={"unversionedId":"develop/stream","id":"develop/stream","title":"流式计算","description":"TDengine 流式计算将数据的写入、预处理、复杂分析、实时计算、报警触发等功能融为一体，是一个能够降低用户部署成本、存储成本和运维成本的计算引擎。","source":"@site/docs/07-develop/06-stream.md","sourceDirName":"07-develop","slug":"/develop/stream","permalink":"/docs/develop/stream","draft":false,"tags":[],"version":"current","sidebarPosition":6,"frontMatter":{"sidebar_label":"流式计算","description":"TDengine 流式计算将数据的写入、预处理、复杂分析、实时计算、报警触发等功能融为一体，是一个能够降低用户部署成本、存储成本和运维成本的计算引擎。","title":"流式计算"},"sidebar":"defaultSidebar","previous":{"title":"查询数据","permalink":"/docs/develop/query-data/"},"next":{"title":"数据订阅","permalink":"/docs/develop/tmq"}};const assets={};const toc=[{value:'流式计算的创建',id:'流式计算的创建',level:2},{value:'示例一',id:'示例一',level:2},{value:'创建 DB 和原始数据表',id:'创建-db-和原始数据表',level:3},{value:'创建流',id:'创建流',level:3},{value:'写入数据',id:'写入数据',level:3},{value:'查询以观察结果',id:'查询以观察结果',level:3},{value:'示例二',id:'示例二',level:2},{value:'创建 DB 和原始数据表',id:'创建-db-和原始数据表-1',level:3},{value:'创建流',id:'创建流-1',level:3},{value:'写入数据',id:'写入数据-1',level:3},{value:'查询以观察结果',id:'查询以观察结果-1',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存。在传统的时序数据解决方案中，常常需要部署 Kafka、Flink 等流处理系统。而流处理系统的复杂性，带来了高昂的开发与运维成本。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 3.0 的流式计算引擎提供了实时处理写入的数据流的能力，使用 SQL 定义实时流变换，当数据被写入流的源表后，数据会被以定义的方式自动处理，并根据定义的触发模式向目的表推送结果。它提供了替代复杂流处理系统的轻量级解决方案，并能够在高吞吐的数据写入的情况下，提供毫秒级的计算结果延迟。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`流式计算可以包含数据过滤，标量函数计算（含UDF），以及窗口聚合（支持滑动窗口、会话窗口与状态窗口），可以以超级表、子表、普通表为源表，写入到目的超级表。在创建流时，目的超级表将被自动创建，随后新插入的数据会被流定义的方式处理并写入其中，通过 partition by 子句，可以以表名或标签划分 partition，不同的 partition 将写入到目的超级表的不同子表。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 的流式计算能够支持分布在多个 vnode 中的超级表聚合；还能够处理乱序数据的写入：它提供了 watermark 机制以度量容忍数据乱序的程度，并提供了 ignore expired 配置项以决定乱序数据的处理策略——丢弃或者重新计算。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`详见 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../taos-sql/stream"},`流式计算`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"流式计算的创建"},`流式计算的创建`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery
stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
 IGNORE EXPIRED [0 | 1]
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`详细的语法规则参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../taos-sql/stream"},`流式计算`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"示例一"},`示例一`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`企业电表的数据经常都是成百上千亿条的，那么想要将这些分散、凌乱的数据清洗或转换都需要比较长的时间，很难做到高效性和实时性，以下例子中，通过流计算可以将电表电压大于 220V 的数据清洗掉，然后以 5 秒为窗口整合并计算出每个窗口中电流的最大值，最后将结果输出到指定的数据表中。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建-db-和原始数据表"},`创建 DB 和原始数据表`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`首先准备数据，完成建库、建一张超级表和多张子表操作`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`DROP DATABASE IF EXISTS power;
CREATE DATABASE power;
USE power;

CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);

CREATE TABLE d1001 USING meters TAGS ("California.SanFrancisco", 2);
CREATE TABLE d1002 USING meters TAGS ("California.SanFrancisco", 3);
CREATE TABLE d1003 USING meters TAGS ("California.LosAngeles", 2);
CREATE TABLE d1004 USING meters TAGS ("California.LosAngeles", 3);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建流"},`创建流`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create stream current_stream trigger at_once into current_stream_output_stb as select _wstart as wstart, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"写入数据"},`写入数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`insert into d1001 values("2018-10-03 14:38:05.000", 10.30000, 219, 0.31000);
insert into d1001 values("2018-10-03 14:38:15.000", 12.60000, 218, 0.33000);
insert into d1001 values("2018-10-03 14:38:16.800", 12.30000, 221, 0.31000);
insert into d1002 values("2018-10-03 14:38:16.650", 10.30000, 218, 0.25000);
insert into d1003 values("2018-10-03 14:38:05.500", 11.80000, 221, 0.28000);
insert into d1003 values("2018-10-03 14:38:16.600", 13.40000, 223, 0.29000);
insert into d1004 values("2018-10-03 14:38:05.000", 10.80000, 223, 0.29000);
insert into d1004 values("2018-10-03 14:38:06.500", 11.50000, 221, 0.35000);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查询以观察结果"},`查询以观察结果`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select wstart, wend, max_current from current_stream_output_stb;
          wstart         |          wend           |     max_current      |
===========================================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:10.000 |             10.30000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:20.000 |             12.60000 |
Query OK, 2 rows in database (0.018762s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"示例二"},`示例二`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`依然以示例一中的数据为基础，我们已经采集到了每个智能电表的电流和电压数据，现在需要求出有功功率和无功功率，并将地域和电表名以符号 "." 拼接，然后以电表名称分组输出到新的数据表中。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建-db-和原始数据表-1"},`创建 DB 和原始数据表`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`参考示例一 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#%E5%88%9B%E5%BB%BA-db-%E5%92%8C%E5%8E%9F%E5%A7%8B%E6%95%B0%E6%8D%AE%E8%A1%A8"},`创建 DB 和原始数据表`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建流-1"},`创建流`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create stream power_stream trigger at_once into power_stream_output_stb as select ts, concat_ws(".", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"写入数据-1"},`写入数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`参考示例一 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#%E5%86%99%E5%85%A5%E6%95%B0%E6%8D%AE"},`写入数据`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查询以观察结果-1"},`查询以观察结果`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select ts, meter_location, active_power, reactive_power from power_stream_output_stb;
           ts            |         meter_location         |       active_power        |      reactive_power       |
===================================================================================================================
 2018-10-03 14:38:05.000 | California.LosAngeles.d1004    |            2307.834596289 |             688.687331847 |
 2018-10-03 14:38:06.500 | California.LosAngeles.d1004    |            2387.415754896 |             871.474763418 |
 2018-10-03 14:38:05.500 | California.LosAngeles.d1003    |            2506.240411679 |             720.680274962 |
 2018-10-03 14:38:16.600 | California.LosAngeles.d1003    |            2863.424274422 |             854.482390839 |
 2018-10-03 14:38:05.000 | California.SanFrancisco.d1001  |            2148.178871730 |             688.120784090 |
 2018-10-03 14:38:15.000 | California.SanFrancisco.d1001  |            2598.589176205 |             890.081451418 |
 2018-10-03 14:38:16.800 | California.SanFrancisco.d1001  |            2588.728381186 |             829.240910475 |
 2018-10-03 14:38:16.650 | California.SanFrancisco.d1002  |            2175.595991997 |             555.520860397 |
Query OK, 8 rows in database (0.014753s)
`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);