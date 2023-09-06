"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[6873],{

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

/***/ 2271:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,title:'授权码管理',sidebar_label:'授权码管理'};const contentTitle=undefined;const metadata={"unversionedId":"enterprise/licensing","id":"enterprise/licensing","title":"授权码管理","description":"概述","source":"@site/docs/30-enterprise/15-licensing.md","sourceDirName":"30-enterprise","slug":"/enterprise/licensing","permalink":"/docs/enterprise/licensing","draft":false,"tags":[],"version":"current","sidebarPosition":15,"frontMatter":{"toc_max_heading_level":4,"title":"授权码管理","sidebar_label":"授权码管理"},"sidebar":"defaultSidebar","previous":{"title":"审计日志","permalink":"/docs/enterprise/audit"}};const assets={};const toc=[{value:'概述',id:'概述',level:2},{value:'授权码与授权项',id:'授权码与授权项',level:2},{value:'授权码生成与发放',id:'授权码生成与发放',level:2},{value:'授权码设置与查看',id:'授权码设置与查看',level:2},{value:'配置文件',id:'配置文件',level:3},{value:'SQL 命令',id:'sql-命令',level:3},{value:'删除授权码',id:'删除授权码',level:3},{value:'查看授权码',id:'查看授权码',level:3},{value:'查看集群授权状态',id:'查看集群授权状态',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"概述"},`概述`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine Enterprise 的授权，是通过对集群中的服务器设置授权码 (activeCode) 的方式完成的。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"授权码与授权项"},`授权码与授权项`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`授权码中，包含各个授权项的值。授权项中，常用的包括过期时间和测点数，也包括存储空间、数据库实例数、用户数、dnode 实例数、cpu 核数等。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`授权码是以集群为单位生效的。如果集群中包含多个有效的授权码，则按较大值优先的原则对各个授权项取并集。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"授权码生成与发放"},`授权码生成与发放`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`授权码由 TDengine 交付团队负责生成，支持 2 种生成方式：基于机器码 (machine code) 或集群 Id (cluster id)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`机器码的获取方式：在目标服务器上运行 taosd -k。集群中的每台服务器都对应一个机器码。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`集群 Id 的获取方式：在 taos 客户端执行 show cluster\\G;,  其中，id 字段为 集群 Id。集群中的所有服务器共享一个 集群 Id。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show cluster\\G;
*************************** 1.row ***************************
         id: 3743835620574542136
       name: 324cbefb-e667-462f-9ebd-d9b0fc753bb3
     uptime: 0
create_time: 2023-08-31 10:37:36.767
    version: trial
expire_time: NULL
Query OK, 1 row(s) in set (0.001976s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"授权码设置与查看"},`授权码设置与查看`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`支持通过 taos.cfg 配置文件和 SQL 命令 2 种设置方式，并支持通过 show dnodes 命令查看。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`SQL 命令的优先级更高，如果 2 种方式都设置，taos.cfg 中的授权码会被忽略。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`推荐使用 SQL 命令的设置方式，支持 taos.cfg 设置是为了兼容老版本。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`通过 SQL 命令设置的授权码会保存到集群中，因此，支持通过 show dnodes 命令查看。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`3.1.0.0 版本之前，taos.cfg 中设置的授权码，无法通过 show dnodes 命令查看。3.1.0.0 版本起，如果授权码是在 taos.cfg 中设置的， taosd 会自动读取 taos.cfg 中的授权码并保存到集群中并支持通过 show dnodes 命令查看。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"配置文件"},`配置文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用配置文件激活授权码：在 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` 中以 activeCode 开头添加一行，空格后部分为授权码。添加后，重启 mnode leader 所在的 dnode 立即生效，或者不重启 5 分钟内生效。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"sql-命令"},`SQL 命令`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 SQL 命令 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`alter dnode`),` 激活授权码：在 taos 客户端执行，支持针对单个 dnode  或所有 dnode 设置。设置授权码一般在 5 秒内生效，如果集群节点较多，生效时间会长一些。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`下述命令中，dnode 后边的 1 为 dnode id，可以通过在 taos 客户端执行 show dnodes 获取。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`针对集群中所有 dnode 设置相同的授权码。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"删除授权码"},`删除授权码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`以下两种方法均有效`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode' '';
taos> alter all dnodes 'activeCode';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查看授权码"},`查看授权码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show dnodes\\G;
*************************** 1.row ***************************
            id: 1
      endpoint: u3-31:6030
        vnodes: 0
support_vnodes: 17
        status: ready
   create_time: 2023-08-31 10:37:36.765
   reboot_time: 2023-08-31 10:37:36.754
          note: 
   active_code: Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=
 c_active_code: 
Query OK, 1 row(s) in set (0.003062s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查看集群授权状态"},`查看集群授权状态`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`授权状态是以集群为单位的，通过在 taos 客户端执行 show grants\\G; 查看。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show grants\\G;
*************************** 1.row ***************************
    version: trial
expire_time: 2023-10-31 09:37:36
    expired: false
    storage: unlimited
 timeseries: unlimited
  databases: unlimited
      users: unlimited
   accounts: unlimited
     dnodes: unlimited
connections: unlimited
    streams: unlimited
  cpu_cores: unlimited
      speed: unlimited
  querytime: unlimited
     opc_da: {"type":"OPC_DA","number":1,"speed":-1,"expire":"19615"}
     opc_ua: {"type":"OPC_UA","number":1,"speed":-1,"expire":"19615"}
         pi: {"type":"Pi","number":1,"speed":-1,"expire":"19615"}
      kafka: {"type":"Kafka","number":1,"speed":-1,"expire":"19615"}
   influxdb: {"type":"InfluxDB","number":1,"speed":-1,"expire":"19615"}
       mqtt: {"type":"MQTT","number":1,"speed":-1,"expire":"19615"}
Query OK, 1 row(s) in set (0.003706s)
`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);