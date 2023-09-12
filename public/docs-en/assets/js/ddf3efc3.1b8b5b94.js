"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[6873],{

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

/***/ 2271:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,title:'Licence Key Management',sidebar_label:'Licensing'};const contentTitle=undefined;const metadata={"unversionedId":"enterprise/licensing","id":"enterprise/licensing","title":"Licence Key Management","description":"Introduction","source":"@site/docs/30-enterprise/15-licensing.md","sourceDirName":"30-enterprise","slug":"/enterprise/licensing","permalink":"/docs-en/enterprise/licensing","draft":false,"tags":[],"version":"current","sidebarPosition":15,"frontMatter":{"toc_max_heading_level":4,"title":"Licence Key Management","sidebar_label":"Licensing"},"sidebar":"defaultSidebar","previous":{"title":"Audit Log","permalink":"/docs-en/enterprise/audit"}};const assets={};const toc=[{value:'Introduction',id:'introduction',level:2},{value:'activeCode and Licensed Items',id:'activecode-and-licensed-items',level:2},{value:'Generate activeCode',id:'generate-activecode',level:2},{value:'Enable activeCode',id:'enable-activecode',level:2},{value:'Configuration File',id:'configuration-file',level:3},{value:'SQL',id:'sql',level:3},{value:'Drop activeCode',id:'drop-activecode',level:3},{value:'Display activeCode',id:'display-activecode',level:3},{value:'Display clsuter license status',id:'display-clsuter-license-status',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"introduction"},`Introduction`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine Enterprise license is managed by enabling activeCode for TDengine cluster of the hosts in the cluster.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"activecode-and-licensed-items"},`activeCode and Licensed Items`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`An activeCode contains the values of one or more licensed items. The licensed items include expiration time, number of data collection points, storage size, number of databases, number of users, number of dnodes, number of CPU cores, etc.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The license is applied per cluster. If there are multiple valid activeCode, then for each licensed item the maximum value among multiple activeCodes is the effective one.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"generate-activecode"},`Generate activeCode`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`activeCode is generated by TDengine support team, based on machine code or cluster ID.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Acquire machine code: run `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taosd -k`),` on the target host, machine code must be generated for each host in the cluster.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Acquire cluster ID: run `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`show cluster\\G`),`, all the hosts in a cluster share same cluster ID.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show cluster\\G;
*************************** 1.row ***************************
         id: 3743835620574542136
       name: 324cbefb-e667-462f-9ebd-d9b0fc753bb3
     uptime: 0
create_time: 2023-08-31 10:37:36.767
    version: trial
expire_time: NULL
Query OK, 1 row(s) in set (0.001976s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"enable-activecode"},`Enable activeCode`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`activeCode can be enabled by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` configuration file or SQL command, and can be shown by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`show dnodes`),` SQL command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`SQL command precedes configuration file. If both ways are used, the activeCode in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` is ignored.S`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`The way of using SQL command is recommended. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`The activeCode enabled by SQL command is stored in the cluster. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Since 3.1.0.0, activeCode enabled in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` can be shown by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`show dndoes`),` command too.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"configuration-file"},`Configuration File`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` file, add one line starting with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`activeCode`),`, followed by a space and then the code. Then, the activeCode can be enabled by rebooting the mnode leader or waiting for 5 minutes.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"sql"},`SQL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can also use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`alter dnode`),` to enable an activeCode. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In following example, enable activeCode for a specific dndoe.下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Enable same activeCode for all dnodes in the cluster.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"drop-activecode"},`Drop activeCode`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Both ways work.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode' '';
taos> alter all dnodes 'activeCode';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"display-activecode"},`Display activeCode`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show dnodes\\G;
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"display-clsuter-license-status"},`Display clsuter license status`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`taos> show grants\\G;
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