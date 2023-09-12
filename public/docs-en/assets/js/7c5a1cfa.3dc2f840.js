"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[7042],{

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

/***/ 2570:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'Use Google Data Studio to access TDengine',sidebar_label:'Google Data Studio',description:'This document describes how to integrate TDengine with Google Data Studio.'};const contentTitle=undefined;const metadata={"unversionedId":"third-party/google-data-studio","id":"third-party/google-data-studio","title":"Use Google Data Studio to access TDengine","description":"This document describes how to integrate TDengine with Google Data Studio.","source":"@site/docs/20-third-party/12-google-data-studio.md","sourceDirName":"20-third-party","slug":"/third-party/google-data-studio","permalink":"/docs-en/third-party/google-data-studio","draft":false,"tags":[],"version":"current","sidebarPosition":12,"frontMatter":{"title":"Use Google Data Studio to access TDengine","sidebar_label":"Google Data Studio","description":"This document describes how to integrate TDengine with Google Data Studio."},"sidebar":"defaultSidebar","previous":{"title":"Kafka","permalink":"/docs-en/third-party/kafka"},"next":{"title":"JupyterLab","permalink":"/docs-en/third-party/Jupyter"}};const assets={};const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Data Studio is a powerful tool for reporting and visualization, offering a wide variety of charts and connectors and making it easy to generate reports based on predefined templates. Its ease of use and robust ecosystem have made it one of the first choices for people working in data analysis.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine is a high-performance, scalable time-series database that supports SQL. Many businesses and developers in fields spanning from IoT and Industry Internet to IT and finance are using TDengine as their time-series database management solution.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The TDengine team immediately saw the benefits of using TDengine to process time-series data with Data Studio to analyze it, and they got to work to create a connector for Data Studio.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`With the release of the TDengine connector in Data Studio, you can now get even more out of your data. To obtain the connector, first go to the Data Studio Connector Gallery, click Connect to Data, and search for "TDengine".`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"02",src:(__webpack_require__(981)/* ["default"] */ .Z),width:"977",height:"514"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Select the TDengine connector and click Authorize.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"03",src:(__webpack_require__(3286)/* ["default"] */ .Z),width:"802",height:"523"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then sign in to your Google Account and click Allow to enable the connection to TDengine.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"04",src:(__webpack_require__(4756)/* ["default"] */ .Z),width:"467",height:"647"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In the Enter URL field, type the hostname and port of the server running the TDengine REST service. In the following fields, type your username, password, database name, table name, and the start and end times of your query range. Then, click Connect.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"05",src:(__webpack_require__(5504)/* ["default"] */ .Z),width:"1024",height:"426"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`After the connection is established, you can use Data Studio to process your data and create reports.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"06",src:(__webpack_require__(6565)/* ["default"] */ .Z),width:"1024",height:"368"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In Data Studio, TDengine timestamps and tags are considered dimensions, and all other items are considered metrics. You can create all kinds of custom charts with your data - some examples are shown below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"07",src:(__webpack_require__(8400)/* ["default"] */ .Z),width:"1024",height:"528"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`With the ability to process petabytes of data per day and provide monitoring and alerting in real time, TDengine is a great solution for time-series data management. Now, with the Data Studio connector, we're sure you'll be able to gain new insights and obtain even more value from your data.`));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 981:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/gds-02.png-d5c7d22e5b1bee94999756beab99a490.webp");

/***/ }),

/***/ 3286:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/gds-03.png-a10113ef49acf7b2e03c17d69ad9252e.webp");

/***/ }),

/***/ 4756:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/gds-04.png-606977ededc8f47df15da570ed47f034.webp");

/***/ }),

/***/ 5504:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 6565:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 8400:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/gds-07.png-eb132d72cbfc844d7a25b31f18e959cc.webp");

/***/ })

}]);