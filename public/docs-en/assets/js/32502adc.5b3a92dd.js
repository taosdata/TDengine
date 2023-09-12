"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1170],{

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

/***/ 6810:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'User-Defined Functions (UDF)',sidebar_label:'UDF',description:'This document describes how to create user-defined functions (UDF), your own scalar and aggregate functions that can expand the query capabilities of TDengine.'};const contentTitle=undefined;const metadata={"unversionedId":"develop/udf","id":"develop/udf","title":"User-Defined Functions (UDF)","description":"This document describes how to create user-defined functions (UDF), your own scalar and aggregate functions that can expand the query capabilities of TDengine.","source":"@site/docs/07-develop/09-udf.md","sourceDirName":"07-develop","slug":"/develop/udf","permalink":"/docs-en/develop/udf","draft":false,"tags":[],"version":"current","sidebarPosition":9,"frontMatter":{"title":"User-Defined Functions (UDF)","sidebar_label":"UDF","description":"This document describes how to create user-defined functions (UDF), your own scalar and aggregate functions that can expand the query capabilities of TDengine."},"sidebar":"defaultSidebar","previous":{"title":"Caching","permalink":"/docs-en/develop/cache"},"next":{"title":"TDengine SQL","permalink":"/docs-en/taos-sql/"}};const assets={};const toc=[{value:'Implement a UDF in C',id:'implement-a-udf-in-c',level:2},{value:'Implementing a Scalar Function in C',id:'implementing-a-scalar-function-in-c',level:3},{value:'Implementing an Aggregate Function in C',id:'implementing-an-aggregate-function-in-c',level:3},{value:'UDF Interface Definition in C',id:'udf-interface-definition-in-c',level:3},{value:'Scalar Interface',id:'scalar-interface',level:4},{value:'Aggregate Interface',id:'aggregate-interface',level:4},{value:'Initialization and Cleanup Interface',id:'initialization-and-cleanup-interface',level:4},{value:'Data Structures for UDF in C',id:'data-structures-for-udf-in-c',level:3},{value:'Compiling C UDF',id:'compiling-c-udf',level:3},{value:'UDF Sample Code in C',id:'udf-sample-code-in-c',level:3},{value:'Scalar function: bit_and',id:'scalar-function-bit_and',level:4},{value:'Aggregate function 1: l2norm',id:'aggregate-function-1-l2norm',level:4},{value:'Aggregate function 2: max_vol',id:'aggregate-function-2-max_vol',level:4},{value:'Implement a UDF in Python',id:'implement-a-udf-in-python',level:2},{value:'Prepare Environment',id:'prepare-environment',level:3},{value:'Interface definition',id:'interface-definition',level:3},{value:'Introduction to Interface',id:'introduction-to-interface',level:4},{value:'Scalar UDF Interface',id:'scalar-udf-interface',level:4},{value:'Aggregate UDF Interface',id:'aggregate-udf-interface',level:4},{value:'Initialization and Cleanup Interface',id:'initialization-and-cleanup-interface-1',level:4},{value:'Python UDF Template',id:'python-udf-template',level:3},{value:'Scalar Template',id:'scalar-template',level:4},{value:'Aggregate Template',id:'aggregate-template',level:4},{value:'Data Mapping between TDengine SQL and Python UDF',id:'data-mapping-between-tdengine-sql-and-python-udf',level:3},{value:'Development Guide',id:'development-guide',level:3},{value:'Sample 1: Simplest UDF',id:'sample-1-simplest-udf',level:4},{value:'Sample 2: Abnormal Processing',id:'sample-2-abnormal-processing',level:4},{value:'Sample 3: UDF with n arguments',id:'sample-3-udf-with-n-arguments',level:4},{value:'Sample 4: Utilize 3rd party package',id:'sample-4-utilize-3rd-party-package',level:4},{value:'Sample 5: Aggregate Function',id:'sample-5-aggregate-function',level:4},{value:'SQL Commands',id:'sql-commands',level:3},{value:'More Python UDF Samples',id:'more-python-udf-samples',level:3},{value:'Scalar Function pybitand',id:'scalar-function-pybitand',level:4},{value:'Aggregate Function pyl2norm',id:'aggregate-function-pyl2norm',level:4},{value:'Aggregate Function pycumsum',id:'aggregate-function-pycumsum',level:4}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The built-in functions of TDengine may not be sufficient for the use cases of every application. In this case, you can define custom functions for use in TDengine queries. These are known as user-defined functions (UDF). A user-defined function takes one column of data or the result of a subquery as its input.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`User-defined functions can be scalar functions or aggregate functions. Scalar functions, such as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`abs`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sin`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`concat`),`, output a value for every row of data. Aggregate functions, such as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`avg`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`max`),` output one value for multiple rows of data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine supports user-defined functions written in C or Python. This document describes the usage of user-defined functions.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"implement-a-udf-in-c"},`Implement a UDF in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`When you create a user-defined function, you must implement standard interface functions:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`For scalar functions, implement the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`scalarfn`),` interface function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`For aggregate functions, implement the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`aggfn_start`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`aggfn`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`aggfn_finish`),` interface functions.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`To initialize your function, implement the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`udf_init`),` function. To terminate your function, implement the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`udf_destroy`),` function.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are strict naming conventions for these interface functions. The names of the start, finish, init, and destroy interfaces must be `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_start`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_finish`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_init`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_destroy`),`, respectively. Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`scalarfn`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggfn`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`udf`),` with the name of your user-defined function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"implementing-a-scalar-function-in-c"},`Implementing a Scalar Function in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The implementation of a scalar function is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// initialization function. if no initialization, we can skip definition of it. The initialization function shall be concatenation of the udf name and _init suffix
// @return error number defined in taoserror.h
int32_t scalarfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// scalar function main computation function
// @param inputDataBlock, input data block composed of multiple columns with each column defined by SUdfColumn
// @param resultColumn, output column
// @return error number defined in taoserror.h
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn* resultColumn) {
    // read data from inputDataBlock and process, then output to resultColumn.
    return TSDB_CODE_SUCCESS;
}

// cleanup function. if no cleanup related processing, we can skip definition of it. The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t scalarfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`scalarfn`),` with the name of your function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"implementing-an-aggregate-function-in-c"},`Implementing an Aggregate Function in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The implementation of an aggregate function is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// Initialization function. if no initialization, we can skip definition of it. The initialization function shall be concatenation of the udf name and _init suffix
// @return error number defined in taoserror.h
int32_t aggfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// aggregate start function. The intermediate value or the state(@interBuf) is initialized in this function. The function name shall be concatenation of udf name and _start suffix
// @param interbuf intermediate value to initialize
// @return error number defined in taoserror.h
int32_t aggfn_start(SUdfInterBuf* interBuf) {
    // initialize intermediate value in interBuf
    return TSDB_CODE_SUCCESS;
}

// aggregate reduce function. This function aggregate old state(@interbuf) and one data bock(inputBlock) and output a new state(@newInterBuf).
// @param inputBlock input data block
// @param interBuf old state
// @param newInterBuf new state
// @return error number defined in taoserror.h
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    // read from inputBlock and interBuf and output to newInterBuf
    return TSDB_CODE_SUCCESS;
}

// aggregate function finish function. This function transforms the intermediate value(@interBuf) into the final output(@result). The function name must be concatenation of aggfn and _finish suffix.
// @interBuf : intermediate value
// @result: final result
// @return error number defined in taoserror.h
int32_t int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result) {
    // read data from inputDataBlock and process, then output to result
    return TSDB_CODE_SUCCESS;
}

// cleanup function. if no cleanup related processing, we can skip definition of it. The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t aggfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggfn`),` with the name of your function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"udf-interface-definition-in-c"},`UDF Interface Definition in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are strict naming conventions for interface functions. The names of the start, finish, init, and destroy interfaces must be <udf-name`,`>`,`_start, <udf-name`,`>`,`_finish, <udf-name`,`>`,`_init, and <udf-name`,`>`,`_destroy, respectively. Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`scalarfn`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggfn`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`udf`),` with the name of your user-defined function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Interface functions return a value that indicates whether the operation was successful. If an operation fails, the interface function returns an error code. Otherwise, it returns TSDB_CODE_SUCCESS. The error codes are defined in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taoserror.h`),` and in the common API error codes in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.h`),`. For example, TSDB_CODE_UDF_INVALID_INPUT indicates invalid input. TSDB_CODE_OUT_OF_MEMORY indicates insufficient memory.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For information about the parameters for interface functions, see Data Model`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"scalar-interface"},`Scalar Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,` `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,` Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`scalarfn`),` with the name of your function. This function performs scalar calculations on data blocks. You can configure a value through the parameters in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`resultColumn`),` structure.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The parameters in the function are defined as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`inputDataBlock: The data block to input.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`resultColumn: The column to output. The column to output.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-interface"},`Aggregate Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn_start(SUdfInterBuf *interBuf)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggfn`),` with the name of your function. In the function, aggfn_start is called to generate a result buffer. Data is then divided between multiple blocks, and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggfn`),` function is called on each block to update the result. Finally, aggfn_finish is called to generate the final results from the intermediate results. The final result contains only one or zero data points.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The parameters in the function are defined as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`interBuf: The intermediate result buffer.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`inputBlock: The data block to input.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`newInterBuf: The new intermediate result buffer.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`result: The final result.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"initialization-and-cleanup-interface"},`Initialization and Cleanup Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t udf_init()`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t udf_destroy()`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Replace `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`udf`),` with the name of your function. udf_init initializes the function. udf_destroy terminates the function. If it is not necessary to initialize your function, udf_init is not required. If it is not necessary to terminate your function, udf_destroy is not required.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"data-structures-for-udf-in-c"},`Data Structures for UDF in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`typedef struct SUdfColumnMeta {
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SUdfColumnMeta;

typedef struct SUdfColumnData {
  int32_t numOfRows;
  int32_t rowsAlloc;
  union {
    struct {
      int32_t nullBitmapLen;
      char   *nullBitmap;
      int32_t dataLen;
      char   *data;
    } fixLenCol;

    struct {
      int32_t varOffsetsLen;
      int32_t   *varOffsets;
      int32_t payloadLen;
      char   *payload;
      int32_t payloadAllocLen;
    } varLenCol;
  };
} SUdfColumnData;

typedef struct SUdfColumn {
  SUdfColumnMeta colMeta;
  bool           hasNull;
  SUdfColumnData colData;
} SUdfColumn;

typedef struct SUdfDataBlock {
  int32_t numOfRows;
  int32_t numOfCols;
  SUdfColumn **udfCols;
} SUdfDataBlock;

typedef struct SUdfInterBuf {
  int32_t bufLen;
  char* buf;
  int8_t numOfResult; //zero or one
} SUdfInterBuf;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The data structure is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The SUdfDataBlock block includes the number of rows (numOfRows) and the number of columns (numCols). udfCols`,`[i]`,` (0 <= i <= numCols-1) indicates that each column is of type SUdfColumn.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfColumn includes the definition of the data type of the column (colMeta) and the data in the column (colData).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The member definitions of SUdfColumnMeta are the same as the data type definitions in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.h`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The data in SUdfColumnData can become longer. varLenCol indicates variable-length data, and fixLenCol indicates fixed-length data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfInterBuf defines the intermediate structure `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`buffer`),` and the number of results in the buffer `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`numOfResult`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Additional functions are defined in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosudf.h`),` to make it easier to work with these structures.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"compiling-c-udf"},`Compiling C UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`To use your user-defined function in TDengine, first, compile it to a shared library.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example, the sample UDF `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`bit_and.c`),` can be compiled into a DLL as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The generated DLL file `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`libbitand.so`),` can now be used to implement your function. Note: GCC 7.5 or later is required.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"udf-sample-code-in-c"},`UDF Sample Code in C`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"scalar-function-bit_and"},`Scalar function: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c"},`bit_and`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The bit_and function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The bit_and function ignores null values.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"bit_and.c"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

DLL_EXPORT int32_t bit_and_init() { return 0; }

DLL_EXPORT int32_t bit_and_destroy() { return 0; }

DLL_EXPORT int32_t bit_and(SUdfDataBlock* block, SUdfColumn* resultCol) {
  if (block->numOfCols < 2) {
    return TSDB_CODE_UDF_INVALID_INPUT;
  }

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (!(col->colMeta.type == TSDB_DATA_TYPE_INT)) {
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }

  SUdfColumnData* resultData = &resultCol->colData;

  for (int32_t i = 0; i < block->numOfRows; ++i) {
    if (udfColDataIsNull(block->udfCols[0], i)) {
      udfColDataSetNull(resultCol, i);
      continue;
    }
    int32_t result = *(int32_t*)udfColDataGetData(block->udfCols[0], i);
    int     j = 1;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        break;
      }

      char* colData = udfColDataGetData(block->udfCols[j], i);
      result &= *(int32_t*)colData;
    }
    if (j == block->numOfCols) {
      udfColDataSet(resultCol, i, (char*)&result, false);
    }
  }
  resultData->numOfRows = block->numOfRows;

  return TSDB_CODE_SUCCESS;
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/bit_and.c"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-function-1-l2norm"},`Aggregate function 1: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c"},`l2norm`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The l2norm function finds the second-order norm for all data in the input column. This squares the values, takes a cumulative sum, and finds the square root.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"l2norm.c"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#include "taosudf.h"

DLL_EXPORT int32_t l2norm_init() {
  return 0;
}

DLL_EXPORT int32_t l2norm_destroy() {
  return 0;
}

DLL_EXPORT int32_t l2norm_start(SUdfInterBuf *buf) {
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = sizeof(double);
  buf->numOfResult = 1;
  return 0;
}

DLL_EXPORT int32_t l2norm(SUdfDataBlock* block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  double sumSquares = *(double*)interBuf->buf;
  int8_t numNotNull = 0;
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (!(col->colMeta.type == TSDB_DATA_TYPE_INT || 
          col->colMeta.type == TSDB_DATA_TYPE_DOUBLE)) {
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        continue;
      }
      switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_INT: {
          char* cell = udfColDataGetData(col, j);
          int32_t num = *(int32_t*)cell;
          sumSquares += (double)num * num;
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          char* cell = udfColDataGetData(col, j);
          double num = *(double*)cell;
          sumSquares += num * num;
          break;
        }
        default: 
          break;
      }
      ++numNotNull;
    }
  }

  *(double*)(newInterBuf->buf) = sumSquares;
  newInterBuf->bufLen = sizeof(double);
  newInterBuf->numOfResult = 1;
  return 0;
}

DLL_EXPORT int32_t l2norm_finish(SUdfInterBuf* buf, SUdfInterBuf *resultData) {
  double sumSquares = *(double*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  return 0;
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/l2norm.c"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-function-2-max_vol"},`Aggregate function 2: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/max_vol.c"},`max_vol`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The max_vol function returns a string concatenating the deviceId column, the row number and column number of the maximum voltage and the maximum voltage given several voltage columns as input.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Create Table:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Create the UDF:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use the UDF in the query:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`select max_vol(vol1,vol2,vol3,deviceid) from battery;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"max_vol.c"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#include "taosudf.h"

#define STR_MAX_LEN 256 // inter buffer length

// init
DLL_EXPORT int32_t max_vol_init()
{
    return 0;
}

// destory
DLL_EXPORT int32_t max_vol_destroy()
{
    return 0;
}

// start 
DLL_EXPORT int32_t max_vol_start(SUdfInterBuf *buf)
{
    memset(buf->buf, 0, sizeof(float) + STR_MAX_LEN);
    // set init value
    *((float*)buf->buf) = -10000000;
    buf->bufLen = sizeof(float)  + STR_MAX_LEN;
    buf->numOfResult = 0;
    return 0;
}

DLL_EXPORT int32_t max_vol(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    float maxValue = *(float *)interBuf->buf;
    char strBuff[STR_MAX_LEN] = "inter1buf";
    
    if (block->numOfCols < 2)
    {
        return TSDB_CODE_UDF_INVALID_INPUT;
    }

    // check data type
    for (int32_t i = 0; i < block->numOfCols; ++i)
    {
        SUdfColumn *col = block->udfCols[i];
        if( i == block->numOfCols - 1) {
          // last column is device id , must varchar
          if (col->colMeta.type != TSDB_DATA_TYPE_VARCHAR ) {
            return TSDB_CODE_UDF_INVALID_INPUT;
          }
        } else {
          if (col->colMeta.type != TSDB_DATA_TYPE_FLOAT) {
            return TSDB_CODE_UDF_INVALID_INPUT;
          }
        }
    }

    // calc max voltage
    SUdfColumn *lastCol = block->udfCols[block->numOfCols - 1];
    for (int32_t i = 0; i < (block->numOfCols - 1); ++i) {
        for (int32_t j = 0; j < block->numOfRows; ++j) {
          SUdfColumn *col = block->udfCols[i];
          if (udfColDataIsNull(col, j)) {
            continue;
          }
          char *data = udfColDataGetData(col, j);
          float voltage = *(float *)data;
          if (voltage > maxValue) {
            maxValue = voltage;
            char *valData = udfColDataGetData(lastCol, j);
            // get device id
            char *deviceId = valData + sizeof(uint16_t);
            sprintf(strBuff, "%s_(%d,%d)_%f", deviceId, j, i, maxValue);
          }
        }
    }

    *(float*)newInterBuf->buf = maxValue;
    strcpy(newInterBuf->buf + sizeof(float), strBuff);
    newInterBuf->bufLen = sizeof(float) + strlen(strBuff)+1;
    newInterBuf->numOfResult = 1;
    return 0;
}

DLL_EXPORT int32_t max_vol_finish(SUdfInterBuf *buf, SUdfInterBuf *resultData)
{
    char * str = buf->buf + sizeof(float);
    // copy to des
    char * des = resultData->buf + sizeof(uint16_t);
    strcpy(des, str);

    // set binary type len
    uint16_t  len = strlen(str);
    *((uint16_t*)resultData->buf) = len;

    // set buf len
    resultData->bufLen = len + sizeof(uint16_t);
    // set row count
    resultData->numOfResult = 1;
    return 0;
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/max_vol.c"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"implement-a-udf-in-python"},`Implement a UDF in Python`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"prepare-environment"},`Prepare Environment`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Prepare Python Environment`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please follow standard procedure of python environment preparation.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Install Python package `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taospyudf`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`pip3 install taospyudf
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`During this process, some C++ code needs to be compiled. So it's required to have `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cmake`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`gcc`),` on your system. The compiled `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`libtaospyudf.so`),` will be automatically copied to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/usr/local/lib`),` path. If you are not root user, please use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sudo`),`. After installation is done, please check using the command below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then execute the command below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`ldconfig
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`If you want to utilize some 3rd party python packages in your Python UDF, please set configuration parameter `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`UdfdLdLibPath`),` to the value of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`PYTHONPATH`),` before starting `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Launch `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),` service`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../get-started"},`Get Started`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"interface-definition"},`Interface definition`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"introduction-to-interface"},`Introduction to Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Implement the specified interface functions when implementing a UDF in Python.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`implement `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`process`),` function for the scalar UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`implement `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`start`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`reduce`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`finish`),` for the aggregate UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`implement `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`init`),` for initialization and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`destroy`),` for termination.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"scalar-udf-interface"},`Scalar UDF Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The implementation of a scalar UDF is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def process(input: datablock) -> tuple[output_type]:   
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Description: this function processes datablock, which is the input; you can use datablock.data(row, col) to access the python object at location(row,col); the output is a tuple object consisted of objects of type outputtype`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-udf-interface"},`Aggregate UDF Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The implementation of an aggregate function is described as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes   
def finish(buf: bytes) -> output_type:
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Description: first the start() is invoked to generate the initial result `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`buffer`),`; then the input data is divided into multiple row blocks, and reduce() is invoked for each block `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`inputs`),` and current intermediate result `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`buf`),`; finally finish() is invoked to generate the final result from intermediate `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`buf`),`, the final result can only contains 0 or 1 data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"initialization-and-cleanup-interface-1"},`Initialization and Cleanup Interface`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def init()
def destroy()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Description: init() does the work of initialization before processing any data; destroy() does the work of cleanup after the data is processed.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"python-udf-template"},`Python UDF Template`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"scalar-template"},`Scalar Template`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:
    # process input datablock,
    # datablock.data(row, col) is to access the python object in location(row,col)
    # return tuple object consisted of object of type outputtype
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note：process() must be implemented, init() and destroy() must be defined too but they can do nothing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-template"},`Aggregate Template`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
    #initialization
def destroy():
    #destroy
def start() -> bytes:
    #return serialize(init_state)
def reduce(inputs: datablock, buf: bytes) -> bytes
    # deserialize buf to state
    # reduce the inputs and state into new_state.
    # use inputs.data(i,j) to access python object of location(i,j)
    # serialize new_state into new_state_bytes
    return new_state_bytes
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: aggregate UDF requires init(), destroy(), start(), reduce() and finish() to be implemented. start() generates the initial result in buffer, then the input data is divided into multiple row data blocks, reduce() is invoked for each data block `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`inputs`),` and intermediate `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`buf`),`, finally finish() is invoked to generate final result from the intermediate result `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`buf`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"data-mapping-between-tdengine-sql-and-python-udf"},`Data Mapping between TDengine SQL and Python UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following table describes the mapping between TDengine SQL data type and Python UDF Data Type. The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NULL`),` value of all TDengine SQL types is mapped to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`None`),` value in Python.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"th"},`TDengine SQL Data Type`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"th"},`Python Data Type`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TINYINT / SMALLINT / INT  / BIGINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`FLOAT / DOUBLE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BOOL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BINARY / VARCHAR / NCHAR`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bytes`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TIMESTAMP`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`JSON and other types`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Not Supported`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"development-guide"},`Development Guide`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In this section we will demonstrate 5 examples of developing UDF in Python language. In this guide, you will learn the development skills from easy case to hard case, the examples include: `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`A scalar function which accepts only one integer as input and outputs ln(n^2 + 1)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`A scalar function which accepts n integers, like（x1, x2, ..., xn）and output the sum of the product of each input and its sequence number, i.e. x1 + 2 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` x2 + ... + n `),` xn。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`A scalar function which accepts a timestamp and output the next closest Sunday of the timestamp. In this case, we will demonstrate how to use 3rd party library `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`moment`),`. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`An aggregate function which calculates the difference between the maximum and the minimum of a specific column, i.e. same functionality of built-in spread().`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In the guide, some debugging skills of using Python UDF will be explained too. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`We assume you are using Linux system and already have TDengine 3.0.4.0+ and Python 3.7+.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note:`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`You can't use print() function to output log inside a UDF, you have to write the log to a specific file or use logging module of Python.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sample-1-simplest-udf"},`Sample 1: Simplest UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This scalar UDF accepts an integer as input and output ln(n^2 + 1).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Firstly, please compose a Python source code file in your system and save it, e.g. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/root/udf/myfun.py`),`, the code is like below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from math import log

def init():
    pass

def destroy():
    pass

def process(block):
    rows, _ = block.shape()
    return [log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This program consists of 3 functions, init() and destroy() do nothing, but they have to be defined even though there is nothing to do in them because they are critical parts of a python UDF. The most important function is process(), which accepts a data block and the data block object has two methods: `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`shape() returns the number of rows and the number of columns of the data block`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`data(i, j) returns the value at (i,j) in the block`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The output of the process() function of a scalar UDF returns exactly same number of data as the number of input rows. We will ignore the number of columns because we just want to compute on the first column.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then, we create the UDF using the SQL command below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Here is the output example, it may change a little depending on your version being used.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},` taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then, we used the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`show`),` command to prove the creation of the UDF is successful.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Next, we can try to test the function. Before executing the UDF, we need to prepare some data using the command below in TDengine CLI.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Execute the UDF to test it:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Unfortunately, the UDF execution failed. We need to check the log `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`udfd`),` daemon to find out why.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`tail -10 /var/log/taos/udfd.log
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Below is the output.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`From the error message we can find out that `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`libtaospyudf.so`),` was not loaded successfully. Please refer to the `,`[Prepare Environment]`,` section.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`After correcting environment issues, execute the UDF:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Now, we have finished the first PDF in Python, and learned some basic debugging skills.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sample-2-abnormal-processing"},`Sample 2: Abnormal Processing`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`myfun`),` UDF example in sample 1 has passed, but it has two drawbacks.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`It the program accepts only one column of data as input, but it doesn't throw exception if you passes multiple columns.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`null`),` value is not processed. We expect the program to throw exception and terminate if `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`null`),` is passed as input.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`So, we try to optimize the process() function as below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The update the UDF with command below.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`At this time, if we pass two arguments to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`myfun`),`, the execution would fail.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`However, the exception is not shown to end user, but displayed in the log file `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/var/log/taos/taospyudf.log`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Now, we have learned how to update a UDF and check the log of a UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: Prior to TDengine 3.0.5.0 (excluding), updating a UDF requires to restart `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),` service. After 3.0.5.0, restarting is not required. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sample-3-udf-with-n-arguments"},`Sample 3: UDF with n arguments`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A UDF which accepts n integers, likee (x1, x2, ..., xn) and output the sum of the product of each value and its sequence number: 1 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`  x1 + 2 `),` x2 + ... + n * xn. If there is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`null`),` in the input, then the result is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`null`),`. The difference from sample 1 is that it can accept any number of columns as input and process each column. Assume the program is written in /root/udf/nsum.py:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def init():
    pass


def destroy():
    pass


def process(block):
    rows, cols = block.shape()
    result = []
    for i in range(rows):
        total = 0
        for j in range(cols):
            v = block.data(i, j)
            if v is None:
                total = None
                break
            total += (j + 1) * block.data(i, j)
        result.append(total)
    return result
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Crate and test the UDF:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> insert into t values('2023-05-25 09:09:15', 6, null, 8);
Insert OK, 1 row(s) affected (0.003675s)

taos> select ts, v1, v2, v3,  nsum(v1, v2, v3) from t;
           ts            |     v1      |     v2      |     v3      |     nsum(v1, v2, v3)      |
================================================================================================
 2023-05-01 12:13:14.000 |           1 |           2 |           3 |              14.000000000 |
 2023-05-03 08:09:10.000 |           2 |           3 |           4 |              20.000000000 |
 2023-05-10 07:06:05.000 |           3 |           4 |           5 |              26.000000000 |
 2023-05-25 09:09:15.000 |           6 |        NULL |           8 |                      NULL |
Query OK, 4 row(s) in set (0.010653s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sample-4-utilize-3rd-party-package"},`Sample 4: Utilize 3rd party package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A UDF which accepts a timestamp and output the next closed Sunday. This sample requires to use third party package `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`moment`),`, you need to install it firstly.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`pip3 install moment
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then compose the Python code in /root/udf/nextsunday.py`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import moment


def init():
    pass


def destroy():
    pass


def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception("require only 1 parameter")
    if not type(block.data(0, 0)) is int:
        raise Exception("type error")
    return [moment.unix(block.data(i, 0)).replace(weekday=7).format('YYYY-MM-DD')
            for i in range(rows)]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`UDF framework will map the TDengine timestamp to Python int type, so this function only accepts an integer representing millisecond. process() firstly validates the parameters, then use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`moment`),` to replace the time, format the result and output. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Create and test the UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If your `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),` is started using `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`systemd`),`, you may encounter the error below. Next we will show how to debug.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},` tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This is because `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`moment`),` doesn't exist in the default library search path of python UDF, please check the log file `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosdpyudf.log`),`. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`grep 'sys path' taospyudf.log  | tail -1
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You may find that the default library search path is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/lib/python3/dist-packages`),` (just for example, it may be different in your system), but `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`moment`),` is installed to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/usr/local/lib/python3.8/dist-packages`),` (for example, it may be different in your system). Then we change the library search path of python UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Check `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sys.path`),`, which must include the packages you install with pip3 command previously, as shown below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Copy the output and edit /var/taos/taos.cfg to add below configuration parameter.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Save it, then restart `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),`, using `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`systemctl restart taosd`),`, and test again, it will succeed this time.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: If your cluster consists of multiple `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),` instances, you have to repeat same process for each of them.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select ts, nextsunday(ts) from t;
           ts            | nextsunday(ts) |
===========================================
 2023-05-01 12:13:14.000 | 2023-05-07     |
 2023-05-03 08:09:10.000 | 2023-05-07     |
 2023-05-10 07:06:05.000 | 2023-05-14     |
 2023-05-25 09:09:15.000 | 2023-05-28     |
Query OK, 4 row(s) in set (1.011474s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sample-5-aggregate-function"},`Sample 5: Aggregate Function`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`An aggregate function which calculates the difference of the maximum and the minimum in a column. An aggregate funnction takes multiple rows as input and output only one data. The execution process of an aggregate UDF is like map-reduce, the framework divides the input into multiple parts, each mapper processes one block and the reducer aggregates the result of the mappers. The reduce() of Python UDF has the functionality of both map() and reduce(). The reduce() takes two arguments: the data to be processed; and the result of other tasks executing reduce(). For example, assume the code is in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/root/udf/myspread.py`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import io
import math
import pickle

LOG_FILE: io.TextIOBase = None


def init():
    global LOG_FILE
    LOG_FILE = open("/var/log/taos/spread.log", "wt")
    log("init function myspead success")


def log(o):
    LOG_FILE.write(str(o) + '\\n')


def destroy():
    log("close log file: spread.log")
    LOG_FILE.close()


def start():
    return pickle.dumps((-math.inf, math.inf))


def reduce(block, buf):
    max_number, min_number = pickle.loads(buf)
    log(f"initial max_number={max_number}, min_number={min_number}")
    rows, _ = block.shape()
    for i in range(rows):
        v = block.data(i, 0)
        if v > max_number:
            log(f"max_number={v}")
            max_number = v
        if v < min_number:
            log(f"min_number={v}")
            min_number = v
    return pickle.dumps((max_number, min_number))


def finish(buf):
    max_number, min_number = pickle.loads(buf)
    return max_number - min_number
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In this example, we implemented an aggregate function, and added some logging. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`init() opens a file for logging `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`log() is the function for logging, it converts the input object to string and output with an end of line`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`destroy() closes the log file \\`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`start() returns the initial buffer for storing the intermediate result `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`reduce() processes each data block and aggregates the result`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`finish() converts the final buffer() to final result\\ `)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Create the UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This SQL command has two important different points from the command creating scalar UDF.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`keyword `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`aggregate`),` is used`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`keyword `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`bufsize`),` is used to specify the memory size for storing the intermediate result. In this example, the result is 32 bytes, but we specified 128 bytes for `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`bufsize`),`. You can use the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`python`),` CLI to print actual size.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Test this function, you can see the result is same as built-in spread() function. \\`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myspread(v1) from t;
       myspread(v1)        |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.013486s)

taos> select spread(v1) from t;
        spread(v1)         |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.005501s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`At last, check the log file, we can see that the reduce() function is executed 3 times, max value is updated 3 times and min value is updated only one time.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`root@slave11 /var/log/taos $ cat spread.log
init function myspead success
initial max_number=-inf, min_number=inf
max_number=1
min_number=1
initial max_number=1, min_number=1
max_number=2
max_number=3
initial max_number=3, min_number=1
max_number=6
close log file: spread.log
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"sql-commands"},`SQL Commands`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Create Scalar UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Create Aggregate UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Update Scalar UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE OR REPLACE FUNCTION function_name AS OUTPUTTYPE int LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":4},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Update Aggregate UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE OR REPLACE AGGREGATE FUNCTION function_name AS OUTPUTTYPE BUFSIZE buf_size int LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: If keyword `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`AGGREGATE`),` used, the UDF will be treated as aggregate UDF despite what it was before; Similarly, if there is no keyword `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`aggregate`),`, the UDF will be treated as scalar function despite what it was before.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":5},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Show the UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The version of a UDF is increased by one every time it's updated.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select * from ins_functions \\G;     
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":6},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Show and Drop existing UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`SHOW functions;
DROP FUNCTION function_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"more-python-udf-samples"},`More Python UDF Samples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"scalar-function-pybitand"},`Scalar Function `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py"},`pybitand`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pybitand`),` function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pybitand`),` function ignores null values.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pybitand.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
    pass

def process(block):
    (rows, cols) = block.shape()
    result = []
    for i in range(rows):
        r = 2 ** 32 - 1
        for j in range(cols):
            cell = block.data(i,j)
            if cell is None:
                result.append(None)
                break
            else:
                r = r & cell
        else:
            result.append(r)
    return result

def destroy():
    pass

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pybitand.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-function-pyl2norm"},`Aggregate Function `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py"},`pyl2norm`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pyl2norm`),` function finds the second-order norm for all data in the input columns. This squares the values, takes a cumulative sum, and finds the square root.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pyl2norm.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`import json
import math

def init():
    pass

def destroy():
    pass

def start():
    return json.dumps(0.0).encode('utf-8')

def finish(buf):
    sum_squares = json.loads(buf)
    result = math.sqrt(sum_squares)
    return result

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum_squares = json.loads(buf)
    
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i,j)
            if cell is not None:
                sum_squares += cell * cell
    return json.dumps(sum_squares).encode('utf-8') 

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pyl2norm.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"aggregate-function-pycumsum"},`Aggregate Function `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py"},`pycumsum`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pycumsum`),` function finds the cumulative sum for all data in the input columns.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pycumsum.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`import pickle
import numpy as np

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0.0)

def finish(buf):
    return pickle.loads(buf)

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    row = []
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i, j)
            if cell is not None:
                row.append(datablock.data(i, j))
    if len(row) > 1:
        new_state = np.cumsum(row)[-1]
    else:
        new_state = state
    return pickle.dumps(new_state)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pycumsum.py"},`view source code`))),"## Manage and Use UDF You need to add UDF to TDengine before using it in SQL queries. For more information about how to manage UDF and how to invoke UDF, please see [Manage and Use UDF](../taos-sql/udf/).");};MDXContent.isMDXComponent=true;

/***/ })

}]);