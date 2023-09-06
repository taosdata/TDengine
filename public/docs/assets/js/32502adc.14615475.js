"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1170],{

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

/***/ 6810:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={sidebar_label:'用户定义函数',title:'UDF（用户定义函数）',description:'支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。'};const contentTitle=undefined;const metadata={"unversionedId":"develop/udf","id":"develop/udf","title":"UDF（用户定义函数）","description":"支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。","source":"@site/docs/07-develop/09-udf.md","sourceDirName":"07-develop","slug":"/develop/udf","permalink":"/docs/develop/udf","draft":false,"tags":[],"version":"current","sidebarPosition":9,"frontMatter":{"sidebar_label":"用户定义函数","title":"UDF（用户定义函数）","description":"支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。"},"sidebar":"defaultSidebar","previous":{"title":"缓存","permalink":"/docs/develop/cache"},"next":{"title":"连接器","permalink":"/docs/connector/"}};const assets={};const toc=[{value:'用 C 语言实现 UDF',id:'用-c-语言实现-udf',level:2},{value:'用 C 语言实现标量函数',id:'用-c-语言实现标量函数',level:3},{value:'用 C 语言实现聚合函数',id:'用-c-语言实现聚合函数',level:3},{value:'C 语言 UDF 接口函数定义',id:'c-语言-udf-接口函数定义',level:3},{value:'标量函数接口',id:'标量函数接口',level:4},{value:'聚合函数接口',id:'聚合函数接口',level:4},{value:'初始化和销毁接口',id:'初始化和销毁接口',level:4},{value:'C 语言 UDF 数据结构',id:'c-语言-udf-数据结构',level:3},{value:'编译 C UDF',id:'编译-c-udf',level:3},{value:'C UDF 示例代码',id:'c-udf-示例代码',level:3},{value:'标量函数示例 bit_and',id:'标量函数示例-bit_and',level:4},{value:'聚合函数示例1 返回值为数值类型 l2norm',id:'聚合函数示例1-返回值为数值类型-l2norm',level:4},{value:'聚合函数示例2 返回值为字符串类型 max_vol',id:'聚合函数示例2-返回值为字符串类型-max_vol',level:4},{value:'用 Python 语言实现 UDF',id:'用-python-语言实现-udf',level:2},{value:'准备环境',id:'准备环境',level:3},{value:'接口定义',id:'接口定义',level:3},{value:'接口概述',id:'接口概述',level:4},{value:'标量函数接口',id:'标量函数接口-1',level:4},{value:'聚合函数接口',id:'聚合函数接口-1',level:4},{value:'初始化和销毁接口',id:'初始化和销毁接口-1',level:4},{value:'Python UDF 函数模板',id:'python-udf-函数模板',level:3},{value:'标量函数实现模板',id:'标量函数实现模板',level:4},{value:'聚合函数实现模板',id:'聚合函数实现模板',level:4},{value:'数据类型映射',id:'数据类型映射',level:3},{value:'开发指南',id:'开发指南',level:3},{value:'最简单的 UDF',id:'最简单的-udf',level:4},{value:'示例二：异常处理',id:'示例二异常处理',level:4},{value:'示例三： 接收 n 个参数的 UDF',id:'示例三-接收-n-个参数的-udf',level:4},{value:'示例四：使用第三方库',id:'示例四使用第三方库',level:4},{value:'示例五：聚合函数',id:'示例五聚合函数',level:4},{value:'SQL 命令',id:'sql-命令',level:3},{value:'更多 Python UDF 示例代码',id:'更多-python-udf-示例代码',level:3},{value:'标量函数示例 pybitand',id:'标量函数示例-pybitand',level:4},{value:'聚合函数示例 pyl2norm',id:'聚合函数示例-pyl2norm',level:4},{value:'聚合函数示例 pycumsum',id:'聚合函数示例-pycumsum',level:4}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF(User Defined Function) 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。 UDF 通常以数据表中的一列数据做为输入，同时支持以嵌套子查询的结果作为输入。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`用户可以通过 UDF 实现两类函数：标量函数和聚合函数。标量函数对每行数据输出一个值，如求绝对值 abs，正弦函数 sin，字符串拼接函数 concat 等。聚合函数对多行数据进行输出一个值，如求平均数 avg，最大值 max 等。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 支持通过 C/Python 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"用-c-语言实现-udf"},`用 C 语言实现 UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 C 语言实现 UDF 时，需要实现规定的接口函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`标量函数需要实现标量接口函数 scalarfn 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`聚合函数需要实现聚合接口函数 aggfn_start ， aggfn ， aggfn_finish。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`如果需要初始化，实现 udf_init；如果需要清理工作，实现udf_destroy。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`接口函数的名称是 UDF 名称，或者是 UDF 名称和特定后缀（`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_start`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_finish`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_init`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`_destroy`),`)的连接。列表中的scalarfn，aggfn, udf需要替换成udf函数名。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"用-c-语言实现标量函数"},`用 C 语言实现标量函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`标量函数实现模板如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include "taos.h"
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`scalarfn 为函数名的占位符，需要替换成函数名，如bit_and。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"用-c-语言实现聚合函数"},`用 C 语言实现聚合函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`聚合函数的实现模板如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include "taos.h"
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`aggfn为函数名的占位符，需要修改为自己的函数名，如l2norm。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"c-语言-udf-接口函数定义"},`C 语言 UDF 接口函数定义`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`接口函数的名称是 udf 名称，或者是 udf 名称和特定后缀（_start, _finish, _init, _destroy)的连接。以下描述中函数名称中的 scalarfn，aggfn, udf 需要替换成udf函数名。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`接口函数返回值表示是否成功。如果返回值是 TSDB_CODE_SUCCESS，表示操作成功，否则返回的是错误代码。错误代码定义在 taoserror.h，和 taos.h 中的API共享错误码的定义。例如， TSDB_CODE_UDF_INVALID_INPUT 表示输入无效输入。TSDB_CODE_OUT_OF_MEMORY 表示内存不足。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`接口函数参数类型见数据结构定义。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"标量函数接口"},`标量函数接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,` `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)`),` `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,` 其中 scalarFn 是函数名的占位符。这个函数对数据块进行标量计算，通过设置resultColumn结构体中的变量设置值`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`参数的具体含义是：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`inputDataBlock: 输入的数据块`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`resultColumn: 输出列 `)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数接口"},`聚合函数接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn_start(SUdfInterBuf *interBuf)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`其中 aggfn 是函数名的占位符。首先调用aggfn_start生成结果buffer，然后相关的数据会被分为多个行数据块，对每个数据块调用 aggfn 用数据块更新中间结果，最后再调用 aggfn_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`参数的具体含义是：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`interBuf：中间结果 buffer。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`inputBlock：输入的数据块。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`newInterBuf：新的中间结果buffer。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`result：最终结果。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"初始化和销毁接口"},`初始化和销毁接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t udf_init()`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`int32_t udf_destroy()`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`其中 udf 是函数名的占位符。udf_init 完成初始化工作。 udf_destroy 完成清理工作。如果没有初始化工作，无需定义udf_init函数。如果没有清理工作，无需定义udf_destroy函数。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"c-语言-udf-数据结构"},`C 语言 UDF 数据结构`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`typedef struct SUdfColumnMeta {
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`数据结构说明如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfDataBlock 数据块包含行数 numOfRows 和列数 numCols。udfCols`,`[i]`,` (0 <= i <= numCols-1)表示每一列数据，类型为SUdfColumn*。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfColumn 包含列的数据类型定义 colMeta 和列的数据 colData。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfColumnMeta 成员定义同 taos.h 数据类型定义。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfColumnData 数据可以变长，varLenCol 定义变长数据，fixLenCol 定义定长数据。 `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SUdfInterBuf 定义中间结构 buffer，以及 buffer 中结果个数 numOfResult`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`为了更好的操作以上数据结构，提供了一些便利函数，定义在 taosudf.h。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"编译-c-udf"},`编译 C UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`用户定义函数的 C 语言源代码无法直接被 TDengine 系统使用，而是需要先编译为 动态链接库，之后才能载入 TDengine 系统。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`例如，按照上一章节描述的规则准备好了用户定义函数的源代码 bit_and.c，以 Linux 为例可以执行如下指令编译得到动态链接库文件：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`这样就准备好了动态链接库 libbitand.so 文件，可以供后文创建 UDF 时使用了。为了保证可靠的系统运行，编译器 GCC 推荐使用 7.5 及以上版本。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"c-udf-示例代码"},`C UDF 示例代码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"标量函数示例-bit_and"},`标量函数示例 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c"},`bit_and`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`bit_add 实现多列的按位与功能。如果只有一列，返回这一列。bit_add 忽略空值。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"bit_and.c"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include <stdio.h>
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/bit_and.c"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数示例1-返回值为数值类型-l2norm"},`聚合函数示例1 返回值为数值类型 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c"},`l2norm`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`l2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"l2norm.c"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`#include <string.h>
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/l2norm.c"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数示例2-返回值为字符串类型-max_vol"},`聚合函数示例2 返回值为字符串类型 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/max_vol.c"},`max_vol`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`max_vol 实现了从多个输入的电压列中找到最大电压，返回由设备ID + 最大电压所在（行，列）+ 最大电压值 组成的组合字符串值`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`创建表：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`创建自定义函数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C'; 
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用自定义函数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`select max_vol(vol1,vol2,vol3,deviceid) from battery;
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/max_vol.c"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"用-python-语言实现-udf"},`用 Python 语言实现 UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"准备环境"},`准备环境`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`准备好 Python 运行环境 `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`安装 Python 包 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taospyudf`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`pip3 install taospyudf
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`安装过程中会编译 C++ 源码，因此系统上要有 cmake 和 gcc。编译生成的 libtaospyudf.so 文件自动会被复制到 /usr/local/lib/ 目录，因此如果是非 root 用户，安装时需加 sudo。安装完可以检查这个目录是否有了这个文件:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`然后执行命令`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`ldconfig
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`如果 Python UDF 程序执行时，通过 PYTHONPATH 引用其它的包，可以设置 taos.cfg 的 UdfdLdLibPath 变量为PYTHONPATH的内容`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`启动 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosd`),` 服务
细节请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../get-started"},`立即开始`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"接口定义"},`接口定义`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"接口概述"},`接口概述`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 Python 语言实现 UDF 时，需要实现规定的接口函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`标量函数需要实现标量接口函数 process 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`聚合函数需要实现聚合接口函数 start ，reduce ，finish。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`如果需要初始化，实现 init；如果需要清理工作，实现 destroy。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"标量函数接口-1"},`标量函数接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def process(input: datablock) -> tuple[output_type]:
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`说明：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`- input:datablock 类似二维矩阵，通过成员方法 data(row,col)返回位于 row 行，col 列的 python 对象
- 返回值是一个 Python 对象元组，每个元素类型为输出类型。
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数接口-1"},`聚合函数接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`说明：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`首先调用 start 生成最初结果 buffer`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`然后输入数据会被分为多个行数据块，对每个数据块 inputs 和当前中间结果 buf 调用 reduce，得到新的中间结果`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`最后再调用 finish 从中间结果 buf 产生最终输出，最终输出只能含 0 或 1 条数据。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"初始化和销毁接口-1"},`初始化和销毁接口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init()
def destroy()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`说明：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`init 完成初始化工作`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`destroy 完成清理工作`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"python-udf-函数模板"},`Python UDF 函数模板`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"标量函数实现模板"},`标量函数实现模板`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`标量函数实现模版如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:  
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：定义标题函数最重要是要实现 process 函数，同时必须定义 init 和 destroy 函数即使什么都不做`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数实现模板"},`聚合函数实现模板`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`聚合函数实现模版如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：定义聚合函数最重要是要实现  start, reduce 和 finish，且必须定义 init 和 destroy 函数。start 生成最初结果 buffer，然后输入数据会被分为多个行数据块，对每个数据块 inputs 和当前中间结果 buf 调用 reduce，得到新的中间结果，最后再调用 finish 从中间结果 buf 产生最终输出。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"数据类型映射"},`数据类型映射`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`下表描述了TDengine SQL数据类型和Python数据类型的映射。任何类型的NULL值都映射成Python的None值。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"th"},`TDengine SQL数据类型`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"th"},`Python数据类型`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TINYINT / SMALLINT / INT  / BIGINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`FLOAT / DOUBLE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BOOL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BINARY / VARCHAR / NCHAR`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bytes`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TIMESTAMP`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`JSON and other types`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`不支持`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"开发指南"},`开发指南`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本文内容由浅入深包括 4 个示例程序：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`定义一个只接收一个整数的标量函数： 输入 n， 输出 ln(n^2 + 1)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`定义一个接收 n 个整数的标量函数， 输入 （x1, x2, ..., xn）, 输出每个值和它们的序号的乘积的和： x1 + 2 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` x2 + ... + n `),` xn。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`定义一个标量函数，输入一个时间戳，输出距离这个时间最近的下一个周日。完成这个函数要用到第三方库 moment。我们在这个示例中讲解使用第三方库的注意事项。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`定义一个聚合函数，计算某一列最大值和最小值的差,  也就是实现 TDengien 内置的 spread 函数。
同时也包含大量实用的 debug 技巧。
本文假设你用的是 Linux 系统，且已安装好了 TDengine 3.0.4.0+ 和 Python 3.7+。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`UDF 内无法通过 print 函数输出日志，需要自己写文件或用 python 内置的 logging 库写文件`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"最简单的-udf"},`最简单的 UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`编写一个只接收一个整数的 UDF 函数： 输入 n， 输出 ln(n^2 + 1)。
首先编写一个 Python 文件，存在系统某个目录，比如 /root/udf/myfun.py 内容如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from math import log

def init():
    pass

def destroy():
    pass

def process(block):
    rows, _ = block.shape()
    return [log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`这个文件包含 3 个函数， init 和 destroy 都是空函数，它们是 UDF 的生命周期函数，即使什么都不做也要定义。最关键的是 process 函数， 它接受一个数据块，这个数据块对象有两个方法：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`shape() 返回数据块的行数和列数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`data(i, j) 返回 i 行 j 列的数据
标量函数的 process 方法传人的数据块有多少行，就需要返回多少个数据。上述代码中我们忽略的列数，因为我们只想对每行的第一个数做计算。
接下来我们创建对应的 UDF 函数，在 TDengine CLI 中执行下面语句：`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`其输出如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},` taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`看起来很顺利，接下来 show 一下系统中所有的自定义函数，确认创建成功：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`接下来就来测试一下这个函数，测试之前先执行下面的 SQL 命令，制造些测试数据，在 TDengine CLI 中执行下述命令`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`测试 myfun 函数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`不幸的是执行失败了，什么原因呢？
查看 udfd 进程的日志`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`tail -10 /var/log/taos/udfd.log
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`发现以下错误信息：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`错误很明确：没有加载到 Python 插件 libtaospyudf.so，如果遇到此错误，请参考前面的准备环境一节。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`修复环境错误后再次执行，如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`至此，我们完成了第一个 UDF 😊，并学会了简单的 debug 方法。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"示例二异常处理"},`示例二：异常处理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`上面的 myfun 虽然测试测试通过了，但是有两个缺点：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`这个标量函数只接受 1 列数据作为输入，如果用户传入了多列也不会抛异常。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`没有处理 null 值。我们期望如果输入有 null，则会抛异常终止执行。
因此 process 函数改进如下：`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`然后执行下面的语句更新已有的 UDF：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`再传入 myfun 两个参数，就会执行失败了`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`但遗憾的是我们自定义的异常信息没有展示给用户，而是在插件的日志文件 /var/log/taos/taospyudf.log  中：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`至此，我们学会了如何更新 UDF，并查看 UDF 输出的错误日志。
（注：如果 UDF 更新后未生效，在 TDengine 3.0.5.0 以前（不含）的版本中需要重启 taosd，在 3.0.5.0 及之后的版本中不需要重启 taosd 即可生效。）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"示例三-接收-n-个参数的-udf"},`示例三： 接收 n 个参数的 UDF`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`编写一个 UDF：输入（x1, x2, ..., xn）, 输出每个值和它们的序号的乘积的和： 1 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`  x1 + 2 `),` x2 + ... + n * xn。如果 x1 至 xn 中包含 null，则结果为 null。
这个示例与示例一的区别是，可以接受任意多列作为输入，且要处理每一列的值。编写 UDF 文件 /root/udf/nsum.py：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def init():
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`创建 UDF：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`测试 UDF：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> insert into t values('2023-05-25 09:09:15', 6, null, 8);
Insert OK, 1 row(s) affected (0.003675s)

taos> select ts, v1, v2, v3,  nsum(v1, v2, v3) from t;
           ts            |     v1      |     v2      |     v3      |     nsum(v1, v2, v3)      |
================================================================================================
 2023-05-01 12:13:14.000 |           1 |           2 |           3 |              14.000000000 |
 2023-05-03 08:09:10.000 |           2 |           3 |           4 |              20.000000000 |
 2023-05-10 07:06:05.000 |           3 |           4 |           5 |              26.000000000 |
 2023-05-25 09:09:15.000 |           6 |        NULL |           8 |                      NULL |
Query OK, 4 row(s) in set (0.010653s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"示例四使用第三方库"},`示例四：使用第三方库`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`编写一个 UDF，输入一个时间戳，输出距离这个时间最近的下一个周日。比如今天是 2023-05-25， 则下一个周日是 2023-05-28。
完成这个函数要用到第三方库 momen。先安装这个库：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`pip3 install moment
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`然后编写 UDF 文件 /root/udf/nextsunday.py`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import moment


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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`UDF 框架会将 TDengine 的 timestamp 类型映射为 Python 的 int 类型，所以这个函数只接受一个表示毫秒数的整数。process 方法先做参数检查，然后用 moment 包替换时间的星期为星期日，最后格式化输出。输出的字符串长度是固定的10个字符长，因此可以这样创建 UDF 函数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`此时测试函数，如果你是用 systemctl 启动的 taosd，肯定会遇到错误：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},` tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`这是因为 “moment” 所在位置不在 python udf 插件默认的库搜索路径中。怎么确认这一点呢？通过以下命令搜索 taospyudf.log:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`grep 'sys path' taospyudf.log  | tail -1
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`输出如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`发现 python udf 插件默认搜索的第三方库安装路径是： /lib/python3/dist-packages，而 moment 默认安装到了 /usr/local/lib/python3.8/dist-packages。下面我们修改 python udf 插件默认的库搜索路径。
先打开 python3 命令行，查看当前的 sys.path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`复制上面脚本的输出的字符串，然后编辑 /var/taos/taos.cfg 加入以下配置：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`保存后执行 systemctl restart taosd, 再测试就不报错了：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select ts, nextsunday(ts) from t;
           ts            | nextsunday(ts) |
===========================================
 2023-05-01 12:13:14.000 | 2023-05-07     |
 2023-05-03 08:09:10.000 | 2023-05-07     |
 2023-05-10 07:06:05.000 | 2023-05-14     |
 2023-05-25 09:09:15.000 | 2023-05-28     |
Query OK, 4 row(s) in set (1.011474s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"示例五聚合函数"},`示例五：聚合函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`编写一个聚合函数，计算某一列最大值和最小值的差。
聚合函数与标量函数的区别是：标量函数是多行输入对应多个输出，聚合函数是多行输入对应一个输出。聚合函数的执行过程有点像经典的 map-reduce 框架的执行过程，框架把数据分成若干块，每个 mapper 处理一个块，reducer 再把 mapper 的结果做聚合。不一样的地方在于，对于 TDengine Python UDF 中的 reduce 函数既有 map 的功能又有 reduce 的功能。reduce 函数接受两个参数：一个是自己要处理的数据，一个是别的任务执行 reduce 函数的处理结果。如下面的示例 /root/udf/myspread.py:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import io
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在这个示例中我们不光定义了一个聚合函数，还添加记录执行日志的功能，讲解如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`init 函数不再是空函数，而是打开了一个文件用于写执行日志`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`log 函数是记录日志的工具，自动将传入的对象转成字符串，加换行符输出`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`destroy 函数用来在执行结束关闭文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`start 返回了初始的 buffer，用来存聚合函数的中间结果，我们把最大值初始化为负无穷大，最小值初始化为正无穷大`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`reduce 处理每个数据块并聚合结果`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`finish 函数将最终的 buffer 转换成最终的输出
执行下面的 SQL语句创建对应的 UDF：`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`这个 SQL 语句与创建标量函数的 SQL 语句有两个重要区别：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`增加了 aggregate 关键字`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`增加了 bufsize 关键字，用来指定存储中间结果的内存大小，这个数值可以大于实际使用的数值。本例中间结果是两个浮点数组成的 tuple，序列化后实际占用大小只有 32 个字节，但指定的 bufsize 是128，可以用 python 命令行打印实际占用的字节数`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`测试这个函数，可以看到 myspread 的输出结果和内置的 spread 函数的输出结果是一致的。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`taos> select myspread(v1) from t;
       myspread(v1)        |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.013486s)

taos> select spread(v1) from t;
        spread(v1)         |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.005501s)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`最后，查看我们自己打印的执行日志，从日志可以看出，reduce 函数被执行了 3 次。执行过程中 max 值被更新了 4 次， min 值只被更新 1 次。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`root@slave11 /var/log/taos $ cat spread.log
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`通过这个示例，我们学会了如何定义聚合函数，并打印自定义的日志信息。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"sql-命令"},`SQL 命令`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`创建标量函数的语法`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`创建聚合函数的语法`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`更新标量函数`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE OR REPLACE FUNCTION function_name AS OUTPUTTYPE int LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":4},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`更新聚合函数`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE OR REPLACE AGGREGATE FUNCTION function_name AS OUTPUTTYPE BUFSIZE buf_size int LANGUAGE 'Python';
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：如果加了 “AGGREGATE” 关键字，更新之后函数将被当作聚合函数，无论之前是什么类型的函数。相反，如果没有加 “AGGREGATE” 关键字，更新之后的函数将被当作标量函数，无论之前是什么类型的函数。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":5},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`查看函数信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`同名的 UDF 每更新一次，版本号会增加 1。 `))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select * from ins_functions \\G;     
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":6},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`查看和删除已有的 UDF`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`SHOW functions;
DROP FUNCTION function_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`上面的命令可以查看 UDF  的完整信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"更多-python-udf-示例代码"},`更多 Python UDF 示例代码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"标量函数示例-pybitand"},`标量函数示例 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py"},`pybitand`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`pybitand 实现多列的按位与功能。如果只有一列，返回这一列。pybitand 忽略空值。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pybitand.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-Python"},`def init():
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pybitand.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数示例-pyl2norm"},`聚合函数示例 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py"},`pyl2norm`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`pyl2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pyl2norm.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`import json
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pyl2norm.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"聚合函数示例-pycumsum"},`聚合函数示例 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"h4","href":"https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py"},`pycumsum`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`pycumsum 使用 numpy 计算输入列所有数据的累积和。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"pycumsum.py"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-c"},`import pickle
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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/tests/script/sh/pycumsum.py"},`查看源码`))),"## \u7BA1\u7406\u548C\u4F7F\u7528 UDF \u5728\u4F7F\u7528 UDF \u4E4B\u524D\u9700\u8981\u5148\u5C06\u5176\u52A0\u5165\u5230 TDengine \u7CFB\u7EDF\u4E2D\u3002\u5173\u4E8E\u5982\u4F55\u7BA1\u7406\u548C\u4F7F\u7528 UDF\uFF0C\u8BF7\u53C2\u8003[\u7BA1\u7406\u548C\u4F7F\u7528 UDF](../../taos-sql/udf)");};MDXContent.isMDXComponent=true;

/***/ })

}]);