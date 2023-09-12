"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9660],{

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

/***/ 2630:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'Time-Series Extensions',sidebar_label:'Time-Series Extensions',description:'This document describes the extended functions specific to time-series data processing available in TDengine.'};const contentTitle=undefined;const metadata={"unversionedId":"taos-sql/distinguished","id":"taos-sql/distinguished","title":"Time-Series Extensions","description":"This document describes the extended functions specific to time-series data processing available in TDengine.","source":"@site/docs/12-taos-sql/12-distinguished.md","sourceDirName":"12-taos-sql","slug":"/taos-sql/distinguished","permalink":"/docs-en/taos-sql/distinguished","draft":false,"tags":[],"version":"current","sidebarPosition":12,"frontMatter":{"title":"Time-Series Extensions","sidebar_label":"Time-Series Extensions","description":"This document describes the extended functions specific to time-series data processing available in TDengine."},"sidebar":"defaultSidebar","previous":{"title":"Functions","permalink":"/docs-en/taos-sql/function"},"next":{"title":"Data Subscription","permalink":"/docs-en/taos-sql/tmq"}};const assets={};const toc=[{value:'Partitioned Queries',id:'partitioned-queries',level:2},{value:'Windowed Queries',id:'windowed-queries',level:2},{value:'Other Rules',id:'other-rules',level:3},{value:'Window Pseudocolumns',id:'window-pseudocolumns',level:3},{value:'FILL Clause',id:'fill-clause',level:3},{value:'Time Window',id:'time-window',level:3},{value:'State Window',id:'state-window',level:3},{value:'Session Window',id:'session-window',level:3},{value:'Event Window',id:'event-window',level:3},{value:'Examples',id:'examples',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As a purpose-built database for storing and processing time-series data, TDengine provides time-series-specific extensions to standard SQL.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`These extensions include partitioned queries and windowed queries.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"partitioned-queries"},`Partitioned Queries`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`When you query a supertable, you may need to partition the supertable by some dimensions and perform additional operations on a specific partition. In this case, you can use the following SQL clause:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`PARTITION BY part_list
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`part_list can be any scalar expression, such as a column, constant, scalar function, or a combination of the preceding items.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A PARTITION BY clause is processed as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The PARTITION BY clause must occur after the WHERE clause`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The PARTITION BY clause partitions the data according to the specified dimensions, then perform computation on each partition. The performed computation is determined by the rest of the statement - a window clause, GROUP BY clause, or SELECT clause.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The PARTITION BY clause can be used together with a window clause or GROUP BY clause. In this case, the window or GROUP BY clause takes effect on every partition. For example, the following statement partitions the table by the location tag, performs downsampling over a 10 minute window, and returns the maximum value:`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select max(current) from meters partition by location interval(10m)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The most common usage of PARTITION BY is partitioning the data in subtables by tags then perform computation when querying data in a supertable. More specifically, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`PARTITION BY TBNAME`),` partitions the data of each subtable into a single timeline, and this method facilitates the statistical analysis in many use cases of processing timeseries data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"windowed-queries"},`Windowed Queries`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Aggregation by time window is supported in TDengine. For example, in the case where temperature sensors report the temperature every seconds, the average temperature for every 10 minutes can be retrieved by performing a query with a time window. Window related clauses are used to divide the data set to be queried into subsets and then aggregation is performed across the subsets. There are four kinds of windows: time window, status window, session window, and event window. There are two kinds of time windows: sliding window and flip time/tumbling window. The syntax of window clause is as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval [, offset]) [SLIDING sliding] [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following restrictions apply:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"other-rules"},`Other Rules`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The window clause must occur after the PARTITION BY clause. It cannot be used with a GROUP BY clause.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SELECT clauses on windows can contain only the following expressions:`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Constants`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Aggregate functions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Expressions that include the preceding expressions.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The window clause cannot be used with a GROUP BY clause.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`WHERE`),` clause can be used to specify the starting and ending time and other filter conditions`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"window-pseudocolumns"},`Window Pseudocolumns`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`_`,`WSTART, `,`_`,`WEND, and `,`_`,`WDURATION`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,`_`,`WSTART, `,`_`,`WEND, and `,`_`,`WDURATION pseudocolumns indicate the beginning, end, and duration of a window.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`These pseudocolumns occur after the aggregation clause.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"fill-clause"},`FILL Clause`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`FILL`),` clause is used to specify how to fill when there is data missing in any window, including:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NONE: No fill (the default fill mode)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`VALUE: Fill with a fixed value, which should be specified together, for example `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL(VALUE, 1.23)`),` Note: The value filled depends on the data type. For example, if you run FILL(VALUE 1.23) on an integer column, the value 1 is filled.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`PREV: Fill with the previous non-NULL value, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL(PREV)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NULL: Fill with NULL, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL(NULL)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`LINEAR: Fill with the closest non-NULL value, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL(LINEAR)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NEXT: Fill with the next non-NULL value, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL(NEXT)`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In the above filling modes, except for `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NONE`),` mode, the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fill`),` clause will be ignored if there is no data in the defined time range, i.e. no data would be filled and the query result would be empty. This behavior is reasonable when the filling mode is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`PREV`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NEXT`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`LINEAR`),`, because filling can't be performed if there is not any data. For filling modes `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NULL`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`VALUE`),`, however, filling can be performed even though there is not any data, filling or not depends on the choice of user's application.  To accomplish the need of this force filling behavior and not break the behavior of existing filling modes, TDengine added two new filling modes since version 3.0.3.0. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NULL_F: Fill `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL`),` by force`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`VALUE_F: Fill `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE`),` by force`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The detailed beaviors of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NULL`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`NULL_F`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`VALUE`),`, and VALUE_F are described below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`When used with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`INTERVAL`),`: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL_F`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE_F`),` are filling by force; `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE`),`  don't fill by force. The behavior of each filling mode is exactly same as what the name suggests.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`When used with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`INTERVAL`),` in stream processing: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL_F`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL`),` are same, i.e. don't fill by force; `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE_F`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE`),` and same, i.e. don't fill by force. It's suggested that there is no filling by force in stream processing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`When used with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`INTERP`),`: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`NULL_F`),` and same, i.e. filling by force; `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`VALUE_F`),` are same, i.e. filling by force. It's suggested that there is always filling by force when used with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`INTERP`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"info"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`A huge volume of interpolation output may be returned using `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL`),`, so it's recommended to specify the time range when using `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FILL`),`. The maximum number of interpolation values that can be returned in a single query is 10,000,000.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`The result set is in ascending order of timestamp when you aggregate by time window.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If aggregate by window is used on STable, the aggregate function is performed on all the rows matching the filter conditions. If `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`PARTITION BY`),` is not used in the query, the result set will be returned in strict ascending order of timestamp; otherwise the result set will be returned in the order of ascending timestamp in each group.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"time-window"},`Time Window`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are two kinds of time windows: sliding window and flip time/tumbling window.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),` clause is used to generate time windows of the same time interval. The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`SLIDING`),` parameter is used to specify the time step for which the time window moves forward. The query is performed on one time window each time, and the time window moves forward with time. When defining a continuous query, both the size of the time window and the step of forward sliding time need to be specified. As shown in the figure blow, `,`[t0s, t0e]`,`, `,`[t1s, t1e]`,`, `,`[t2s, t2e]`,` are respectively the time ranges of three time windows on which continuous queries are executed. The time step for which time window moves forward is marked by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sliding time`),`. Query, filter and aggregate operations are executed on each time window respectively. When the time step specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`SLIDING`),` is same as the time interval specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),`, the sliding time window is actually a flip time/tumbling window.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database Time Window",src:(__webpack_require__(9562)/* ["default"] */ .Z),width:"2276",height:"1077"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`SLIDING`),` should be used with aggregate functions and select functions. The SQL statement below is illegal because no aggregate or selection function is used with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT * FROM temp_tb_1 INTERVAL(1m);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The time step specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`SLIDING`),` cannot exceed the time interval specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),`. The SQL statement below is illegal because the time length specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`SLIDING`),` exceeds that specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`INTERVAL`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`When using time windows, note the following:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The window length for aggregation depends on the value of INTERVAL. The minimum interval is 10 ms. You can configure a window as an offset from UTC 0:00. The offset cannot be smaller than the interval. You can use SLIDING to specify the length of time that the window moves forward.
Please note that the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`timezone`),` parameter should be configured to be the same value in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` configuration file on client side and server side.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The result set is in ascending order of timestamp when you aggregate by time window.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"state-window"},`State Window`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In case of using integer, bool, or string to represent the status of a device at any given moment, continuous rows with the same status belong to a status window. Once the status changes, the status window closes. As shown in the following figure, there are two state windows according to status, `,`[2019-04-28 14:22:07, 2019-04-28 14:22:10]`,` and `,`[2019-04-28 14:22:11, 2019-04-28 14:22:12]`,`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database Status Window",src:(__webpack_require__(4075)/* ["default"] */ .Z),width:"580",height:"177"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`STATE_WINDOW`),` is used to specify the column on which the status window will be based. For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Only care about the information of the status window when the status is 2. For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT * FROM (SELECT COUNT(*) AS cnt, FIRST(ts) AS fst, status FROM temp_tb_1 STATE_WINDOW(status)) t WHERE status = 2;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine also supports the use of CASE expressions in state quantities. It can express that the beginning of a state is triggered by meeting a certain condition, and the end of this state is triggered by meeting another condition. For example, if the normal voltage range of the smart meter is 205V to 235V, you can judge whether the circuit is normal by monitoring the voltage.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT tbname, _wstart, CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END status FROM meters PARTITION BY tbname STATE_WINDOW(CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"session-window"},`Session Window`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The primary key, i.e. timestamp, is used to determine which session window a row belongs to. As shown in the figure below, if the limit of time interval for the session window is specified as 12 seconds, then the 6 rows in the figure constitutes 2 time windows, `,`[2019-04-28 14:22:10, 2019-04-28 14:22:30]`,` and `,`[2019-04-28 14:23:10, 2019-04-28 14:23:30]`,` because the time difference between 2019-04-28 14:22:30 and 2019-04-28 14:23:10 is 40 seconds, which exceeds the time interval limit of 12 seconds.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database Session Window",src:(__webpack_require__(8614)/* ["default"] */ .Z),width:"589",height:"175"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the time interval between two continuous rows are within the time interval specified by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`tol_value`),` they belong to the same session window; otherwise a new session window is started automatically. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`
SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, tol_val);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"event-window"},`Event Window`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Event window is determined according to the window start condition and the window close condition. The window is started when `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`start_trigger_condition`),` is evaluated to true, the window is closed when `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`end_trigger_condition`),` is evaluated to true. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`start_trigger_condition`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`end_trigger_condition`),` can be any conditional expressions supported by TDengine and can include multiple columns.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There may be only one row of data in an event window, when a row meets both the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`start_trigger_condition`),` and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`end_trigger_condition`),`. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The window is treated as invalid or non-existing if the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`end_trigger_condition`),` can't be met. There will be no output in case that a window can't be closed. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the event window query is performed on a super table, TDengine consolidates all the data of all child tables into a single timeline then perform event window based query.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If you want to perform event window based query on the result set of a sub-query, the result set of the sub-query should be arranged in the order of timestamp and include the column of timestamp.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example, the diagram below illustrates the event windows generated by the query below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select _wstart, _wend, count(*) from t event_window start with c1 > 0 end with c2 < 10 
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"Event Window Illustration",src:(__webpack_require__(6333)/* ["default"] */ .Z),width:"909",height:"522"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"examples"},`Examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A table of intelligent meters can be created by the SQL statement below:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The average current, maximum current and median of current in every 10 minutes for the past 24 hours can be calculated using the SQL statement below, with missing values filled with the previous non-NULL values. The query statement is as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 6333:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/event_window-39853742a08cd1c44351f42ca5ac2265.webp");

/***/ }),

/***/ 9562:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 8614:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 4075:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/timewindow-3-39adcc99799ac60a5fb375141e2c8884.webp");

/***/ })

}]);