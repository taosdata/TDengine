"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9660],{

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

/***/ 2630:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={sidebar_label:'特色查询',title:'特色查询',description:'TDengine 提供的时序数据特有的查询功能'};const contentTitle=undefined;const metadata={"unversionedId":"taos-sql/distinguished","id":"taos-sql/distinguished","title":"特色查询","description":"TDengine 提供的时序数据特有的查询功能","source":"@site/docs/12-taos-sql/12-distinguished.md","sourceDirName":"12-taos-sql","slug":"/taos-sql/distinguished","permalink":"/docs/taos-sql/distinguished","draft":false,"tags":[],"version":"current","sidebarPosition":12,"frontMatter":{"sidebar_label":"特色查询","title":"特色查询","description":"TDengine 提供的时序数据特有的查询功能"},"sidebar":"defaultSidebar","previous":{"title":"函数","permalink":"/docs/taos-sql/function"},"next":{"title":"数据订阅","permalink":"/docs/taos-sql/tmq"}};const assets={};const toc=[{value:'数据切分查询',id:'数据切分查询',level:2},{value:'窗口切分查询',id:'窗口切分查询',level:2},{value:'窗口子句的规则',id:'窗口子句的规则',level:3},{value:'FILL 子句',id:'fill-子句',level:3},{value:'时间窗口',id:'时间窗口',level:3},{value:'状态窗口',id:'状态窗口',level:3},{value:'会话窗口',id:'会话窗口',level:3},{value:'事件窗口',id:'事件窗口',level:3},{value:'时间戳伪列',id:'时间戳伪列',level:3},{value:'示例',id:'示例',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 在支持标准 SQL 的基础之上，还提供了一系列满足时序业务场景需求的特色查询语法，这些语法能够为时序场景的应用的开发带来极大的便利。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 提供的特色查询包括数据切分查询和时间窗口切分查询。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"数据切分查询"},`数据切分查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`当需要按一定的维度对数据进行切分然后在切分出的数据空间内再进行一系列的计算时使用数据切分子句，数据切分语句的语法如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`PARTITION BY part_list
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`part_list 可以是任意的标量表达式，包括列、常量、标量函数和它们的组合。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 按如下方式处理数据切分子句：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`数据切分子句位于 WHERE 子句之后。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`数据切分子句将表数据按指定的维度进行切分，每个切分的分片进行指定的计算。计算由之后的子句定义（窗口子句、GROUP BY 子句或 SELECT 子句）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`数据切分子句可以和窗口切分子句（或 GROUP BY 子句）一起使用，此时后面的子句作用在每个切分的分片上。例如，将数据按标签 location 进行分组，并对每个组按 10 分钟进行降采样，取其最大值。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select max(current) from meters partition by location interval(10m)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`数据切分子句最常见的用法就是在超级表查询中，按标签将子表数据进行切分，然后分别进行计算。特别是 PARTITION BY TBNAME 用法，它将每个子表的数据独立出来，形成一条条独立的时间序列，极大的方便了各种时序场景的统计分析。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"窗口切分查询"},`窗口切分查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 支持按时间窗口切分方式进行聚合结果查询，比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值。这种场景下可以使用窗口子句来获得需要的查询结果。窗口子句用于针对查询的数据集合按照窗口切分成为查询子集并进行聚合，窗口包含时间窗口（time window）、状态窗口（status window）、会话窗口（session window）、事件窗口（event window）四种窗口。其中时间窗口又可划分为滑动时间窗口和翻转时间窗口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`窗口子句语法如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在上述语法中的具体限制如下`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"窗口子句的规则"},`窗口子句的规则`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`窗口子句位于数据切分子句之后，不可以和 GROUP BY 子句一起使用。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`窗口子句将数据按窗口进行切分，对每个窗口进行 SELECT 列表中的表达式的计算，SELECT 列表中的表达式只能包含：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`常量。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`_wstart伪列、_wend伪列和_wduration伪列。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`聚集函数（包括选择函数和可以由参数确定输出行数的时序特有函数）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`包含上面表达式的表达式。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`且至少包含一个聚集函数。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`窗口子句不可以和 GROUP BY 子句一起使用。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`WHERE 语句可以指定查询的起止时间和其他过滤条件。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"fill-子句"},`FILL 子句`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`FILL 语句指定某一窗口区间数据缺失的情况下的填充模式。填充模式包括以下几种：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`不进行填充：NONE（默认填充模式）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`VALUE 填充：固定值填充，此时需要指定填充的数值。例如：FILL(VALUE, 1.23)。这里需要注意，最终填充的值受由相应列的类型决定，如 FILL(VALUE, 1.23)，相应列为 INT 类型，则填充值为 1。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`PREV 填充：使用前一个非 NULL 值填充数据。例如：FILL(PREV)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NULL 填充：使用 NULL 填充数据。例如：FILL(NULL)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`LINEAR 填充：根据前后距离最近的非 NULL 值做线性插值填充。例如：FILL(LINEAR)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NEXT 填充：使用下一个非 NULL 值填充数据。例如：FILL(NEXT)。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`以上填充模式中，除了 NONE 模式默认不填充值之外，其他模式在查询的整个时间范围内如果没有数据 FILL 子句将被忽略，即不产生填充数据，查询结果为空。这种行为在部分模式（PREV、NEXT、LINEAR）下具有合理性，因为在这些模式下没有数据意味着无法产生填充数值。而对另外一些模式（NULL、VALUE）来说，理论上是可以产生填充数值的，至于需不需要输出填充数值，取决于应用的需求。所以为了满足这类需要强制填充数据或 NULL 的应用的需求，同时不破坏现有填充模式的行为兼容性，从 3.0.3.0 版本开始，增加了两种新的填充模式：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":7},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`NULL_F: 强制填充 NULL 值 `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`VALUE_F: 强制填充 VALUE 值`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`NULL, NULL_F, VALUE, VALUE_F 这几种填充模式针对不同场景区别如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`INTERVAL 子句： NULL_F, VALUE_F 为强制填充模式；NULL, VALUE 为非强制模式。在这种模式下下各自的语义与名称相符`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`流计算中的 INTERVAL 子句：NULL_F 与 NULL 行为相同，均为非强制模式；VALUE_F 与 VALUE 行为相同，均为非强制模式。即流计算中的 INTERVAL 没有强制模式`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`INTERP 子句：NULL 与 NULL_F 行为相同，均为强制模式；VALUE 与 VALUE_F 行为相同，均为强制模式。即 INTERP 中没有非强制模式。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"info"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`使用 FILL 语句的时候可能生成大量的填充输出，务必指定查询的时间区间。针对每次查询，系统可返回不超过 1 千万条具有插值的结果。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在时间维度聚合中，返回的结果中时间序列严格单调递增。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如果查询对象是超级表，则聚合函数会作用于该超级表下满足值过滤条件的所有表的数据。如果查询中没有使用 PARTITION BY 语句，则返回的结果按照时间序列严格单调递增；如果查询中使用了 PARTITION BY 语句分组，则返回结果中每个 PARTITION 内按照时间序列严格单调递增。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"时间窗口"},`时间窗口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`时间窗口又可分为滑动时间窗口和翻转时间窗口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`INTERVAL 子句用于产生相等时间周期的窗口，SLIDING 用以指定窗口向前滑动的时间。每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。在定义连续查询的时候需要指定时间窗口（time window ）大小和每次前向增量时间（forward sliding times）。如图，`,`[t0s, t0e]`,` ，`,`[t1s , t1e]`,`， `,`[t2s, t2e]`,` 是分别是执行三次连续查询的时间窗口范围，窗口的前向滑动的时间范围 sliding time 标识 。查询过滤、聚合等操作按照每个时间窗口为独立的单位执行。当 SLIDING 与 INTERVAL 相等的时候，滑动窗口即为翻转窗口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database 时间窗口示意图",src:(__webpack_require__(8745)/* ["default"] */ .Z),width:"2276",height:"1077"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`INTERVAL 和 SLIDING 子句需要配合聚合和选择函数来使用。以下 SQL 语句非法：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT * FROM temp_tb_1 INTERVAL(1m);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`SLIDING 的向前滑动的时间不能超过一个窗口的时间范围。以下语句非法：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用时间窗口需要注意：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`聚合时间段的窗口宽度由关键词 INTERVAL 指定，最短时间间隔 10 毫秒（10a）；并且支持偏移 offset（偏移必须小于间隔），也即时间窗口划分与“UTC 时刻 0”相比的偏移量。SLIDING 语句用于指定聚合时间段的前向增量，也即每次窗口向前滑动的时长。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`使用 INTERVAL 语句时，除非极特殊的情况，都要求把客户端和服务端的 taos.cfg 配置文件中的 timezone 参数配置为相同的取值，以避免时间处理函数频繁进行跨时区转换而导致的严重性能影响。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`返回的结果中时间序列严格单调递增。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"状态窗口"},`状态窗口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用整数（布尔值）或字符串来标识产生记录时候设备的状态量。产生的记录如果具有相同的状态量数值则归属于同一个状态窗口，数值改变后该窗口关闭。如下图所示，根据状态量确定的状态窗口分别是`,`[2019-04-28 14:22:07，2019-04-28 14:22:10]`,`和`,`[2019-04-28 14:22:11，2019-04-28 14:22:12]`,`两个。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database 时间窗口示意图",src:(__webpack_require__(3590)/* ["default"] */ .Z),width:"580",height:"177"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 STATE_WINDOW 来确定状态窗口划分的列。例如：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`仅关心 status 为 2 时的状态窗口的信息。例如：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT * FROM (SELECT COUNT(*) AS cnt, FIRST(ts) AS fst, status FROM temp_tb_1 STATE_WINDOW(status)) t WHERE status = 2;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 还支持将 CASE 表达式用在状态量，可以表达某个状态的开始是由满足某个条件而触发，这个状态的结束是由另外一个条件满足而触发的语义。例如，智能电表的电压正常范围是 205V 到 235V，那么可以通过监控电压来判断电路是否正常。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT tbname, _wstart, CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END status FROM meters PARTITION BY tbname STATE_WINDOW(CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"会话窗口"},`会话窗口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`会话窗口根据记录的时间戳主键的值来确定是否属于同一个会话。如下图所示，如果设置时间戳的连续的间隔小于等于 12 秒，则以下 6 条记录构成 2 个会话窗口，分别是：`,`[2019-04-28 14:22:10，2019-04-28 14:22:30]`,`和`,`[2019-04-28 14:23:10，2019-04-28 14:23:30]`,`。因为 2019-04-28 14:22:30 与 2019-04-28 14:23:10 之间的时间间隔是 40 秒，超过了连续时间间隔（12 秒）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database 时间窗口示意图",src:(__webpack_require__(7635)/* ["default"] */ .Z),width:"589",height:"175"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 tol_value 时间间隔范围内的结果都认为归属于同一个窗口，如果连续的两条记录的时间超过 tol_val，则自动开启下一个窗口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`
SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, tol_val);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"事件窗口"},`事件窗口`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`事件窗口根据开始条件和结束条件来划定窗口，当start_trigger_condition满足时则窗口开始，直到end_trigger_condition满足时窗口关闭。start_trigger_condition和end_trigger_condition可以是任意 TDengine 支持的条件表达式，且可以包含不同的列。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`事件窗口可以仅包含一条数据。即当一条数据同时满足start_trigger_condition和end_trigger_condition，且当前不在一个窗口内时，这条数据自己构成了一个窗口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`事件窗口无法关闭时，不构成一个窗口，不会被输出。即有数据满足start_trigger_condition，此时窗口打开，但后续数据都不能满足end_trigger_condition，这个窗口无法被关闭，这部分数据不够成一个窗口，不会被输出。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`如果直接在超级表上进行事件窗口查询，TDengine 会将超级表的数据汇总成一条时间线，然后进行事件窗口的计算。
如果需要对子查询的结果集进行事件窗口查询，那么子查询的结果集需要满足按时间线输出的要求，且可以输出有效的时间戳列。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`以下面的 SQL 语句为例，事件窗口切分如图所示：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`select _wstart, _wend, count(*) from t event_window start with c1 > 0 end with c2 < 10 
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database 事件窗口示意图",src:(__webpack_require__(6926)/* ["default"] */ .Z),width:"908",height:"483"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"时间戳伪列"},`时间戳伪列`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`窗口聚合查询结果中，如果 SQL 语句中没有指定输出查询结果中的时间戳列，那么最终结果中不会自动包含窗口的时间列信息。如果需要在结果中输出聚合结果所对应的时间窗口信息，需要在 SELECT 子句中使用时间戳相关的伪列: 时间窗口起始时间 (`,`_`,`WSTART), 时间窗口结束时间 (`,`_`,`WEND), 时间窗口持续时间 (`,`_`,`WDURATION), 以及查询整体窗口相关的伪列: 查询窗口起始时间(`,`_`,`QSTART) 和查询窗口结束时间(`,`_`,`QEND)。需要注意的是时间窗口起始时间和结束时间均是闭区间，时间窗口持续时间是数据当前时间分辨率下的数值。例如，如果当前数据库的时间分辨率是毫秒，那么结果中 500 就表示当前时间窗口的持续时间是 500毫秒 (500 ms)。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"示例"},`示例`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`智能电表的建表语句如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`针对智能电表采集的数据，以 10 分钟为一个阶段，计算过去 24 小时的电流数据的平均值、最大值、电流的中位数。如果没有计算值，用前一个非 NULL 值填充。使用的查询语句如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SELECT _WSTART, _WEND, AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 6926:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/event_window-d1103d9a3a0bc58219d972bc18a1d89e.webp");

/***/ }),

/***/ 8745:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 7635:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 3590:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/timewindow-3-39adcc99799ac60a5fb375141e2c8884.webp");

/***/ })

}]);