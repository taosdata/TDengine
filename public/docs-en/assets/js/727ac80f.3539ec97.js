"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[4101],{

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

/***/ 5618:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'Database',sidebar_label:'Database',description:'This document describes how to create and perform operations on databases.'};const contentTitle=undefined;const metadata={"unversionedId":"taos-sql/database","id":"taos-sql/database","title":"Database","description":"This document describes how to create and perform operations on databases.","source":"@site/docs/12-taos-sql/02-database.md","sourceDirName":"12-taos-sql","slug":"/taos-sql/database","permalink":"/docs-en/taos-sql/database","draft":false,"tags":[],"version":"current","sidebarPosition":2,"frontMatter":{"title":"Database","sidebar_label":"Database","description":"This document describes how to create and perform operations on databases."},"sidebar":"defaultSidebar","previous":{"title":"Data Types","permalink":"/docs-en/taos-sql/data-type"},"next":{"title":"Table","permalink":"/docs-en/taos-sql/table"}};const assets={};const toc=[{value:'Create a Database',id:'create-a-database',level:2},{value:'Parameters',id:'parameters',level:2},{value:'Example Statement',id:'example-statement',level:3},{value:'Specify the Database in Use',id:'specify-the-database-in-use',level:3},{value:'Drop a Database',id:'drop-a-database',level:2},{value:'Change Database Configuration',id:'change-database-configuration',level:2},{value:'ALTER CACHESIZE',id:'alter-cachesize',level:3},{value:'View Databases',id:'view-databases',level:2},{value:'View All Databases',id:'view-all-databases',level:3},{value:'View the CREATE Statement for a Database',id:'view-the-create-statement-for-a-database',level:3},{value:'View Database Configuration',id:'view-database-configuration',level:3},{value:'Delete Expired Data',id:'delete-expired-data',level:2},{value:'Flush Data',id:'flush-data',level:2},{value:'Redistribute Vgroup',id:'redistribute-vgroup',level:2},{value:'Balance Vgroup',id:'balance-vgroup',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"create-a-database"},`Create a Database`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
    BUFFER value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | COMP {0 | 1 | 2}
  | DURATION value
  | WAL_FSYNC_PERIOD value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | PAGES value
  | PAGESIZE  value
  | PRECISION {'ms' | 'us' | 'ns'}
  | REPLICA value
  | RETENTIONS ingestion_duration:keep_duration ...
  | WAL_LEVEL {1 | 2}
  | VGROUPS value
  | SINGLE_STABLE {0 | 1}
  | STT_TRIGGER value
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | TSDB_PAGESIZE value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"parameters"},`Parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`BUFFER: specifies the size (in MB) of the write buffer for each vnode. Enter a value between 3 and 16384. The default value is 256.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`CACHEMODEL: specifies how the latest data in subtables is stored in the cache. The default value is none.`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`none: The latest data is not cached.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`last_row: The last row of each subtable is cached. This option significantly improves the performance of the LAST_ROW function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`last_value: The last non-null value of each column in each subtable is cached. This option significantly improves the performance of the LAST function under normal circumstances, such as statements including the WHERE, ORDER BY, GROUP BY, and INTERVAL keywords.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`both: The last row of each subtable and the last non-null value of each column in each subtable are cached.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`CACHESIZE: specifies the amount (in MB) of memory used for subtable caching on each vnode. Enter a value between 1 and 65536. The default value is 1.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`COMP: specifies how databases are compressed. The default value is 2.`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`0: Compression is disabled.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`1: One-pass compression is enabled.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`2: Two-pass compression is enabled.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`DURATION: specifies the time period contained in each data file. After the time specified by this parameter has elapsed, TDengine creates a new data file to store incoming data. You can use m (minutes), h (hours), and d (days) as the unit, for example DURATION 100h or DURATION 10d. If you do not include a unit, d is used by default.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`WAL_FSYNC_PERIOD: specifies the interval (in milliseconds) at which data is written from the WAL to disk. This parameter takes effect only when the WAL parameter is set to 2. The default value is 3000. Enter a value between 0 and 180000. The value 0 indicates that incoming data is immediately written to disk.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`MAXROWS: specifies the maximum number of rows recorded in a block. The default value is 4096.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`MINROWS: specifies the minimum number of rows recorded in a block. The default value is 100.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`KEEP: specifies the time for which data is retained. Enter a value between 1 and 365000. The default value is 3650. The value of the KEEP parameter must be greater than or equal to the value of the DURATION parameter. TDengine automatically deletes data that is older than the value of the KEEP parameter. You can use m (minutes), h (hours), and d (days) as the unit, for example KEEP 100h or KEEP 10d. If you do not include a unit, d is used by default. TDengine Enterprise supports `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.tdengine.com/tdinternal/arch/#tiered-storage"},`Tiered Storage`),` function, thus multiple KEEP values (comma separated and up to 3 values supported, and meet keep 0 <= keep 1 <= keep 2, e.g. KEEP 100h,100d,3650d) are supported; TDengine OSS does not support Tiered Storage function (although multiple keep values are configured, they do not take effect, only the maximum keep value is used as KEEP).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`PAGES: specifies the number of pages in the metadata storage engine cache on each vnode. Enter a value greater than or equal to 64. The default value is 256. The space occupied by metadata storage on each vnode is equal to the product of the values of the PAGESIZE and PAGES parameters. The space occupied by default is 1 MB.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`PAGESIZE: specifies the size (in KB) of each page in the metadata storage engine cache on each vnode. The default value is 4. Enter a value between 1 and 16384.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`PRECISION: specifies the precision at which a database records timestamps. Enter ms for milliseconds, us for microseconds, or ns for nanoseconds. The default value is ms.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`REPLICA: specifies the number of replicas that are made of the database. Enter 1 or 3. The default value is 1. The value of the REPLICA parameter cannot exceed the number of dnodes in the cluster.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`RETENTIONS: specifies the retention period for data aggregated at various intervals. For example, RETENTIONS 15s:7d,1m:21d,15m:50d indicates that data aggregated every 15 seconds is retained for 7 days, data aggregated every 1 minute is retained for 21 days, and data aggregated every 15 minutes is retained for 50 days. You must enter three aggregation intervals and corresponding retention periods.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`WAL_LEVEL: specifies whether fsync is enabled. The default value is 1.`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`1: WAL is enabled but fsync is disabled.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`2: WAL and fsync are both enabled.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`VGROUPS: specifies the initial number of vgroups when a database is created.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SINGLE_STABLE: specifies whether the database can contain more than one supertable.`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`0: The database can contain multiple supertables.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`1: The database can contain only one supertable.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`STT_TRIGGER: specifies the number of file merges triggered by flushed files. The default is 8, ranging from 1 to 16. For high-frequency scenarios with few tables, it is recommended to use the default configuration or a smaller value for this parameter; For multi-table low-frequency scenarios, it is recommended to configure this parameter with a larger value.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TABLE_PREFIX: The prefix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the prefix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "0001" is used if TSDB_PREFIX is set to 2 but "v3" is used if TSDB_PREFIX is set to -2; It can help you to control the distribution of tables.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TABLE_SUFFIX: The suffix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the suffix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "v300" is used if TSDB_SUFFIX is set to 2 but "01" is used if TSDB_SUFFIX is set to -2; It can help you to control the distribution of tables. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TSDB_PAGESIZE: The page size of the data storage engine in a vnode. The unit is KB. The default is 4 KB. The range is 1 to 16384, that is, 1 KB to 16 MB.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`WAL_RETENTION_PERIOD: specifies the maximum time of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a time in seconds. The default value is 3600, which means the data in latest 3600 seconds will be kept in WAL for data subscription. Please adjust this parameter to a more proper value for your data subscription.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`WAL_RETENTION_SIZE: specifies the maximum total size of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a size in KB. The default value is 0. A value of 0 indicates that the total size of WAL files to keep for consumption has no upper limit.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"example-statement"},`Example Statement`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`create database if not exists db vgroups 10 buffer 10

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement creates a database named db that has 10 vgroups and whose vnodes have a 10 MB cache.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"specify-the-database-in-use"},`Specify the Database in Use`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`USE db_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement switches to the specified database. (If you connect to TDengine over the REST API, this statement does not take effect.)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"drop-a-database"},`Drop a Database`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`DROP DATABASE [IF EXISTS] db_name
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement deletes the specified database. This statement will delete all tables in the database and destroy all vgroups associated with it. Exercise caution when using this statement.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"change-database-configuration"},`Change Database Configuration`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`ALTER DATABASE db_name [alter_database_options]

alter_database_options:
    alter_database_option ...

alter_database_option: {
    CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | BUFFER value
  | PAGES value
  | REPLICA value
  | STT_TRIGGER value
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"alter-cachesize"},`ALTER CACHESIZE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The command of changing database configuration parameters is easy to use, but it's hard to determine whether a parameter is proper or not. In this section we will describe how to determine whether cachesize is big enough.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`How to check cachesize?`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can use  `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`select * from information_schema.ins_databases;`),` to get the value of cachesize.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`How to check cacheload?`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`show <db_name>.vgroups;`),` to check the value of cacheload.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Determine whether cachesize is big engough`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the value of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cacheload`),` is very close to the value of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cachesize`),`, then it's very probably that `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cachesize`),` is too small. If the value of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cacheload`),` is much smaller than the value of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cachesize`),`, then `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cachesize`),` is big enough. You can use this simple principle to determine. Depending on how much memory is available in your system, you can choose to double `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cachesize`),` or incrase it by even 5 or more times.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`Other parameters cannot be modified after the database has been created.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"view-databases"},`View Databases`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"view-all-databases"},`View All Databases`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SHOW DATABASES;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"view-the-create-statement-for-a-database"},`View the CREATE Statement for a Database`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`SHOW CREATE DATABASE db_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement can be used in migration scenarios. This command can be used to get the CREATE statement, which can be used in another TDengine instance to create the exact same database.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"view-database-configuration"},`View Database Configuration`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='DBNAME' \\G;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement shows the value of each parameter for the specified database. One value is displayed per line.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"delete-expired-data"},`Delete Expired Data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`TRIM DATABASE db_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The preceding SQL statement deletes data that has expired and orders the remaining data in accordance with the storage configuration.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"flush-data"},`Flush Data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`FLUSH DATABASE db_name;
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Flush data from memory onto disk. Before shutting down a node, executing this command can avoid data restore after restarting and speed up the startup process.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"redistribute-vgroup"},`Redistribute Vgroup`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Adjust the distribution of vnodes in the vgroup according to the given list of dnodes. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"balance-vgroup"},`Balance Vgroup`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`BALANCE VGROUP
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Automatically adjusts the distribution of vnodes in all vgroups of the cluster, which is equivalent to load balancing the data of the cluster at the vnode level.`));};MDXContent.isMDXComponent=true;

/***/ })

}]);