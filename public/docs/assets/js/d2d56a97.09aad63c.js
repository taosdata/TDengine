"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9325],{

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

/***/ 5162:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {


// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "Z": () => (/* binding */ TabItem)
});

// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/clsx/dist/clsx.m.js
var clsx_m = __webpack_require__(6010);
;// CONCATENATED MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/TabItem/styles.module.css
// extracted by mini-css-extract-plugin
/* harmony default export */ const styles_module = ({"tabItem":"tabItem_Ymn6"});
;// CONCATENATED MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/TabItem/index.js
/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */function TabItem(_ref){let{children,hidden,className}=_ref;return/*#__PURE__*/react.createElement("div",{role:"tabpanel",className:(0,clsx_m/* default */.Z)(styles_module.tabItem,className),hidden},children);}

/***/ }),

/***/ 4866:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {


// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "Z": () => (/* binding */ Tabs)
});

// EXTERNAL MODULE: ./node_modules/@docusaurus/core/node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(3117);
// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/clsx/dist/clsx.m.js
var clsx_m = __webpack_require__(6010);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-common/lib/utils/scrollUtils.js
var scrollUtils = __webpack_require__(2466);
// EXTERNAL MODULE: ./node_modules/react-router/esm/react-router.js
var react_router = __webpack_require__(6550);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-common/lib/utils/historyUtils.js
var historyUtils = __webpack_require__(1980);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-common/lib/utils/jsUtils.js
var jsUtils = __webpack_require__(7392);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-common/lib/utils/storageUtils.js
var storageUtils = __webpack_require__(12);
;// CONCATENATED MODULE: ./node_modules/@docusaurus/theme-common/lib/utils/tabsUtils.js
/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */// A very rough duck type, but good enough to guard against mistakes while
// allowing customization
function isTabItem(comp){const{props}=comp;return!!props&&typeof props==='object'&&'value'in props;}function ensureValidChildren(children){return react.Children.map(children,child=>{// Pass falsy values through: allow conditionally not rendering a tab
if(!child||/*#__PURE__*/(0,react.isValidElement)(child)&&isTabItem(child)){return child;}// child.type.name will give non-sensical values in prod because of
// minification, but we assume it won't throw in prod.
throw new Error(`Docusaurus error: Bad <Tabs> child <${// @ts-expect-error: guarding against unexpected cases
typeof child.type==='string'?child.type:child.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`);})?.filter(Boolean)??[];}function extractChildrenTabValues(children){return ensureValidChildren(children).map(_ref=>{let{props:{value,label,attributes,default:isDefault}}=_ref;return{value,label,attributes,default:isDefault};});}function ensureNoDuplicateValue(values){const dup=(0,jsUtils/* duplicates */.l)(values,(a,b)=>a.value===b.value);if(dup.length>0){throw new Error(`Docusaurus error: Duplicate values "${dup.map(a=>a.value).join(', ')}" found in <Tabs>. Every value needs to be unique.`);}}function useTabValues(props){const{values:valuesProp,children}=props;return (0,react.useMemo)(()=>{const values=valuesProp??extractChildrenTabValues(children);ensureNoDuplicateValue(values);return values;},[valuesProp,children]);}function isValidValue(_ref2){let{value,tabValues}=_ref2;return tabValues.some(a=>a.value===value);}function getInitialStateValue(_ref3){let{defaultValue,tabValues}=_ref3;if(tabValues.length===0){throw new Error('Docusaurus error: the <Tabs> component requires at least one <TabItem> children component');}if(defaultValue){// Warn user about passing incorrect defaultValue as prop.
if(!isValidValue({value:defaultValue,tabValues})){throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${defaultValue}" but none of its children has the corresponding value. Available values are: ${tabValues.map(a=>a.value).join(', ')}. If you intend to show no default tab, use defaultValue={null} instead.`);}return defaultValue;}const defaultTabValue=tabValues.find(tabValue=>tabValue.default)??tabValues[0];if(!defaultTabValue){throw new Error('Unexpected error: 0 tabValues');}return defaultTabValue.value;}function getStorageKey(groupId){if(!groupId){return null;}return`docusaurus.tab.${groupId}`;}function getQueryStringKey(_ref4){let{queryString=false,groupId}=_ref4;if(typeof queryString==='string'){return queryString;}if(queryString===false){return null;}if(queryString===true&&!groupId){throw new Error(`Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".`);}return groupId??null;}function useTabQueryString(_ref5){let{queryString=false,groupId}=_ref5;const history=(0,react_router/* useHistory */.k6)();const key=getQueryStringKey({queryString,groupId});const value=(0,historyUtils/* useQueryStringValue */._X)(key);const setValue=(0,react.useCallback)(newValue=>{if(!key){return;// no-op
}const searchParams=new URLSearchParams(history.location.search);searchParams.set(key,newValue);history.replace({...history.location,search:searchParams.toString()});},[key,history]);return[value,setValue];}function useTabStorage(_ref6){let{groupId}=_ref6;const key=getStorageKey(groupId);const[value,storageSlot]=(0,storageUtils/* useStorageSlot */.Nk)(key);const setValue=(0,react.useCallback)(newValue=>{if(!key){return;// no-op
}storageSlot.set(newValue);},[key,storageSlot]);return[value,setValue];}function useTabs(props){const{defaultValue,queryString=false,groupId}=props;const tabValues=useTabValues(props);const[selectedValue,setSelectedValue]=(0,react.useState)(()=>getInitialStateValue({defaultValue,tabValues}));const[queryStringValue,setQueryString]=useTabQueryString({queryString,groupId});const[storageValue,setStorageValue]=useTabStorage({groupId});// We sync valid querystring/storage value to state on change + hydration
const valueToSync=(()=>{const value=queryStringValue??storageValue;if(!isValidValue({value,tabValues})){return null;}return value;})();// Sync in a layout/sync effect is important, for useScrollPositionBlocker
// See https://github.com/facebook/docusaurus/issues/8625
(0,react.useLayoutEffect)(()=>{if(valueToSync){setSelectedValue(valueToSync);}},[valueToSync]);const selectValue=(0,react.useCallback)(newValue=>{if(!isValidValue({value:newValue,tabValues})){throw new Error(`Can't select invalid tab value=${newValue}`);}setSelectedValue(newValue);setQueryString(newValue);setStorageValue(newValue);},[setQueryString,setStorageValue,tabValues]);return{selectedValue,selectValue,tabValues};}
// EXTERNAL MODULE: ./node_modules/@docusaurus/core/lib/client/exports/useIsBrowser.js
var useIsBrowser = __webpack_require__(2389);
;// CONCATENATED MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/Tabs/styles.module.css
// extracted by mini-css-extract-plugin
/* harmony default export */ const styles_module = ({"tabList":"tabList__CuJ","tabItem":"tabItem_LNqP"});
;// CONCATENATED MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/Tabs/index.js
/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */function TabList(_ref){let{className,block,selectedValue,selectValue,tabValues}=_ref;const tabRefs=[];const{blockElementScrollPositionUntilNextRender}=(0,scrollUtils/* useScrollPositionBlocker */.o5)();const handleTabChange=event=>{const newTab=event.currentTarget;const newTabIndex=tabRefs.indexOf(newTab);const newTabValue=tabValues[newTabIndex].value;if(newTabValue!==selectedValue){blockElementScrollPositionUntilNextRender(newTab);selectValue(newTabValue);}};const handleKeydown=event=>{let focusElement=null;switch(event.key){case'Enter':{handleTabChange(event);break;}case'ArrowRight':{const nextTab=tabRefs.indexOf(event.currentTarget)+1;focusElement=tabRefs[nextTab]??tabRefs[0];break;}case'ArrowLeft':{const prevTab=tabRefs.indexOf(event.currentTarget)-1;focusElement=tabRefs[prevTab]??tabRefs[tabRefs.length-1];break;}default:break;}focusElement?.focus();};return/*#__PURE__*/react.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,clsx_m/* default */.Z)('tabs',{'tabs--block':block},className)},tabValues.map(_ref2=>{let{value,label,attributes}=_ref2;return/*#__PURE__*/react.createElement("li",(0,esm_extends/* default */.Z)({// TODO extract TabListItem
role:"tab",tabIndex:selectedValue===value?0:-1,"aria-selected":selectedValue===value,key:value,ref:tabControl=>tabRefs.push(tabControl),onKeyDown:handleKeydown,onClick:handleTabChange},attributes,{className:(0,clsx_m/* default */.Z)('tabs__item',styles_module.tabItem,attributes?.className,{'tabs__item--active':selectedValue===value})}),label??value);}));}function TabContent(_ref3){let{lazy,children,selectedValue}=_ref3;const childTabs=(Array.isArray(children)?children:[children]).filter(Boolean);if(lazy){const selectedTabItem=childTabs.find(tabItem=>tabItem.props.value===selectedValue);if(!selectedTabItem){// fail-safe or fail-fast? not sure what's best here
return null;}return/*#__PURE__*/(0,react.cloneElement)(selectedTabItem,{className:'margin-top--md'});}return/*#__PURE__*/react.createElement("div",{className:"margin-top--md"},childTabs.map((tabItem,i)=>/*#__PURE__*/(0,react.cloneElement)(tabItem,{key:i,hidden:tabItem.props.value!==selectedValue})));}function TabsComponent(props){const tabs=useTabs(props);return/*#__PURE__*/react.createElement("div",{className:(0,clsx_m/* default */.Z)('tabs-container',styles_module.tabList)},/*#__PURE__*/react.createElement(TabList,(0,esm_extends/* default */.Z)({},props,tabs)),/*#__PURE__*/react.createElement(TabContent,(0,esm_extends/* default */.Z)({},props,tabs)));}function Tabs(props){const isBrowser=(0,useIsBrowser/* default */.Z)();return/*#__PURE__*/react.createElement(TabsComponent// Remount tabs after hydration
// Temporary fix for https://github.com/facebook/docusaurus/issues/5653
,(0,esm_extends/* default */.Z)({key:String(isBrowser)},props));}

/***/ }),

/***/ 4184:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

namespace TDengineExample
{
    internal class InfluxDBLineExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            PrepareDatabase(conn);
            string[] lines = {
                "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"
            };
            IntPtr res = TDengine.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("SchemalessInsert failed since " + TDengine.Error(res));
            }
            else
            {
                int affectedRows = TDengine.AffectRows(res);
                Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
            }
            TDengine.FreeResult(res);

        }
        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to create database, reason: " + TDengine.Error(res));
            }
            res = TDengine.Query(conn, "USE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to change database, reason: " + TDengine.Error(res));
            }
        }

    }

}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/influxdbLine/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5549:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

namespace TDengineExample
{
    internal class OptsJsonExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                PrepareDatabase(conn);
                string[] lines = { "[{\\"metric\\": \\"meters.current\\", \\"timestamp\\": 1648432611249, \\"value\\": 10.3, \\"tags\\": {\\"location\\": \\"California.SanFrancisco\\", \\"groupid\\": 2}}," +
                " {\\"metric\\": \\"meters.voltage\\", \\"timestamp\\": 1648432611249, \\"value\\": 219, \\"tags\\": {\\"location\\": \\"California.LosAngeles\\", \\"groupid\\": 1}}, " +
                "{\\"metric\\": \\"meters.current\\", \\"timestamp\\": 1648432611250, \\"value\\": 12.6, \\"tags\\": {\\"location\\": \\"California.SanFrancisco\\", \\"groupid\\": 2}}," +
                " {\\"metric\\": \\"meters.voltage\\", \\"timestamp\\": 1648432611250, \\"value\\": 221, \\"tags\\": {\\"location\\": \\"California.LosAngeles\\", \\"groupid\\": 1}}]"
            };

                IntPtr res = TDengine.SchemalessInsert(conn, lines, 1, (int)TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
                if (TDengine.ErrorNo(res) != 0)
                {
                    throw new Exception("SchemalessInsert failed since " + TDengine.Error(res));
                }
                else
                {
                    int affectedRows = TDengine.AffectRows(res);
                    Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
                }
                TDengine.FreeResult(res);
            }
            finally
            {
                TDengine.Close(conn);
            }
        }
        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to create database, reason: " + TDengine.Error(res));
            }
            res = TDengine.Query(conn, "USE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to change database, reason: " + TDengine.Error(res));
            }
        }
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/optsJSON/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 9036:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

namespace TDengineExample
{
    internal class OptsTelnetExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                PrepareDatabase(conn);
                string[] lines = {
                "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
                "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
                "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
                "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
                "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
                "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
                "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
                "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
            };
                IntPtr res = TDengine.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
                if (TDengine.ErrorNo(res) != 0)
                {
                    throw new Exception("SchemalessInsert failed since " + TDengine.Error(res));
                }
                else
                {
                    int affectedRows = TDengine.AffectRows(res);
                    Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
                }
                TDengine.FreeResult(res);
            }
            catch
            {
                TDengine.Close(conn);
            }
        }
        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to create database, reason: " + TDengine.Error(res));
            }
            res = TDengine.Query(conn, "USE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to change database, reason: " + TDengine.Error(res));
            }
        }
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/optsTelnet/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2917:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;


namespace TDengineExample
{
    internal class SQLInsertExample
    {

        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                IntPtr res = TDengine.Query(conn, "CREATE DATABASE power WAL_RETENTION_PERIOD 3600");
                CheckRes(conn, res, "failed to create database");
                res = TDengine.Query(conn, "USE power");
                CheckRes(conn, res, "failed to change database");
                res = TDengine.Query(conn, "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
                CheckRes(conn, res, "failed to create stable");
                var sql = "INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000) " +
                            "d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000) " +
                            "d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                            "d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000)('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)";
                res = TDengine.Query(conn, sql);
                CheckRes(conn, res, "failed to insert data");
                int affectedRows = TDengine.AffectRows(res);
                Console.WriteLine("affectedRows " + affectedRows);
                TDengine.FreeResult(res);
            }
            finally
            {
                TDengine.Close(conn);
            }

        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void CheckRes(IntPtr conn, IntPtr res, String errorMsg)
        {
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception($"{errorMsg} since: {TDengine.Error(res)}");
            }
        }

    }
}

// output:
// Connect to TDengine success
// affectedRows 8

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/sqlInsert/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2546:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;
using TDengineDriver.Impl;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    internal class QueryExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                // run query
                IntPtr res = TDengine.Query(conn, "SELECT * FROM meters LIMIT 2");
                if (TDengine.ErrorNo(res) != 0)
                {
                    throw new Exception("Failed to query since: " + TDengine.Error(res));
                }

                // get filed count
                int fieldCount = TDengine.FieldCount(res);
                Console.WriteLine("fieldCount=" + fieldCount);

                // print column names
                List<TDengineMeta> metas = LibTaos.GetMeta(res);
                for (int i = 0; i < metas.Count; i++)
                {
                    Console.Write(metas[i].name + "\\t");
                }
                Console.WriteLine();

                // print values
                List<Object> resData = LibTaos.GetData(res);
                for (int i = 0; i < resData.Count; i++)
                {
                    Console.Write($"|{resData[i].ToString()} \\t");
                    if (((i + 1) % metas.Count == 0))
                    {
                        Console.WriteLine("");
                    }
                }
                Console.WriteLine();

                // Free result after use
                TDengine.FreeResult(res);
            }
            finally
            {
                TDengine.Close(conn);
            }

        }
        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "power";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }
    }
}

// output:
// Connect to TDengine success
// fieldCount=6
// ts      current voltage phase   location        groupid
// 1648432611249   10.3    219     0.31    California.SanFrancisco        2
// 1648432611749   12.6    218     0.33    California.SanFrancisco        2
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/query/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2170:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineDriver.Impl;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    public class AsyncQueryExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                QueryAsyncCallback queryAsyncCallback = new QueryAsyncCallback(QueryCallback);
                TDengine.QueryAsync(conn, "select * from meters", queryAsyncCallback, IntPtr.Zero);
                Thread.Sleep(2000);
            }
            finally
            {
                TDengine.Close(conn);
            }

        }

        static void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                TDengine.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
            }
            else
            {
                throw new Exception($"async query data failed,code:{code},reason:{TDengine.Error(taosRes)}");
            }
        }

        // Iteratively call this interface until "numOfRows" is no greater than 0.
        static void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
        {
            if (numOfRows > 0)
            {
                Console.WriteLine($"{numOfRows} rows async retrieved");
                IntPtr pdata = TDengine.GetRawBlock(taosRes);
                List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
                List<object> dataList = LibTaos.ReadRawBlock(pdata, metaList, numOfRows);

                for (int i = 0; i < dataList.Count; i++)
                {
                    if (i != 0 && (i + 1) % metaList.Count == 0)
                    {
                        Console.WriteLine("{0}\\t|", dataList[i]);
                    }
                    else
                    {
                        Console.Write("{0}\\t|", dataList[i]);
                    }
                }
                Console.WriteLine("");
                TDengine.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
            }
            else
            {
                if (numOfRows == 0)
                {
                    Console.WriteLine("async retrieve complete.");
                }
                else
                {
                    throw new Exception($"FetchRawBlockCallback callback error, error code {numOfRows}");
                }
                TDengine.FreeResult(taosRes);
            }
        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "power";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }
    }
}

// //output:
// // Connect to TDengine success
// // 8 rows async retrieved

// // 1538548685500   |       11.8    |       221     |       0.28    |       california.losangeles   |       2       |
// // 1538548696600   |       13.4    |       223     |       0.29    |       california.losangeles   |       2       |
// // 1538548685000   |       10.8    |       223     |       0.29    |       california.losangeles   |       3       |
// // 1538548686500   |       11.5    |       221     |       0.35    |       california.losangeles   |       3       |
// // 1538548685000   |       10.3    |       219     |       0.31    |       california.sanfrancisco         |       2       |
// // 1538548695000   |       12.6    |       218     |       0.33    |       california.sanfrancisco         |       2       |
// // 1538548696800   |       12.3    |       221     |       0.31    |       california.sanfrancisco         |       2       |
// // 1538548696650   |       10.3    |       218     |       0.25    |       california.sanfrancisco         |       3       |
// // async retrieve complete.
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/asyncQuery/Program.cs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5596:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_11__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* harmony import */ var _preparation_mdx__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(8462);
/* harmony import */ var _07_develop_03_insert_data_cs_sql_mdx__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(2917);
/* harmony import */ var _07_develop_03_insert_data_cs_line_mdx__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(4184);
/* harmony import */ var _07_develop_03_insert_data_cs_opts_telnet_mdx__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(9036);
/* harmony import */ var _07_develop_03_insert_data_cs_opts_json_mdx__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(5549);
/* harmony import */ var _07_develop_04_query_data_cs_mdx__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(2546);
/* harmony import */ var _07_develop_04_query_data_cs_async_mdx__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(2170);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,sidebar_label:'C#',title:'C# Connector'};const contentTitle=undefined;const metadata={"unversionedId":"connector/csharp","id":"connector/csharp","title":"C# Connector","description":"TDengine.Connector 是 TDengine 提供的 C# 语言连接器。C# 开发人员可以通过它开发存取 TDengine 集群数据的 C# 应用软件。","source":"@site/docs/08-connector/40-csharp.mdx","sourceDirName":"08-connector","slug":"/connector/csharp","permalink":"/docs/connector/csharp","draft":false,"tags":[],"version":"current","sidebarPosition":40,"frontMatter":{"toc_max_heading_level":4,"sidebar_label":"C#","title":"C# Connector"},"sidebar":"defaultSidebar","previous":{"title":"Node.js","permalink":"/docs/connector/node"},"next":{"title":"R","permalink":"/docs/connector/r-lang"}};const assets={};const toc=[{value:'支持的平台',id:'支持的平台',level:2},{value:'版本支持',id:'版本支持',level:2},{value:'支持的功能特性',id:'支持的功能特性',level:2},{value:'安装步骤',id:'安装步骤',level:2},{value:'安装前准备',id:'安装前准备',level:3},{value:'安装 TDengine.Connector',id:'安装-tdengineconnector',level:3},{value:'建立连接',id:'建立连接',level:2},{value:'使用示例',id:'使用示例',level:2},{value:'写入数据',id:'写入数据',level:3},{value:'SQL 写入',id:'sql-写入',level:4},{value:'InfluxDB 行协议写入',id:'influxdb-行协议写入',level:4},{value:'OpenTSDB Telnet 行协议写入',id:'opentsdb-telnet-行协议写入',level:4},{value:'OpenTSDB JSON 行协议写入',id:'opentsdb-json-行协议写入',level:4},{value:'参数绑定',id:'参数绑定',level:4},{value:'查询数据',id:'查询数据',level:3},{value:'同步查询',id:'同步查询',level:4},{value:'异步查询',id:'异步查询',level:4},{value:'更多示例程序',id:'更多示例程序',level:3},{value:'重要更新记录',id:'重要更新记录',level:2},{value:'其他说明',id:'其他说明',level:2},{value:'第三方驱动',id:'第三方驱动',level:3},{value:'常见问题',id:'常见问题',level:2},{value:'API 参考',id:'api-参考',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_11__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 是 TDengine 提供的 C# 语言连接器。C# 开发人员可以通过它开发存取 TDengine 集群数据的 C# 应用软件。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 连接器支持通过 TDengine 客户端驱动（taosc）建立与 TDengine 运行实例的连接，提供数据写入、查询、数据订阅、schemaless 数据写入、参数绑定接口数据写入等功能。 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 自 v3.0.1 起还支持 WebSocket，通过 DSN 建立 WebSocket 连接，提供数据写入、查询、参数绑定接口数据写入等功能。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本文介绍如何在 Linux 或 Windows 环境中安装 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),`，并通过 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 连接 TDengine 集群，进行数据写入、查询等基本操作。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 3.x 不兼容 TDengine 2.x，如果在运行 TDengine 2.x 版本的环境下需要使用 C# 连接器请使用 TDengine.Connector 的 1.x 版本 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 的源码托管在 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-dotnet/tree/3.0"},`GitHub`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"支持的平台"},`支持的平台`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`支持的平台和 TDengine 客户端驱动支持的平台一致。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`注意 TDengine 不再支持 32 位 Windows 平台。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"版本支持"},`版本支持`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`请参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../#%E7%89%88%E6%9C%AC%E6%94%AF%E6%8C%81"},`版本支持列表`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"支持的功能特性"},`支持的功能特性`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`连接管理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`普通查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`连续查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`参数绑定`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`数据订阅（TMQ）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Schemaless`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`连接管理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`普通查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`连续查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`参数绑定`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"安装步骤"},`安装步骤`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"安装前准备"},`安装前准备`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`安装 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://dotnet.microsoft.com/download"},`.NET SDK`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.microsoft.com/en-us/nuget/install-nuget-client-tools"},`Nuget 客户端`),` （可选安装）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`安装 TDengine 客户端驱动，具体步骤请参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"../#%E5%AE%89%E8%A3%85%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%A9%B1%E5%8A%A8"},`安装客户端驱动`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"安装-tdengineconnector"},`安装 TDengine.Connector`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"CLI",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"CLI",label:"\u4F7F\u7528\u672C\u5730\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`可以在当前 .NET 项目的路径下，通过 dotnet CLI 添加 Nuget package `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 到当前项目。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`dotnet add package TDengine.Connector
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`也可以修改当前项目的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`.csproj`),` 文件，添加如下 ItemGroup。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-XML"},`  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.*" />
  </ItemGroup>
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"source",label:"\u4F7F\u7528 WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`需要修改目标项目的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`.csproj`),` 项目文件，将 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`.nupkg`),` 中的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`runtimes`),` 目录中的动态库复制到当前项目的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`$(OutDir)`),` 目录下。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-XML"},`  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.*" GeneratePathProperty="true" />
  </ItemGroup>
  <Target Name="copyDLLDependency" BeforeTargets="BeforeBuild">
    <ItemGroup>
      <DepDLLFiles Include="$(PkgTDengine_Connector)\\runtimes\\**\\*.*" />
    </ItemGroup>
    <Copy SourceFiles="@(DepDLLFiles)" DestinationFolder="$(OutDir)" />
  </Target>
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine.Connector`),` 自 version>= 3.0.2 的 nuget package 中才会有动态库（ taosws.dll，libtaows.so ）。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"建立连接"},`建立连接`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 host、username、password、port 等信息建立连接。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

namespace TDengineExample
{

    internal class EstablishConnection
    {
        static void Main(String[] args)
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";

            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                Console.WriteLine("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            TDengine.Close(conn);
            TDengine.Cleanup();
        }
    }
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 DSN 建立 WebSocket 连接 DSN 连接。 描述字符串基本结构如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`[<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------------|---|-----------|-----------|------|------|------------|-----------------------|
|   protocol |   | username  | password  | host | port |  database  |  params               |
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`各部分意义见下表：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`protocol`),`: 显示指定以何种方式建立连接，例如：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`ws://localhost:6041`),` 指定以 Websocket 方式建立连接（支持 http/ws ）。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`username/password`),`: 用于创建连接的用户名及密码（默认 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`root/taosdata`),` ）。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`host/port`),`: 指定创建连接的服务器及端口，WebSocket 连接默认为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`localhost:6041`),` 。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`database`),`: 指定默认连接的数据库名，可选参数。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`params`),`：其他可选参数。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSConnExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
  
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
                // close connection.
                LibTaosWS.WSClose(wsConn);
            }

            return 0;
        }
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsConnect/Program.cs"},`查看源码`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"使用示例"},`使用示例`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"写入数据"},`写入数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"sql-写入"},`SQL 写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_cs_sql_mdx__WEBPACK_IMPORTED_MODULE_5__/* ["default"] */ .ZP,{mdxType:"CSInsert"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSInsertExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);

            // Assert if connection is validate
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            string createTable = "CREATE STABLE test.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);";
            string insert = "INSERT INTO test.d1001 USING test.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)" +
                            "test.d1002 USING test.meters TAGS('California.SanFrancisco', 3) VALUES('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)" +
                            "test.d1003 USING test.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                            "test.d1004 USING test.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000)('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)";

            IntPtr wsRes = LibTaosWS.WSQuery(wsConn, createTable);
            ValidInsert("create table", wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQuery(wsConn, insert);
            ValidInsert("insert data", wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            // close connection.
            LibTaosWS.WSClose(wsConn);

            return 0;
        }

        static void ValidInsert(string desc, IntPtr wsRes)
        {
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                Console.WriteLine($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
            }
            else
            {
                Console.WriteLine("{0} success affect {2} rows, cost {1} nanoseconds", desc, LibTaosWS.WSTakeTiming(wsRes), LibTaosWS.WSAffectRows(wsRes));
            }
        }
    }

}
// Establish connect success.
// create table success affect 0 rows, cost 3717542 nanoseconds
// insert data success affect 8 rows, cost 2613637 nanoseconds

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsInsert/Program.cs"},`查看源码`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"influxdb-行协议写入"},`InfluxDB 行协议写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_cs_line_mdx__WEBPACK_IMPORTED_MODULE_6__/* ["default"] */ .ZP,{mdxType:"CSInfluxLine"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"opentsdb-telnet-行协议写入"},`OpenTSDB Telnet 行协议写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_cs_opts_telnet_mdx__WEBPACK_IMPORTED_MODULE_7__/* ["default"] */ .ZP,{mdxType:"CSOpenTSDBTelnet"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"opentsdb-json-行协议写入"},`OpenTSDB JSON 行协议写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_cs_opts_json_mdx__WEBPACK_IMPORTED_MODULE_8__/* ["default"] */ .ZP,{mdxType:"CSOpenTSDBJson"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"参数绑定"},`参数绑定`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

namespace TDengineExample
{
    internal class StmtInsertExample
    {
        private static IntPtr conn;
        private static IntPtr stmt;
        static void Main()
        {
            conn = GetConnection();
            try
            {
                PrepareSTable();
                // 1. init and prepare
                stmt = TDengine.StmtInit(conn);
                if (stmt == IntPtr.Zero)
                {
                    throw new Exception("failed to init stmt.");
                }
                int res = TDengine.StmtPrepare(stmt, "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)");
                CheckStmtRes(res, "failed to prepare stmt");

                // 2. bind table name and tags
                TAOS_MULTI_BIND[] tags = new TAOS_MULTI_BIND[2] { TaosMultiBind.MultiBindBinary(new string[] { "California.SanFrancisco" }), TaosMultiBind.MultiBindInt(new int?[] { 2 }) };
                res = TDengine.StmtSetTbnameTags(stmt, "d1001", tags);
                CheckStmtRes(res, "failed to bind table name and tags");

                // 3. bind values
                TAOS_MULTI_BIND[] values = new TAOS_MULTI_BIND[4] {
                TaosMultiBind.MultiBindTimestamp(new long[2] { 1648432611249, 1648432611749}),
                TaosMultiBind.MultiBindFloat(new float?[2] { 10.3f, 12.6f}),
                TaosMultiBind.MultiBindInt(new int?[2] { 219, 218}),
                TaosMultiBind.MultiBindFloat(new float?[2]{ 0.31f, 0.33f})
            };
                res = TDengine.StmtBindParamBatch(stmt, values);
                CheckStmtRes(res, "failed to bind params");

                // 4. add batch
                res = TDengine.StmtAddBatch(stmt);
                CheckStmtRes(res, "failed to add batch");

                // 5. execute
                res = TDengine.StmtExecute(stmt);
                CheckStmtRes(res, "failed to execute");

                // 6. free 
                TaosMultiBind.FreeTaosBind(tags);
                TaosMultiBind.FreeTaosBind(values);
            }
            finally
            {
                TDengine.Close(conn);
            }

        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareSTable()
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE power WAL_RETENTION_PERIOD 3600");
            CheckResPtr(res, "failed to create database");
            res = TDengine.Query(conn, "USE power");
            CheckResPtr(res, "failed to change database");
            res = TDengine.Query(conn, "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
            CheckResPtr(res, "failed to create stable");
        }

        static void CheckStmtRes(int res, string errorMsg)
        {
            if (res != 0)
            {
                Console.WriteLine(errorMsg + ", " + TDengine.StmtErrorStr(stmt));
                int code = TDengine.StmtClose(stmt);
                if (code != 0)
                {
                    throw new Exception($"failed to close stmt, {code} reason: {TDengine.StmtErrorStr(stmt)} ");
                }
            }
        }

        static void CheckResPtr(IntPtr res, string errorMsg)
        {
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception(errorMsg + " since:" + TDengine.Error(res));
            }
        }

    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/stmtInsert/Program.cs"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using System;
using TDengineWS.Impl;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace Examples
{
    public class WSStmtExample
    {
        static int Main(string[] args)
        {
            const string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            const string table = "meters";
            const string database = "test";
            const string childTable = "d1005";
            string insert = $"insert into ? using {database}.{table} tags(?,?) values(?,?,?,?)";
            const int numOfTags = 2;
            const int numOfColumns = 4;

            // Establish connection
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine($"get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success...");
            }

            // init stmt
            IntPtr wsStmt = LibTaosWS.WSStmtInit(wsConn);
            if (wsStmt != IntPtr.Zero)
            {
                int code = LibTaosWS.WSStmtPrepare(wsStmt, insert);
                ValidStmtStep(code, wsStmt, "WSStmtPrepare");

                TAOS_MULTI_BIND[] wsTags = new TAOS_MULTI_BIND[] { WSMultiBind.WSBindNchar(new string[] { "California.SanDiego" }), WSMultiBind.WSBindInt(new int?[] { 4 }) };
                code = LibTaosWS.WSStmtSetTbnameTags(wsStmt, $"{database}.{childTable}", wsTags, numOfTags);
                ValidStmtStep(code, wsStmt, "WSStmtSetTbnameTags");

                TAOS_MULTI_BIND[] data = new TAOS_MULTI_BIND[4];
                data[0] = WSMultiBind.WSBindTimestamp(new long[] { 1538548687000, 1538548688000, 1538548689000, 1538548690000, 1538548691000 });
                data[1] = WSMultiBind.WSBindFloat(new float?[] { 10.30F, 10.40F, 10.50F, 10.60F, 10.70F });
                data[2] = WSMultiBind.WSBindInt(new int?[] { 223, 221, 222, 220, 219 });
                data[3] = WSMultiBind.WSBindFloat(new float?[] { 0.31F, 0.32F, 0.33F, 0.35F, 0.28F });
                code = LibTaosWS.WSStmtBindParamBatch(wsStmt, data, numOfColumns);
                ValidStmtStep(code, wsStmt, "WSStmtBindParamBatch");

                code = LibTaosWS.WSStmtAddBatch(wsStmt);
                ValidStmtStep(code, wsStmt, "WSStmtAddBatch");

                IntPtr stmtAffectRowPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
                code = LibTaosWS.WSStmtExecute(wsStmt, stmtAffectRowPtr);
                ValidStmtStep(code, wsStmt, "WSStmtExecute");
                Console.WriteLine("WS STMT insert {0} rows...", Marshal.ReadInt32(stmtAffectRowPtr));
                Marshal.FreeHGlobal(stmtAffectRowPtr);

                LibTaosWS.WSStmtClose(wsStmt);

                // Free unmanaged memory
                WSMultiBind.WSFreeTaosBind(wsTags);
                WSMultiBind.WSFreeTaosBind(data);

                //check result with SQL "SELECT * FROM test.d1005;"
            }
            else
            {
                Console.WriteLine("Init STMT failed...");
            }

            // close connection.
            LibTaosWS.WSClose(wsConn);

            return 0;
        }

        static void ValidStmtStep(int code, IntPtr wsStmt, string desc)
        {
            if (code != 0)
            {
                Console.WriteLine($"{desc} failed,reason: {LibTaosWS.WSErrorStr(wsStmt)}, code: {code}");
            }
            else
            {
                Console.WriteLine("{0} success...", desc);
            }
        }
    }
}

// WSStmtPrepare success...
// WSStmtSetTbnameTags success...
// WSStmtBindParamBatch success...
// WSStmtAddBatch success...
// WSStmtExecute success...
// WS STMT insert 5 rows...

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsStmt/Program.cs"},`查看源码`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查询数据"},`查询数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"同步查询"},`同步查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_04_query_data_cs_mdx__WEBPACK_IMPORTED_MODULE_9__/* ["default"] */ .ZP,{mdxType:"CSQuery"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using System;
using TDengineWS.Impl;
using System.Collections.Generic;
using TDengineDriver;

namespace Examples
{
    public class WSQueryExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            string select = "select * from test.meters";

            // optional:wsRes = LibTaosWS.WSQuery(wsConn, select);
            IntPtr wsRes = LibTaosWS.WSQueryTimeout(wsConn, select, 1);
            // Assert if query execute success.
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                Console.WriteLine($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
                LibTaosWS.WSFreeResult(wsRes);
                return -1;
            }

            // get meta data
            List<TDengineMeta> metas = LibTaosWS.WSGetFields(wsRes);
            // get retrieved data
            List<object> dataSet = LibTaosWS.WSGetData(wsRes);

            // do something with result.
            foreach (var meta in metas)
            {
                Console.Write("{0} {1}({2}) \\t|\\t", meta.name, meta.TypeName(), meta.size);
            }
            Console.WriteLine("");

            for (int i = 0; i < dataSet.Count;)
            {
                for (int j = 0; j < metas.Count; j++)
                {
                    Console.Write("{0}\\t|\\t", dataSet[i]);
                    i++;
                }
                Console.WriteLine("");
            }

            // Free result after use.
            LibTaosWS.WSFreeResult(wsRes);

            // close connection.
            LibTaosWS.WSClose(wsConn);

            return 0;
        }
    }
}

// Establish connect success.
// ts TIMESTAMP(8)         |       current FLOAT(4)        |       voltage INT(4)  |       phase FLOAT(4)  |       location BINARY(64)     |       groupid INT(4)  |
// 1538548685000   |       10.8    |       223     |       0.29    |       California.LosAngeles   |       3       |
// 1538548686500   |       11.5    |       221     |       0.35    |       California.LosAngeles   |       3       |
// 1538548685500   |       11.8    |       221     |       0.28    |       California.LosAngeles   |       2       |
// 1538548696600   |       13.4    |       223     |       0.29    |       California.LosAngeles   |       2       |
// 1538548685000   |       10.3    |       219     |       0.31    |       California.SanFrancisco |       2       |
// 1538548695000   |       12.6    |       218     |       0.33    |       California.SanFrancisco |       2       |
// 1538548696800   |       12.3    |       221     |       0.31    |       California.SanFrancisco |       2       |
// 1538548696650   |       10.3    |       218     |       0.25    |       California.SanFrancisco |       3       |

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsQuery/Program.cs"},`查看源码`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"异步查询"},`异步查询`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_04_query_data_cs_async_mdx__WEBPACK_IMPORTED_MODULE_10__/* ["default"] */ .ZP,{mdxType:"CSAsyncQuery"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"更多示例程序"},`更多示例程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`示例程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`示例程序描述`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/Query/Query.cs"},`CURD`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的建表、插入、查询示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/JSONTag"},`JSON Tag`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的写入和查询 JSON tag 类型数据的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/tree/3.0/examples/NET6Examples/Stmt"},`stmt`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的参数绑定插入和查询的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/schemaless"},`schemaless`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的使用 schemaless 写入的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/AsyncQuery/QueryAsync.cs"},`async query`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的异步查询的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/TMQ/TMQ.cs"},`数据订阅（TMQ）`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 实现的订阅数据的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/FrameWork45/WS/WebSocketSample.cs"},`Basic WebSocket Usage`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 的 WebSocket 基本的示例`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/FrameWork45/WS/WebSocketSTMT.cs"},`Basic WebSocket STMT`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`使用 TDengine.Connector 的 WebSocket STMT 基本的示例`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"重要更新记录"},`重要更新记录`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`TDengine.Connector`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`说明`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`3.0.2`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`支持 .NET Framework 4.5 及以上，支持 .NET standard 2.0。Nuget Package 包含 WebSocket 动态库。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`3.0.1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`支持 WebSocket 和 Cloud，查询，插入，参数绑定。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`3.0.0`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`支持 TDengine 3.0.0.0，不兼容 2.x。新增接口TDengine.Impl.GetData()，解析查询结果。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.7`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`修复 TDengine.Query()内存泄露。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.6`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`修复 schemaless 在 1.0.4 和 1.0.5 中失效 bug。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.5`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`修复 Windows 同步查询中文报错 bug。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.4`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`新增异步查询，订阅等功能。修复绑定参数 bug。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.3`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`新增参数绑定、schemaless、 json tag等功能。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`1.0.2`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`新增连接管理、同步查询、错误信息等功能。`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"其他说明"},`其他说明`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"第三方驱动"},`第三方驱动`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/IoTSharp/EntityFrameworkCore.Taos"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`IoTSharp.Data.Taos`)),` 是一个 TDengine 的 ADO.NET 连接器,其中包含了用于EntityFrameworkCore 的提供程序 IoTSharp.EntityFrameworkCore.Taos 和健康检查组件 IoTSharp.HealthChecks.Taos ，支持 Linux，Windows 平台。该连接器由社区贡献者`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`麦壳饼@@maikebing`),` 提供，具体请参考:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`接口下载: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://github.com/IoTSharp/EntityFrameworkCore.Taos"},`https://github.com/IoTSharp/EntityFrameworkCore.Taos`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`用法说明:`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://www.taosdata.com/blog/2020/11/02/1901.html"},`https://www.taosdata.com/blog/2020/11/02/1901.html`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"常见问题"},`常见问题`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`"Unable to establish connection"，"Unable to resolve FQDN"`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`一般是因为 FQDN 配置不正确。可以参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.taosdata.com/blog/2021/07/29/2741.html"},`如何彻底搞懂 TDengine 的 FQDN`),`解决。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Unhandled exception. System.DllNotFoundException: Unable to load DLL 'taos' or one of its dependencies: 找不到指定的模块。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`一般是因为程序没有找到依赖的客户端驱动。解决方法为：Windows 下可以将 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`C:\\TDengine\\driver\\taos.dll`),` 拷贝到 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`C:\\Windows\\System32\\ `),` 目录下，Linux 下建立如下软链接 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so`),` 即可。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"api-参考"},`API 参考`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.taosdata.com/api/connector-csharp/html/860d2ac1-dd52-39c9-e460-0829c4e5a40b.htm"},`API 参考`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 8462:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* unused harmony exports frontMatter, contentTitle, toc, default */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`已安装客户端驱动（使用原生连接必须安装，使用 REST 连接无需安装）`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"info"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`由于 TDengine 的客户端驱动使用 C 语言编写，使用原生连接时需要加载系统对应安装在本地的客户端驱动共享库文件，通常包含在 TDengine 安装包。TDengine Linux 服务端安装包附带了 TDengine 客户端，也可以单独安装 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/get-started/"},`Linux 客户端`),` 。在 Windows 环境开发时需要安装 TDengine 对应的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.taosdata.com/cn/all-downloads/#TDengine-Windows-Client"},`Windows 客户端`),` 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`libtaos.so: 在 Linux 系统中成功安装 TDengine 后，依赖的 Linux 版客户端驱动 libtaos.so 文件会被自动拷贝至 /usr/lib/libtaos.so，该目录包含在 Linux 自动扫描路径上，无需单独指定。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`taos.dll: 在 Windows 系统中安装完客户端之后，依赖的 Windows 版客户端驱动 taos.dll 文件会自动拷贝到系统默认搜索路径 C:/Windows/System32 下，同样无需要单独指定。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`libtaos.dylib: 在 macOS 系统中成功安装 TDengine 后，依赖的 macOS 版客户端驱动 libtaos.dylib 文件会被自动拷贝至 /usr/local/lib/libtaos.dylib，该目录包含在 macOS 自动扫描路径上，无需单独指定。`))));};MDXContent.isMDXComponent=true;

/***/ })

}]);