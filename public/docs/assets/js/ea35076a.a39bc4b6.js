"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[6092],{

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

/***/ 5835:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "log"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/power"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        log.Fatalln("failed to connect TDengine, err:", err)
    }
    defer taos.Close()
    rows, err := taos.Query("SELECT ts, current FROM meters LIMIT 2")
    if err != nil {
        log.Fatalln("failed to select from table, err:", err)
    }

    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts      time.Time
            current float32
        }
        err := rows.Scan(&r.ts, &r.current)
        if err != nil {
            log.Fatalln("scan error:\\n", err)
            return
        }
        log.Println(r.ts, r.current)
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/query/sync/main.go"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5653:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-js"},`const taos = require("@tdengine/client");

const conn = taos.connect({ host: "localhost", database: "power" });
const cursor = conn.cursor();
const query = cursor.query("SELECT ts, current FROM meters LIMIT 2");
query.execute().then(function (result) {
  result.pretty();
});

// output:
// Successfully connected to TDengine
//            ts             |         current          |
// =======================================================
// 2018-10-03 14:38:05.000   | 10.3                     |
// 2018-10-03 14:38:15.000   | 12.6                     |

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/query_example.js"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5682:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::sync::*;

fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("ws:///power")?.build()?;
    let mut result = taos.query("SELECT ts, current FROM meters LIMIT 2")?;
    // print column names
    let meta = result.fields();
    println!("{}", meta.iter().map(|field| field.name()).join("\\t"));

    // print rows
    let rows = result.rows();
    for row in rows {
        let row = row?;
        for (_name, value) in row {
            print!("{}\\t", value);
        }
        println!();
    }
    Ok(())
}

// output(suppose you are in +8 timezone):
// ts      current
// 2018-10-03T14:38:05+08:00       10.3
// 2018-10-03T14:38:15+08:00       12.6

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/restexample/examples/query_example.rs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 1376:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _04_query_data_contentTitle),
  "default": () => (/* binding */ _04_query_data_MDXContent),
  "frontMatter": () => (/* binding */ _04_query_data_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _04_query_data_toc)
});

// EXTERNAL MODULE: ./node_modules/@docusaurus/core/node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(3117);
// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/@mdx-js/react/dist/esm.js
var esm = __webpack_require__(3905);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/Tabs/index.js + 2 modules
var Tabs = __webpack_require__(4866);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/TabItem/index.js + 1 modules
var TabItem = __webpack_require__(5162);
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_java.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import java.sql.*;

public class RestQueryExample {
    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041/power?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void printRow(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String value = rs.getString(i);
            System.out.print(value);
            System.out.print("\\t");
        }
        System.out.println();
    }

    private static void printColName(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String colLabel = meta.getColumnLabel(i);
            System.out.print(colLabel);
            System.out.print("\\t");
        }
        System.out.println();
    }

    private static void processResult(ResultSet rs) throws SQLException {
        printColName(rs);
        while (rs.next()) {
            printRow(rs);
        }
    }

    private static void queryData() throws SQLException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT AVG(voltage) FROM meters GROUP BY location");
                processResult(rs);
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        queryData();
    }
}

// possible output:
// avg(voltage) location
// 222.0    California.LosAngeles
// 219.0    California.SanFrancisco

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/RestQueryExample.java"},`查看源码`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_py.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _py_frontMatter={};const _py_contentTitle=(/* unused pure expression or super */ null && (undefined));const _py_toc=[];const _py_layoutProps={toc: _py_toc};const _py_MDXLayout="wrapper";function _py_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_py_MDXLayout,(0,esm_extends/* default */.Z)({},_py_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`通过迭代逐行获取查询结果。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`def query_api_demo(conn: taos.TaosConnection):
    result: taos.TaosResult = conn.query("SELECT tbname, * FROM meters LIMIT 2")
    print("field count:", result.field_count)
    print("meta of fields[1]:", result.fields[1])
    print("======================Iterate on result=========================")
    for row in result:
        print(row)


# field count: 7
# meta of fields[1]: {name: ts, type: 9, bytes: 8}
# ======================Iterate on result=========================
# ('d1003', datetime.datetime(2018, 10, 3, 14, 38, 5, 500000), 11.800000190734863, 221, 0.2800000011920929, 'california.losangeles', 2)
# ('d1003', datetime.datetime(2018, 10, 3, 14, 38, 16, 600000), 13.399999618530273, 223, 0.28999999165534973, 'california.losangeles', 2)
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/query_example.py"},`查看源码`)),(0,esm/* mdx */.kt)("p",null,`一次获取所有查询结果，并把每一行转化为一个字典返回。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`def fetch_all_demo(conn: taos.TaosConnection):
    result: taos.TaosResult = conn.query("SELECT ts, current FROM meters LIMIT 2")
    rows = result.fetch_all_into_dict()
    print("row count:", result.row_count)
    print("===============all data===================")
    print(rows)


# row count: 2
# ===============all data===================
# [{'ts': datetime.datetime(2018, 10, 3, 14, 38, 5, 500000), 'current': 11.800000190734863},
# {'ts': datetime.datetime(2018, 10, 3, 14, 38, 16, 600000), 'current': 13.399999618530273}]
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/query_example.py"},`查看源码`)));};_py_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_go.mdx
var _go = __webpack_require__(5835);
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_rust.mdx
var _rust = __webpack_require__(5682);
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_js.mdx
var _js = __webpack_require__(5653);
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_cs.mdx
var _cs = __webpack_require__(2546);
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_c.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _c_frontMatter={};const _c_contentTitle=(/* unused pure expression or super */ null && (undefined));const _c_toc=[];const _c_layoutProps={toc: _c_toc};const _c_MDXLayout="wrapper";function _c_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_c_MDXLayout,(0,esm_extends/* default */.Z)({},_c_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`// compile with:
// gcc -o query_example query_example.c -ltaos
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

typedef uint16_t VarDataLenT;

#define TSDB_NCHAR_SIZE sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x) (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define varDataLen(v) ((VarDataLenT *)(v))[0]

int printRow(char *str, TAOS_ROW row, TAOS_FIELD *fields, int numFields) {
  int  len = 0;
  char split = ' ';

  for (int i = 0; i < numFields; ++i) {
    if (i > 0) {
      str[len++] = split;
    }

    if (row[i] == NULL) {
      len += sprintf(str + len, "%s", "NULL");
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += sprintf(str + len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += sprintf(str + len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += sprintf(str + len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += sprintf(str + len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f", fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf", dv);
      } break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_GEOMETRY: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        memcpy(str + len, row[i], charLen);
        len += charLen;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }
  }

  return len;
}

/**
 * @brief print column name and values of each row
 *
 * @param res
 * @return int
 */
static int printResult(TAOS_RES *res) {
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  char        header[256] = {0};
  int len = 0;
  for (int i = 0; i < numFields; ++i) {
    len += sprintf(header + len, "%s ", fields[i].name);
  }
  puts(header);

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(res))) {
    char temp[256] = {0};
    printRow(temp, row, fields, numFields);
    puts(temp);
  }
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "power", 6030);
  if (taos == NULL) {
    puts("failed to connect to server");
    exit(EXIT_FAILURE);
  }
  TAOS_RES *res = taos_query(taos, "SELECT * FROM meters LIMIT 2");
  if (taos_errno(res) != 0) {
    printf("failed to execute taos_query. error: %s\\n", taos_errstr(res));
    exit(EXIT_FAILURE);
  }
  printResult(res);
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}

// output:
// ts current voltage phase location groupid 
// 1648432611249 10.300000 219 0.310000 California.SanFrancisco 2
// 1648432611749 12.600000 218 0.330000 California.SanFrancisco 2
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/query_example.c"},`查看源码`)));};_c_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_php.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _php_frontMatter={};const _php_contentTitle=(/* unused pure expression or super */ null && (undefined));const _php_toc=[];const _php_layoutProps={toc: _php_toc};const _php_MDXLayout="wrapper";function _php_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_php_MDXLayout,(0,esm_extends/* default */.Z)({},_php_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`<?php

use TDengine\\Connection;
use TDengine\\Exception\\TDengineException;

try {
    // instantiate
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = 'power';
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // connect
    $connection->connect();

    $resource = $connection->query('SELECT ts, current FROM meters LIMIT 2');
    var_dump($resource->fetch());
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/php/query.php"},`查看源码`)));};_php_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_py_async.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _py_async_frontMatter={};const _py_async_contentTitle=(/* unused pure expression or super */ null && (undefined));const _py_async_toc=[];const _py_async_layoutProps={toc: _py_async_toc};const _py_async_MDXLayout="wrapper";function _py_async_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_py_async_MDXLayout,(0,esm_extends/* default */.Z)({},_py_async_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`import time
from ctypes import *

from taos import *


def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        result.close()
        return
    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None

    for row in result.rows_iter(num_of_rows):
        print(row)
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)


def query_callback(p_param, p_result, code):
    if p_result is None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)
    result.check_error(code)


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    counter = Counter(count=0)
    conn.query_a("select ts, current, voltage from power.meters", query_callback, byref(counter))

    while not counter.done:
        print(counter)
        time.sleep(1)
    print(counter)
    conn.close()


if __name__ == "__main__":
    test_query(connect())

# possible output:
# { count: 0, done: False }
# fetched  8 rows
# 1538548685000 10.300000 219
# 1538548695000 12.600000 218
# 1538548696800 12.300000 221
# 1538548696650 10.300000 218
# 1538548685500 11.800000 221
# 1538548696600 13.400000 223
# 1538548685500 10.800000 223
# 1538548686500 11.500000 221
# fetched  0 rows
# fetching completed
# { count: 8, done: True }

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/async_query_example.py"},`查看源码`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`这个示例程序，目前在 Windows 系统上还无法运行`)));};_py_async_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_js_async.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _js_async_frontMatter={};const _js_async_contentTitle=(/* unused pure expression or super */ null && (undefined));const _js_async_toc=[];const _js_async_layoutProps={toc: _js_async_toc};const _js_async_MDXLayout="wrapper";function _js_async_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_js_async_MDXLayout,(0,esm_extends/* default */.Z)({},_js_async_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`const taos = require("@tdengine/client");
const conn = taos.connect({ host: "localhost", database: "power" });
const cursor = conn.cursor();

function queryExample() {
  cursor
    .query("SELECT ts, current FROM meters LIMIT 2")
    .execute_a()
    .then((result) => {
      result.pretty();
    });
}

try {
  queryExample();
} finally {
  setTimeout(() => {
    conn.close();
  }, 2000);
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/async_query_example.js"},`查看源码`)));};_js_async_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_cs_async.mdx
var _cs_async = __webpack_require__(2170);
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/_c_async.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _c_async_frontMatter={};const _c_async_contentTitle=(/* unused pure expression or super */ null && (undefined));const _c_async_toc=[];const _c_async_layoutProps={toc: _c_async_toc};const _c_async_MDXLayout="wrapper";function _c_async_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_c_async_MDXLayout,(0,esm_extends/* default */.Z)({},_c_async_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`
/**
 * @brief call back function of taos_fetch_row_a
 *
 * @param param : the third parameter you passed to taos_fetch_row_a
 * @param res : pointer of TAOS_RES
 * @param numOfRow : number of rows fetched in this batch. will be 0 if there is no more data.
 * @return void*
 */
void *fetch_row_callback(void *param, TAOS_RES *res, int numOfRow) {
  printf("numOfRow = %d \\n", numOfRow);
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  TAOS       *_taos = (TAOS *)param;
  if (numOfRow > 0) {
    for (int i = 0; i < numOfRow; ++i) {
      TAOS_ROW row = taos_fetch_row(res);
      char     temp[256] = {0};
      printRow(temp, row, fields, numFields);
      puts(temp);
    }
    taos_fetch_rows_a(res, fetch_row_callback, _taos);
  } else {
    printf("no more data, close the connection.\\n");
    taos_free_result(res);
    taos_close(_taos);
    taos_cleanup();
  }
}

/**
 * @brief callback function of taos_query_a
 *
 * @param param: the fourth parameter you passed to taos_query_a
 * @param res : the result set
 * @param code : status code
 * @return void*
 */
void *select_callback(void *param, TAOS_RES *res, int code) {
  printf("query callback ...\\n");
  TAOS *_taos = (TAOS *)param;
  if (code == 0 && res) {
    printHeader(res);
    taos_fetch_rows_a(res, fetch_row_callback, _taos);
  } else {
    printf("failed to execute taos_query. error: %s\\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(_taos);
    taos_cleanup();
    exit(EXIT_FAILURE);
  }
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "power", 6030);
  if (taos == NULL) {
    puts("failed to connect to server");
    exit(EXIT_FAILURE);
  }
  // param one is the connection returned by taos_connect.
  // param two is the SQL to execute.
  // param three is the callback function.
  // param four can be any pointer. It will be passed to your callback function as the first parameter. we use taos
  // here, because we want to close it after getting data.
  taos_query_a(taos, "SELECT * FROM meters", select_callback, taos);
  sleep(1);
}

// output:
// query callback ...
// ts current voltage phase location groupid
// numOfRow = 8
// 1538548685500 11.800000 221 0.280000 california.losangeles 2
// 1538548696600 13.400000 223 0.290000 california.losangeles 2
// 1538548685000 10.800000 223 0.290000 california.losangeles 3
// 1538548686500 11.500000 221 0.350000 california.losangeles 3
// 1538548685000 10.300000 219 0.310000 california.sanfrancisco 2
// 1538548695000 12.600000 218 0.330000 california.sanfrancisco 2
// 1538548696800 12.300000 221 0.310000 california.sanfrancisco 2
// 1538548696650 10.300000 218 0.250000 california.sanfrancisco 3
// numOfRow = 0
// no more data, close the connection.
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/async_query_example.c"},`查看源码`)));};_c_async_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/04-query-data/index.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _04_query_data_frontMatter={sidebar_label:'查询数据',title:'查询数据',description:'主要查询功能，通过连接器执行同步查询和异步查询'};const _04_query_data_contentTitle=undefined;const metadata={"unversionedId":"develop/query-data/index","id":"develop/query-data/index","title":"查询数据","description":"主要查询功能，通过连接器执行同步查询和异步查询","source":"@site/docs/07-develop/04-query-data/index.mdx","sourceDirName":"07-develop/04-query-data","slug":"/develop/query-data/","permalink":"/docs/develop/query-data/","draft":false,"tags":[],"version":"current","frontMatter":{"sidebar_label":"查询数据","title":"查询数据","description":"主要查询功能，通过连接器执行同步查询和异步查询"},"sidebar":"defaultSidebar","previous":{"title":"高效写入","permalink":"/docs/develop/insert-data/high-volume"},"next":{"title":"流式计算","permalink":"/docs/develop/stream"}};const assets={};const _04_query_data_toc=[{value:'主要查询功能',id:'主要查询功能',level:2},{value:'多表聚合查询',id:'多表聚合查询',level:2},{value:'示例一',id:'示例一',level:3},{value:'示例二',id:'示例二',level:3},{value:'降采样查询、插值',id:'降采样查询插值',level:2},{value:'示例代码',id:'示例代码',level:2},{value:'查询数据',id:'查询数据',level:3},{value:'异步查询',id:'异步查询',level:3}];const _04_query_data_layoutProps={toc: _04_query_data_toc};const _04_query_data_MDXLayout="wrapper";function _04_query_data_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_04_query_data_MDXLayout,(0,esm_extends/* default */.Z)({},_04_query_data_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h2",{"id":"主要查询功能"},`主要查询功能`),(0,esm/* mdx */.kt)("p",null,`TDengine 采用 SQL 作为查询语言。应用程序可以通过 REST API 或连接器发送 SQL 语句，用户还可以通过 TDengine 命令行工具 taos 手动执行 SQL 即席查询（Ad-Hoc Query）。TDengine 支持如下查询功能：`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`单列、多列数据查询`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`标签和数值的多种过滤条件：>, <, =, <`,`>`,`, like 等`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`聚合结果的分组（Group by）、排序（Order by）、约束输出（Limit/Offset）`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`时间窗口（Interval）、会话窗口（Session）和状态窗口（State_window）等窗口切分聚合查询`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`数值列及聚合结果的四则运算`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`时间戳对齐的连接查询（Join Query: 隐式连接）操作`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`多种聚合/计算函数: count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff 等`)),(0,esm/* mdx */.kt)("p",null,`例如：在命令行工具 taos 中，从表 d1001 中查询出 voltage > 215 的记录，按时间降序排列，仅仅输出 2 条。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`taos> select * from d1001 where voltage > 215 order by ts desc limit 2;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
Query OK, 2 row(s) in set (0.001100s)
`)),(0,esm/* mdx */.kt)("p",null,`为满足物联网场景的需求，TDengine 支持几个特殊的函数，比如 twa(时间加权平均)，spread (最大值与最小值的差)，last_row(最后一条记录)等，更多与物联网场景相关的函数将添加进来。`),(0,esm/* mdx */.kt)("p",null,`具体的查询语法请看 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../taos-sql/select"},`TDengine SQL 的数据查询`),` 章节。`),(0,esm/* mdx */.kt)("h2",{"id":"多表聚合查询"},`多表聚合查询`),(0,esm/* mdx */.kt)("p",null,`物联网场景中，往往同一个类型的数据采集点有多个。TDengine 采用超级表(STable)的概念来描述某一个类型的数据采集点，一张普通的表来描述一个具体的数据采集点。同时 TDengine 使用标签来描述数据采集点的静态属性，一个具体的数据采集点有具体的标签值。通过指定标签的过滤条件，TDengine 提供了一高效的方法将超级表(某一类型的数据采集点)所属的子表进行聚合查询。对普通表的聚合函数以及绝大部分操作都适用于超级表，语法完全一样。`),(0,esm/* mdx */.kt)("h3",{"id":"示例一"},`示例一`),(0,esm/* mdx */.kt)("p",null,`在 TDengine CLI，查找加利福尼亚州所有智能电表采集的电压平均值，并按照 location 分组。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> SELECT AVG(voltage), location FROM meters GROUP BY location;
       avg(voltage)        |                             location                             |
===============================================================================================
             219.200000000 | California.SanFrancisco                                          |
             221.666666667 | California.LosAngeles                                            |
Query OK, 2 rows in database (0.005995s)
`)),(0,esm/* mdx */.kt)("h3",{"id":"示例二"},`示例二`),(0,esm/* mdx */.kt)("p",null,`在 TDengine CLI, 查找 groupId 为 2 的所有智能电表的记录条数，电流的最大值。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> SELECT count(*), max(current) FROM meters where groupId = 2;
     cunt(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
`)),(0,esm/* mdx */.kt)("p",null,`在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../taos-sql/select"},`TDengine SQL 的数据查询`),` 一章，查询类操作都会注明是否支持超级表。`),(0,esm/* mdx */.kt)("h2",{"id":"降采样查询插值"},`降采样查询、插值`),(0,esm/* mdx */.kt)("p",null,`物联网场景里，经常需要通过降采样（down sampling）将采集的数据按时间段进行聚合。TDengine 提供了一个简便的关键词 interval 让按照时间窗口的查询操作变得极为简单。比如，将智能电表 d1001 采集的电流值每 10 秒钟求和`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> SELECT _wstart, sum(current) FROM d1001 INTERVAL(10s);
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 rows in database (0.003139s)
`)),(0,esm/* mdx */.kt)("p",null,`降采样操作也适用于超级表，比如：将加利福尼亚州所有智能电表采集的电流值每秒钟求和`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> SELECT _wstart, SUM(current) FROM meters where location like "California%" INTERVAL(1s);
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:04.000 |              10.199999809 |
 2018-10-03 14:38:05.000 |              23.699999809 |
 2018-10-03 14:38:06.000 |              11.500000000 |
 2018-10-03 14:38:15.000 |              12.600000381 |
 2018-10-03 14:38:16.000 |              34.400000572 |
Query OK, 5 rows in database (0.007413s)
`)),(0,esm/* mdx */.kt)("p",null,`降采样操作也支持时间偏移，比如：将所有智能电表采集的电流值每秒钟求和，但要求每个时间窗口从 500 毫秒开始`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`taos> SELECT _wstart, SUM(current) FROM meters INTERVAL(1s, 500a);
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:03.500 |              10.199999809 |
 2018-10-03 14:38:04.500 |              10.300000191 |
 2018-10-03 14:38:05.500 |              13.399999619 |
 2018-10-03 14:38:06.500 |              11.500000000 |
 2018-10-03 14:38:14.500 |              12.600000381 |
 2018-10-03 14:38:16.500 |              34.400000572 |
Query OK, 6 rows in database (0.005515s)
`)),(0,esm/* mdx */.kt)("p",null,`物联网场景里，每个数据采集点采集数据的时间是难同步的，但很多分析算法(比如 FFT)需要把采集的数据严格按照时间等间隔的对齐，在很多系统里，需要应用自己写程序来处理，但使用 TDengine 的降采样操作就轻松解决。`),(0,esm/* mdx */.kt)("p",null,`如果一个时间间隔里，没有采集的数据，TDengine 还提供插值计算的功能。`),(0,esm/* mdx */.kt)("p",null,`语法规则细节请见 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../taos-sql/distinguished"},`TDengine SQL 的按时间窗口切分聚合`),` 章节。`),(0,esm/* mdx */.kt)("h2",{"id":"示例代码"},`示例代码`),(0,esm/* mdx */.kt)("h3",{"id":"查询数据"},`查询数据`),(0,esm/* mdx */.kt)("p",null,`在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/develop/insert-data/sql-writing"},`SQL 写入`),` 一章，我们创建了 power 数据库，并向 meters 表写入了一些数据，以下示例代码展示如何查询这个表的数据。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"JavaQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_py_MDXContent,{mdxType:"PyQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_go/* default */.ZP,{mdxType:"GoQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_rust/* default */.ZP,{mdxType:"RustQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"nodejs",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_js/* default */.ZP,{mdxType:"NodeQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_cs/* default */.ZP,{mdxType:"CsQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_c_MDXContent,{mdxType:"CQuery"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_php_MDXContent,{mdxType:"PhpQuery"}))),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("ol",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ol"},`无论是使用 REST 连接还是原生连接的连接器，以上示例代码都能正常工作。`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`唯一需要注意的是：由于 REST 接口无状态， 不能使用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`use db`),` 语句来切换数据库。除了在 REST 参数中指定数据库以外也可以在 SQL 语句中使用 <db_name>.<table_name> 来指定数据库。`))),(0,esm/* mdx */.kt)("h3",{"id":"异步查询"},`异步查询`),(0,esm/* mdx */.kt)("p",null,`除同步查询 API 之外，TDengine 还提供性能更高的异步调用 API 处理数据插入、查询操作。在软硬件环境相同的情况下，异步 API 处理数据插入的速度比同步 API 快 2-4 倍。异步 API 采用非阻塞式的调用方式，在系统真正完成某个具体数据库操作前，立即返回。调用的线程可以去处理其他工作，从而可以提升整个应用的性能。异步 API 在网络延迟严重的情况下，优点尤为突出。`),(0,esm/* mdx */.kt)("p",null,`需要注意的是，只有使用原生连接的连接器，才能使用异步查询功能。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"python",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_py_async_MDXContent,{mdxType:"PyAsync"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_cs_async/* default */.ZP,{mdxType:"CsAsync"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_c_async_MDXContent,{mdxType:"CAsync"}))));};_04_query_data_MDXContent.isMDXComponent=true;

/***/ })

}]);