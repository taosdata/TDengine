"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[4764],{

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

/***/ 4518:
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
    "fmt"
    "log"

    "github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
    _, err := conn.Exec("CREATE DATABASE test")
    if err != nil {
        panic(err)
    }
    _, err = conn.Exec("USE test")
    if err != nil {
        panic(err)
    }
}

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    if err != nil {
        fmt.Println("fail to connect, err:", err)
    }
    defer conn.Close()
    prepareDatabase(conn)
    var lines = []string{
        "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
        "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
        "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
        "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
    }

    err = conn.InfluxDBInsertLines(lines, "ms")
    if err != nil {
        log.Fatalln("insert error:", err)
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/line/main.go"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3899:
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
    "log"

    "github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
    _, err := conn.Exec("CREATE DATABASE test")
    if err != nil {
        panic(err)
    }
    _, err = conn.Exec("USE test")
    if err != nil {
        panic(err)
    }
}

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    if err != nil {
        log.Fatalln("fail to connect, err:", err)
    }
    defer conn.Close()
    prepareDatabase(conn)

    payload := \`[{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
                {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}},
                {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
                {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]\`

    err = conn.OpenTSDBInsertJsonPayload(payload)
    if err != nil {
        log.Fatalln("insert error:", err)
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/json/main.go"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5185:
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
    "log"

    "github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
    _, err := conn.Exec("CREATE DATABASE test")
    if err != nil {
        panic(err)
    }
    _, err = conn.Exec("USE test")
    if err != nil {
        panic(err)
    }
}

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    if err != nil {
        log.Fatalln("fail to connect, err:", err)
    }
    defer conn.Close()
    prepareDatabase(conn)
    var lines = []string{
        "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
        "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
        "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
        "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
        "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
        "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
        "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
        "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
    }

    err = conn.OpenTSDBInsertTelnetLines(lines)
    if err != nil {
        log.Fatalln("insert error:", err)
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/telnet/main.go"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2010:
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
    "fmt"
    "log"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func createStable(taos *sql.DB) {
    _, err := taos.Exec("CREATE DATABASE power")
    if err != nil {
        log.Fatalln("failed to create database, err:", err)
    }
    _, err = taos.Exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
    if err != nil {
        log.Fatalln("failed to create stable, err:", err)
    }
}

func insertData(taos *sql.DB) {
    sql := \`INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)\`
    result, err := taos.Exec(sql)
    if err != nil {
        log.Fatalln("failed to insert, err:", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        log.Fatalln("failed to get affected rows, err:", err)
    }
    fmt.Println("RowsAffected", rowsAffected)
}

func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        log.Fatalln("failed to connect TDengine, err:", err)
    }
    defer taos.Close()
    createStable(taos)
    insertData(taos)
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/sql/main.go"},`查看源码`)));};MDXContent.isMDXComponent=true;

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

/***/ 322:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* harmony import */ var _preparation_mdx__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(8462);
/* harmony import */ var _07_develop_03_insert_data_go_sql_mdx__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(2010);
/* harmony import */ var _07_develop_03_insert_data_go_line_mdx__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(4518);
/* harmony import */ var _07_develop_03_insert_data_go_opts_telnet_mdx__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(5185);
/* harmony import */ var _07_develop_03_insert_data_go_opts_json_mdx__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(3899);
/* harmony import */ var _07_develop_04_query_data_go_mdx__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(5835);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,sidebar_position:4,sidebar_label:'Go',title:'TDengine Go Connector'};const contentTitle=undefined;const metadata={"unversionedId":"connector/go","id":"connector/go","title":"TDengine Go Connector","description":"driver-go 是 TDengine 的官方 Go 语言连接器，实现了 Go 语言 database/sql  包的接口。Go 开发人员可以通过它开发存取 TDengine 集群数据的应用软件。","source":"@site/docs/08-connector/20-go.mdx","sourceDirName":"08-connector","slug":"/connector/go","permalink":"/docs/connector/go","draft":false,"tags":[],"version":"current","sidebarPosition":4,"frontMatter":{"toc_max_heading_level":4,"sidebar_position":4,"sidebar_label":"Go","title":"TDengine Go Connector"},"sidebar":"defaultSidebar","previous":{"title":"REST API","permalink":"/docs/connector/rest-api"},"next":{"title":"Schemaless API","permalink":"/docs/connector/schemaless-api"}};const assets={};const toc=[{value:'支持的平台',id:'支持的平台',level:2},{value:'版本支持',id:'版本支持',level:2},{value:'处理异常',id:'处理异常',level:2},{value:'TDengine DataType 和 Go DataType',id:'tdengine-datatype-和-go-datatype',level:2},{value:'安装步骤',id:'安装步骤',level:2},{value:'安装前准备',id:'安装前准备',level:3},{value:'安装连接器',id:'安装连接器',level:3},{value:'建立连接',id:'建立连接',level:2},{value:'指定 URL 和 Properties 获取连接',id:'指定-url-和-properties-获取连接',level:3},{value:'配置参数的优先级',id:'配置参数的优先级',level:3},{value:'使用示例',id:'使用示例',level:2},{value:'创建数据库和表',id:'创建数据库和表',level:3},{value:'插入数据',id:'插入数据',level:3},{value:'查询数据',id:'查询数据',level:3},{value:'执行带有 reqId 的 SQL',id:'执行带有-reqid-的-sql',level:3},{value:'通过参数绑定写入数据',id:'通过参数绑定写入数据',level:3},{value:'无模式写入',id:'无模式写入',level:3},{value:'执行带有 reqId 的无模式写入',id:'执行带有-reqid-的无模式写入',level:3},{value:'数据订阅',id:'数据订阅',level:3},{value:'创建 Topic',id:'创建-topic',level:4},{value:'创建 Consumer',id:'创建-consumer',level:4},{value:'订阅消费数据',id:'订阅消费数据',level:4},{value:'指定订阅 Offset',id:'指定订阅-offset',level:4},{value:'关闭订阅',id:'关闭订阅',level:4},{value:'完整示例',id:'完整示例',level:4},{value:'更多示例程序',id:'更多示例程序',level:3},{value:'常见问题',id:'常见问题',level:2},{value:'API 参考',id:'api-参考',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_10__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` 是 TDengine 的官方 Go 语言连接器，实现了 Go 语言 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},`database/sql`),`  包的接口。Go 开发人员可以通过它开发存取 TDengine 集群数据的应用软件。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` 提供两种建立连接的方式。一种是`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`原生连接`),`，它通过 TDengine 客户端驱动程序（taosc）原生连接 TDengine 运行实例，支持数据写入、查询、订阅、schemaless 接口和参数绑定接口等功能。另外一种是 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`REST 连接`),`，它通过 taosAdapter 提供的 REST 接口连接 TDengine 运行实例。REST 连接实现的功能特性集合和原生连接有少量不同。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本文介绍如何安装 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),`，并通过 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` 连接 TDengine 集群、进行数据查询、数据写入等基本操作。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` 的源码托管在 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/driver-go"},`GitHub`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"支持的平台"},`支持的平台`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
REST 连接支持所有能运行 Go 的平台。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"版本支持"},`版本支持`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`请参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/driver-go#remind"},`版本支持列表`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"处理异常"},`处理异常`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`如果是 TDengine 错误可以通过以下方式获取错误码和错误信息。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`// import "github.com/taosdata/driver-go/v3/errors"
    if err != nil {
        tError, is := err.(*errors.TaosError)
        if is {
            fmt.Println("errorCode:", int(tError.Code))
            fmt.Println("errorMessage:", tError.ErrStr)
        } else {
            fmt.Println(err.Error())
        }
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"tdengine-datatype-和-go-datatype"},`TDengine DataType 和 Go DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`TDengine DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Go Type`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TIMESTAMP`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`time.Time`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TINYINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int8`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SMALLINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int16`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`INT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BIGINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TINYINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint8`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SMALLINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint16`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`INT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BIGINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`FLOAT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`DOUBLE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BOOL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BINARY`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`NCHAR`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`JSON`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`[]byte`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`注意`),`：JSON 类型仅在 tag 中支持。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"安装步骤"},`安装步骤`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"安装前准备"},`安装前准备`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`安装 Go 开发环境（Go 1.14 及以上，GCC 4.8.5 及以上）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`如果使用原生连接器，请安装 TDengine 客户端驱动，具体步骤请参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"../#%E5%AE%89%E8%A3%85%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%A9%B1%E5%8A%A8"},`安装客户端驱动`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`配置好环境变量，检查命令：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`go env`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`gcc -v`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"安装连接器"},`安装连接器`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go mod`),` 命令初始化项目：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go mod init taos-demo
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`引入 taosSql ：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
  "database/sql"
  _ "github.com/taosdata/driver-go/v3/taosSql"
)
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go mod tidy`),` 更新依赖包：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go mod tidy
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go run taos-demo`),` 运行程序或使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go build`),` 命令编译出二进制文件。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go run taos-demo
go build
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"建立连接"},`建立连接`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`数据源名称具有通用格式，例如 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"http://pear.php.net/manual/en/package.database.db.intro-dsn.php"},`PEAR DB`),`，但没有类型前缀（方括号表示可选）：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`完整形式的 DSN：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`username:password@protocol(address)/dbname?param=value
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosSql`),` 通过 cgo 实现了 Go 的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` 接口。只需要引入驱动就可以使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),` 的接口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosSql`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` 并且使用一个正确的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),`，DSN 支持的参数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`cfg 指定 taos.cfg 目录`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`示例：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "fmt"

    _ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
    var taosUri = "root:taosdata@tcp(localhost:6030)/"
    taos, err := sql.Open("taosSql", taosUri)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosRestful`),` 通过 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http client`),` 实现了 Go 的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` 接口。只需要引入驱动就可以使用`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),`的接口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosRestful`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` 并且使用一个正确的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),`，DSN 支持的参数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`disableCompression`),` 是否接受压缩数据，默认为 true 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 false。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`readBufferSize`),` 读取数据的缓存区大小默认为 4K（4096），当查询结果数据量多时可以适当调大该值。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`示例：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "fmt"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
    var taosUri = "root:taosdata@http(localhost:6041)/"
    taos, err := sql.Open("taosRestful", taosUri)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosWS`),` 通过 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`WebSocket`),` 实现了 Go 的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` 接口。只需要引入驱动（driver-go 最低版本 3.0.2）就可以使用`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),`的接口。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosWS`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` 并且使用一个正确的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` 作为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),`，DSN 支持的参数：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`writeTimeout`),` 通过 WebSocket 发送数据的超时时间。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`readTimeout`),` 通过 WebSocket 接收响应数据的超时时间。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`示例：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "fmt"

    _ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
    var taosUri = "root:taosdata@ws(localhost:6041)/"
    taos, err := sql.Open("taosWS", taosUri)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
}
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"指定-url-和-properties-获取连接"},`指定 URL 和 Properties 获取连接`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Go 连接器不支持此功能`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"配置参数的优先级"},`配置参数的优先级`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Go 连接器不支持此功能`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"使用示例"},`使用示例`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建数据库和表"},`创建数据库和表`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`var taosDSN = "root:taosdata@tcp(localhost:6030)/"
taos, err := sql.Open("taosSql", taosDSN)
if err != nil {
    log.Fatalln("failed to connect TDengine, err:", err)
}
defer taos.Close()
_, err := taos.Exec("CREATE DATABASE power")
if err != nil {
    log.Fatalln("failed to create database, err:", err)
}
_, err = taos.Exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
if err != nil {
    log.Fatalln("failed to create stable, err:", err)
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"插入数据"},`插入数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_go_sql_mdx__WEBPACK_IMPORTED_MODULE_5__/* ["default"] */ .ZP,{mdxType:"GoInsert"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查询数据"},`查询数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_04_query_data_go_mdx__WEBPACK_IMPORTED_MODULE_9__/* ["default"] */ .ZP,{mdxType:"GoQuery"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"执行带有-reqid-的-sql"},`执行带有 reqId 的 SQL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`此 reqId 可用于请求链路追踪。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`db, err := sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
if err != nil {
    panic(err)
}
defer db.Close()
ctx := context.WithValue(context.Background(), common.ReqIDKey, common.GetReqID())
_, err = db.ExecContext(ctx, "create database if not exists example_taos_sql")
if err != nil {
    panic(err)
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"通过参数绑定写入数据"},`通过参数绑定写入数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "time"

    "github.com/taosdata/driver-go/v3/af"
    "github.com/taosdata/driver-go/v3/common"
    "github.com/taosdata/driver-go/v3/common/param"
)

func main() {
    db, err := af.Open("", "root", "taosdata", "", 0)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    _, err = db.Exec("create database if not exists example_stmt")
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("create table if not exists example_stmt.tb1(ts timestamp," +
        "c1 bool," +
        "c2 tinyint," +
        "c3 smallint," +
        "c4 int," +
        "c5 bigint," +
        "c6 tinyint unsigned," +
        "c7 smallint unsigned," +
        "c8 int unsigned," +
        "c9 bigint unsigned," +
        "c10 float," +
        "c11 double," +
        "c12 binary(20)," +
        "c13 nchar(20)" +
        ")")
    if err != nil {
        panic(err)
    }
    stmt := db.InsertStmt()
    err = stmt.Prepare("insert into example_stmt.tb1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    if err != nil {
        panic(err)
    }
    now := time.Now()
    params := make([]*param.Param, 14)
    params[0] = param.NewParam(2).
        AddTimestamp(now, common.PrecisionMilliSecond).
        AddTimestamp(now.Add(time.Second), common.PrecisionMilliSecond)
    params[1] = param.NewParam(2).AddBool(true).AddNull()
    params[2] = param.NewParam(2).AddTinyint(2).AddNull()
    params[3] = param.NewParam(2).AddSmallint(3).AddNull()
    params[4] = param.NewParam(2).AddInt(4).AddNull()
    params[5] = param.NewParam(2).AddBigint(5).AddNull()
    params[6] = param.NewParam(2).AddUTinyint(6).AddNull()
    params[7] = param.NewParam(2).AddUSmallint(7).AddNull()
    params[8] = param.NewParam(2).AddUInt(8).AddNull()
    params[9] = param.NewParam(2).AddUBigint(9).AddNull()
    params[10] = param.NewParam(2).AddFloat(10).AddNull()
    params[11] = param.NewParam(2).AddDouble(11).AddNull()
    params[12] = param.NewParam(2).AddBinary([]byte("binary")).AddNull()
    params[13] = param.NewParam(2).AddNchar("nchar").AddNull()

    paramTypes := param.NewColumnType(14).
        AddTimestamp().
        AddBool().
        AddTinyint().
        AddSmallint().
        AddInt().
        AddBigint().
        AddUTinyint().
        AddUSmallint().
        AddUInt().
        AddUBigint().
        AddFloat().
        AddDouble().
        AddBinary(6).
        AddNchar(5)
    err = stmt.BindParam(params, paramTypes)
    if err != nil {
        panic(err)
    }
    err = stmt.AddBatch()
    if err != nil {
        panic(err)
    }
    err = stmt.Execute()
    if err != nil {
        panic(err)
    }
    err = stmt.Close()
    if err != nil {
        panic(err)
    }
    // select * from example_stmt.tb1
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "fmt"
    "time"

    "github.com/taosdata/driver-go/v3/common"
    "github.com/taosdata/driver-go/v3/common/param"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
    "github.com/taosdata/driver-go/v3/ws/stmt"
)

func main() {
    db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    prepareEnv(db)

    config := stmt.NewConfig("ws://127.0.0.1:6041/rest/stmt", 0)
    config.SetConnectUser("root")
    config.SetConnectPass("taosdata")
    config.SetConnectDB("example_ws_stmt")
    config.SetMessageTimeout(common.DefaultMessageTimeout)
    config.SetWriteWait(common.DefaultWriteWait)
    config.SetErrorHandler(func(connector *stmt.Connector, err error) {
        panic(err)
    })
    config.SetCloseHandler(func() {
        fmt.Println("stmt connector closed")
    })

    connector, err := stmt.NewConnector(config)
    if err != nil {
        panic(err)
    }
    now := time.Now()
    {
        stmt, err := connector.Init()
        if err != nil {
            panic(err)
        }
        err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        if err != nil {
            panic(err)
        }
        err = stmt.SetTableName("tb1")
        if err != nil {
            panic(err)
        }
        err = stmt.SetTags(param.NewParam(1).AddJson([]byte(\`{"tb":1}\`)), param.NewColumnType(1).AddJson(0))
        if err != nil {
            panic(err)
        }
        params := []*param.Param{
            param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
            param.NewParam(3).AddBool(true).AddNull().AddBool(true),
            param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
            param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
            param.NewParam(3).AddInt(1).AddNull().AddInt(1),
            param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
            param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
            param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
            param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
            param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
            param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
            param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
            param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
            param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
        }
        paramTypes := param.NewColumnType(14).
            AddTimestamp().
            AddBool().
            AddTinyint().
            AddSmallint().
            AddInt().
            AddBigint().
            AddUTinyint().
            AddUSmallint().
            AddUInt().
            AddUBigint().
            AddFloat().
            AddDouble().
            AddBinary(0).
            AddNchar(0)
        err = stmt.BindParam(params, paramTypes)
        if err != nil {
            panic(err)
        }
        err = stmt.AddBatch()
        if err != nil {
            panic(err)
        }
        err = stmt.Exec()
        if err != nil {
            panic(err)
        }
        affected := stmt.GetAffectedRows()
        fmt.Println("all_json affected rows:", affected)
        err = stmt.Close()
        if err != nil {
            panic(err)
        }
    }
    {
        stmt, err := connector.Init()
        if err != nil {
            panic(err)
        }
        err = stmt.Prepare("insert into ? using all_all tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        err = stmt.SetTableName("tb1")
        if err != nil {
            panic(err)
        }

        err = stmt.SetTableName("tb2")
        if err != nil {
            panic(err)
        }
        err = stmt.SetTags(
            param.NewParam(14).
                AddTimestamp(now, 0).
                AddBool(true).
                AddTinyint(2).
                AddSmallint(2).
                AddInt(2).
                AddBigint(2).
                AddUTinyint(2).
                AddUSmallint(2).
                AddUInt(2).
                AddUBigint(2).
                AddFloat(2).
                AddDouble(2).
                AddBinary([]byte("tb2")).
                AddNchar("tb2"),
            param.NewColumnType(14).
                AddTimestamp().
                AddBool().
                AddTinyint().
                AddSmallint().
                AddInt().
                AddBigint().
                AddUTinyint().
                AddUSmallint().
                AddUInt().
                AddUBigint().
                AddFloat().
                AddDouble().
                AddBinary(0).
                AddNchar(0),
        )
        if err != nil {
            panic(err)
        }
        params := []*param.Param{
            param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
            param.NewParam(3).AddBool(true).AddNull().AddBool(true),
            param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
            param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
            param.NewParam(3).AddInt(1).AddNull().AddInt(1),
            param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
            param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
            param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
            param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
            param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
            param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
            param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
            param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
            param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
        }
        paramTypes := param.NewColumnType(14).
            AddTimestamp().
            AddBool().
            AddTinyint().
            AddSmallint().
            AddInt().
            AddBigint().
            AddUTinyint().
            AddUSmallint().
            AddUInt().
            AddUBigint().
            AddFloat().
            AddDouble().
            AddBinary(0).
            AddNchar(0)
        err = stmt.BindParam(params, paramTypes)
        if err != nil {
            panic(err)
        }
        err = stmt.AddBatch()
        if err != nil {
            panic(err)
        }
        err = stmt.Exec()
        if err != nil {
            panic(err)
        }
        affected := stmt.GetAffectedRows()
        fmt.Println("all_all affected rows:", affected)
        err = stmt.Close()
        if err != nil {
            panic(err)
        }

    }
}

func prepareEnv(db *sql.DB) {
    steps := []string{
        "create database example_ws_stmt",
        "create table example_ws_stmt.all_json(ts timestamp," +
            "c1 bool," +
            "c2 tinyint," +
            "c3 smallint," +
            "c4 int," +
            "c5 bigint," +
            "c6 tinyint unsigned," +
            "c7 smallint unsigned," +
            "c8 int unsigned," +
            "c9 bigint unsigned," +
            "c10 float," +
            "c11 double," +
            "c12 binary(20)," +
            "c13 nchar(20)" +
            ")" +
            "tags(t json)",
        "create table example_ws_stmt.all_all(" +
            "ts timestamp," +
            "c1 bool," +
            "c2 tinyint," +
            "c3 smallint," +
            "c4 int," +
            "c5 bigint," +
            "c6 tinyint unsigned," +
            "c7 smallint unsigned," +
            "c8 int unsigned," +
            "c9 bigint unsigned," +
            "c10 float," +
            "c11 double," +
            "c12 binary(20)," +
            "c13 nchar(20)" +
            ")" +
            "tags(" +
            "tts timestamp," +
            "tc1 bool," +
            "tc2 tinyint," +
            "tc3 smallint," +
            "tc4 int," +
            "tc5 bigint," +
            "tc6 tinyint unsigned," +
            "tc7 smallint unsigned," +
            "tc8 int unsigned," +
            "tc9 bigint unsigned," +
            "tc10 float," +
            "tc11 double," +
            "tc12 binary(20)," +
            "tc13 nchar(20))",
    }
    for _, step := range steps {
        _, err := db.Exec(step)
        if err != nil {
            panic(err)
        }
    }
}

`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"无模式写入"},`无模式写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
    "fmt"

    "github.com/taosdata/driver-go/v3/af"
)

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    if err != nil {
        fmt.Println("fail to connect, err:", err)
    }
    defer conn.Close()
    _, err = conn.Exec("create database if not exists example")
    if err != nil {
        panic(err)
    }
    _, err = conn.Exec("use example")
    if err != nil {
        panic(err)
    }
    influxdbData := "st,t1=3i64,t2=4f64,t3=\\"t3\\" c1=3i64,c3=L\\"passit\\",c2=false,c4=4f64 1626006833639000000"
    err = conn.InfluxDBInsertLines([]string{influxdbData}, "ns")
    if err != nil {
        panic(err)
    }
    telnetData := "stb0_0 1626006833 4 host=host0 interface=eth0"
    err = conn.OpenTSDBInsertTelnetLines([]string{telnetData})
    if err != nil {
        panic(err)
    }
    jsonData := "{\\"metric\\": \\"meter_current\\",\\"timestamp\\": 1626846400,\\"value\\": 10.3, \\"tags\\": {\\"groupid\\": 2, \\"location\\": \\"California.SanFrancisco\\", \\"id\\": \\"d1001\\"}}"
    err = conn.OpenTSDBInsertJsonPayload(jsonData)
    if err != nil {
        panic(err)
    }
}    
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
    "database/sql"
    "log"
    "time"

    "github.com/taosdata/driver-go/v3/common"
    _ "github.com/taosdata/driver-go/v3/taosWS"
    "github.com/taosdata/driver-go/v3/ws/schemaless"
)

func main() {
    db, err := sql.Open("taosWS", "root:taosdata@ws(localhost:6041)/")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    _, err = db.Exec("create database if not exists schemaless_ws")
    if err != nil {
        log.Fatal(err)
    }
    s, err := schemaless.NewSchemaless(schemaless.NewConfig("ws://localhost:6041/rest/schemaless", 1,
        schemaless.SetDb("schemaless_ws"),
        schemaless.SetReadTimeout(10*time.Second),
        schemaless.SetWriteTimeout(10*time.Second),
        schemaless.SetUser("root"),
        schemaless.SetPassword("taosdata"),
        schemaless.SetErrorHandler(func(err error) {
            log.Fatal(err)
        }),
    ))
    if err != nil {
        panic(err)
    }
    influxdbData := "st,t1=3i64,t2=4f64,t3=\\"t3\\" c1=3i64,c3=L\\"passit\\",c2=false,c4=4f64 1626006833639000000"
    telnetData := "stb0_0 1626006833 4 host=host0 interface=eth0"
    jsonData := "{\\"metric\\": \\"meter_current\\",\\"timestamp\\": 1626846400,\\"value\\": 10.3, \\"tags\\": {\\"groupid\\": 2, \\"location\\": \\"California.SanFrancisco\\", \\"id\\": \\"d1001\\"}}"

    err = s.Insert(influxdbData, schemaless.InfluxDBLineProtocol, "ns", 0, common.GetReqID())
    if err != nil {
        panic(err)
    }
    err = s.Insert(telnetData, schemaless.OpenTSDBTelnetLineProtocol, "ms", 0, common.GetReqID())
    if err != nil {
        panic(err)
    }
    err = s.Insert(jsonData, schemaless.OpenTSDBJsonFormatProtocol, "ms", 0, common.GetReqID())
    if err != nil {
        panic(err)
    }
}
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"执行带有-reqid-的无模式写入"},`执行带有 reqId 的无模式写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`可以通过 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`common.GetReqID()`),` 获取唯一 id。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"数据订阅"},`数据订阅`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine Go 连接器支持订阅功能，应用 API 如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"创建-topic"},`创建 Topic`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    db, err := af.Open("", "root", "taosdata", "", 0)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    _, err = db.Exec("create database if not exists example_tmq WAL_RETENTION_PERIOD 86400")
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("create topic if not exists example_tmq_topic as DATABASE example_tmq")
    if err != nil {
        panic(err)
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"创建-consumer"},`创建 Consumer`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
        "group.id":                     "test",
        "auto.offset.reset":            "earliest",
        "td.connect.ip":                "127.0.0.1",
        "td.connect.user":              "root",
        "td.connect.pass":              "taosdata",
        "td.connect.port":              "6030",
        "client.id":                    "test_tmq_client",
        "enable.auto.commit":           "false",
        "msg.with.table.name":          "true",
    })
    if err != nil {
        panic(err)
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"订阅消费数据"},`订阅消费数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    err = consumer.Subscribe("example_tmq_topic", nil)
    if err != nil {
        panic(err)
    }
    for i := 0; i < 5; i++ {
        ev := consumer.Poll(500)
        if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
                fmt.Printf("get message:%v\\n", e)
            case tmqcommon.Error:
                fmt.Fprintf(os.Stderr, "%% Error: %v: %v\\n", e.Code(), e)
                panic(e)
            }
            consumer.Commit()
        }
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"指定订阅-offset"},`指定订阅 Offset`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    partitions, err := consumer.Assignment()
    if err != nil {
        panic(err)
    }
    for i := 0; i < len(partitions); i++ {
        fmt.Println(partitions[i])
        err = consumer.Seek(tmqcommon.TopicPartition{
            Topic:     partitions[i].Topic,
            Partition: partitions[i].Partition,
            Offset:    0,
        }, 0)
        if err != nil {
            panic(err)
        }
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"关闭订阅"},`关闭订阅`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    err = consumer.Close()
    if err != nil {
        panic(err)
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"完整示例"},`完整示例`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "fmt"
    "os"

    "github.com/taosdata/driver-go/v3/af"
    "github.com/taosdata/driver-go/v3/af/tmq"
    tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
)

func main() {
    db, err := af.Open("", "root", "taosdata", "", 0)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    _, err = db.Exec("create database if not exists example_tmq WAL_RETENTION_PERIOD 86400")
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("create topic if not exists example_tmq_topic as DATABASE example_tmq")
    if err != nil {
        panic(err)
    }
    if err != nil {
        panic(err)
    }
    consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
        "group.id":                     "test",
        "auto.offset.reset":            "earliest",
        "td.connect.ip":                "127.0.0.1",
        "td.connect.user":              "root",
        "td.connect.pass":              "taosdata",
        "td.connect.port":              "6030",
        "client.id":                    "test_tmq_client",
        "enable.auto.commit":           "false",
        "msg.with.table.name":          "true",
    })
    if err != nil {
        panic(err)
    }
    err = consumer.Subscribe("example_tmq_topic", nil)
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("create table example_tmq.t1 (ts timestamp,v int)")
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("insert into example_tmq.t1 values(now,1)")
    if err != nil {
        panic(err)
    }
    for i := 0; i < 5; i++ {
        ev := consumer.Poll(500)
        if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
                fmt.Printf("get message:%v\\n", e)
            case tmqcommon.Error:
                fmt.Fprintf(os.Stderr, "%% Error: %v: %v\\n", e.Code(), e)
                panic(e)
            }
            consumer.Commit()
        }
    }
    partitions, err := consumer.Assignment()
    if err != nil {
        panic(err)
    }
    for i := 0; i < len(partitions); i++ {
        fmt.Println(partitions[i])
        err = consumer.Seek(tmqcommon.TopicPartition{
            Topic:     partitions[i].Topic,
            Partition: partitions[i].Partition,
            Offset:    0,
        }, 0)
        if err != nil {
            panic(err)
        }
    }

    partitions, err = consumer.Assignment()
    if err != nil {
        panic(err)
    }
    for i := 0; i < len(partitions); i++ {
        fmt.Println(partitions[i])
    }

    err = consumer.Close()
    if err != nil {
        panic(err)
    }
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "database/sql"
    "fmt"

    "github.com/taosdata/driver-go/v3/common"
    tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
    "github.com/taosdata/driver-go/v3/ws/tmq"
)

func main() {
    db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    prepareEnv(db)
    consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
        "ws.url":                "ws://127.0.0.1:6041/rest/tmq",
        "ws.message.channelLen": uint(0),
        "ws.message.timeout":    common.DefaultMessageTimeout,
        "ws.message.writeWait":  common.DefaultWriteWait,
        "td.connect.user":       "root",
        "td.connect.pass":       "taosdata",
        "group.id":              "example",
        "client.id":             "example_consumer",
        "auto.offset.reset":     "earliest",
    })
    if err != nil {
        panic(err)
    }
    err = consumer.Subscribe("example_ws_tmq_topic", nil)
    if err != nil {
        panic(err)
    }
    go func() {
        _, err := db.Exec("create table example_ws_tmq.t_all(ts timestamp," +
            "c1 bool," +
            "c2 tinyint," +
            "c3 smallint," +
            "c4 int," +
            "c5 bigint," +
            "c6 tinyint unsigned," +
            "c7 smallint unsigned," +
            "c8 int unsigned," +
            "c9 bigint unsigned," +
            "c10 float," +
            "c11 double," +
            "c12 binary(20)," +
            "c13 nchar(20)" +
            ")")
        if err != nil {
            panic(err)
        }
        _, err = db.Exec("insert into example_ws_tmq.t_all values(now,true,2,3,4,5,6,7,8,9,10.123,11.123,'binary','nchar')")
        if err != nil {
            panic(err)
        }
    }()
    for i := 0; i < 5; i++ {
        ev := consumer.Poll(500)
        if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
                fmt.Printf("get message:%v\\n", e)
            case tmqcommon.Error:
                fmt.Printf("%% Error: %v: %v\\n", e.Code(), e)
                panic(e)
            }
            consumer.Commit()
        }
    }
    partitions, err := consumer.Assignment()
    if err != nil {
        panic(err)
    }
    for i := 0; i < len(partitions); i++ {
        fmt.Println(partitions[i])
        err = consumer.Seek(tmqcommon.TopicPartition{
            Topic:     partitions[i].Topic,
            Partition: partitions[i].Partition,
            Offset:    0,
        }, 0)
        if err != nil {
            panic(err)
        }
    }

    partitions, err = consumer.Assignment()
    if err != nil {
        panic(err)
    }
    for i := 0; i < len(partitions); i++ {
        fmt.Println(partitions[i])
    }

    err = consumer.Close()
    if err != nil {
        panic(err)
    }
}

func prepareEnv(db *sql.DB) {
    _, err := db.Exec("create database example_ws_tmq WAL_RETENTION_PERIOD 86400")
    if err != nil {
        panic(err)
    }
    _, err = db.Exec("create topic example_ws_tmq_topic as database example_ws_tmq")
    if err != nil {
        panic(err)
    }
}
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"更多示例程序"},`更多示例程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://github.com/taosdata/driver-go/tree/3.0/examples"},`示例程序`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://www.taosdata.com/blog/2020/11/11/1951.html"},`视频教程`),`。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"常见问题"},`常见问题`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`database/sql 中 stmt（参数绑定）相关接口崩溃`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`REST 不支持参数绑定相关接口，建议使用`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`db.Exec`),`和`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`db.Query`),`。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` 语句后执行其他语句报错 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`[0x217] Database not specified or available`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`在 REST 接口中 SQL 语句的执行无上下文关联，使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` 语句不会生效，解决办法见上方使用限制章节。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`使用 taosSql 不报错使用 taosRestful 报错 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`[0x217] Database not specified or available`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`因为 REST 接口无状态，使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` 语句不会生效，解决办法见上方使用限制章节。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`readBufferSize`),` 参数调大后无明显效果`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`readBufferSize`),` 调大后会减少获取结果时 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`syscall`),` 的调用。如果查询结果的数据量不大，修改该参数不会带来明显提升，如果该参数修改过大，瓶颈会在解析 JSON 数据。如果需要优化查询速度，需要根据实际情况调整该值来达到查询效果最优。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`disableCompression`),` 参数设置为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`false`),` 时查询效率降低`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`当 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`disableCompression`),` 参数设置为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`false`),` 时查询结果会使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`gzip`),` 压缩后传输，拿到数据后要先进行 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`gzip`),` 解压。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go get`),` 命令无法获取包，或者获取包超时`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`设置 Go 代理 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go env -w GOPROXY=https://goproxy.cn,direct`),`。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"api-参考"},`API 参考`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`全部 API 见 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://pkg.go.dev/github.com/taosdata/driver-go/v3"},`driver-go 文档`)));};MDXContent.isMDXComponent=true;

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