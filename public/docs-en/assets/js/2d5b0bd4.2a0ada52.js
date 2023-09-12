"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[7810],{

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

/***/ 5162:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {


// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  Z: () => (/* binding */ TabItem)
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
  Z: () => (/* binding */ Tabs)
});

// EXTERNAL MODULE: ./node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(7462);
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
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/line/main.go"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3899:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/json/main.go"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5185:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/telnet/main.go"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2010:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/sql/main.go"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5835:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/query/sync/main.go"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2394:
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
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* harmony import */ var _07_develop_03_insert_data_go_sql_mdx__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(2010);
/* harmony import */ var _07_develop_03_insert_data_go_line_mdx__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(4518);
/* harmony import */ var _07_develop_03_insert_data_go_opts_telnet_mdx__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(5185);
/* harmony import */ var _07_develop_03_insert_data_go_opts_json_mdx__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(3899);
/* harmony import */ var _07_develop_04_query_data_go_mdx__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(5835);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'TDengine Go Connector',sidebar_label:'Go',description:'This document describes the TDengine Go connector.',toc_max_heading_level:4};const contentTitle=undefined;const metadata={"unversionedId":"reference/connector/go","id":"reference/connector/go","title":"TDengine Go Connector","description":"This document describes the TDengine Go connector.","source":"@site/docs/14-reference/03-connector/05-go.mdx","sourceDirName":"14-reference/03-connector","slug":"/reference/connector/go","permalink":"/docs-en/reference/connector/go","draft":false,"tags":[],"version":"current","sidebarPosition":5,"frontMatter":{"title":"TDengine Go Connector","sidebar_label":"Go","description":"This document describes the TDengine Go connector.","toc_max_heading_level":4},"sidebar":"defaultSidebar","previous":{"title":"Java","permalink":"/docs-en/reference/connector/java"},"next":{"title":"Rust","permalink":"/docs-en/reference/connector/rust"}};const assets={};const toc=[{value:'Supported platforms',id:'supported-platforms',level:2},{value:'Version support',id:'version-support',level:2},{value:'Handling exceptions',id:'handling-exceptions',level:2},{value:'TDengine DataType vs. Go DataType',id:'tdengine-datatype-vs-go-datatype',level:2},{value:'Installation Steps',id:'installation-steps',level:2},{value:'Pre-installation preparation',id:'pre-installation-preparation',level:3},{value:'Install the connectors',id:'install-the-connectors',level:3},{value:'Establishing a connection',id:'establishing-a-connection',level:2},{value:'Specify the URL and Properties to get the connection',id:'specify-the-url-and-properties-to-get-the-connection',level:3},{value:'Priority of configuration parameters',id:'priority-of-configuration-parameters',level:3},{value:'Usage examples',id:'usage-examples',level:2},{value:'Create database and tables',id:'create-database-and-tables',level:3},{value:'Insert data',id:'insert-data',level:3},{value:'Querying data',id:'querying-data',level:3},{value:'execute SQL with reqId',id:'execute-sql-with-reqid',level:3},{value:'Writing data via parameter binding',id:'writing-data-via-parameter-binding',level:3},{value:'Schemaless Writing',id:'schemaless-writing',level:3},{value:'Schemaless with reqId',id:'schemaless-with-reqid',level:3},{value:'Data Subscription',id:'data-subscription',level:3},{value:'Create a Topic',id:'create-a-topic',level:4},{value:'Create a Consumer',id:'create-a-consumer',level:4},{value:'Subscribe to consume data',id:'subscribe-to-consume-data',level:4},{value:'Assignment subscription Offset',id:'assignment-subscription-offset',level:4},{value:'Close subscriptions',id:'close-subscriptions',level:4},{value:'Full Sample Code',id:'full-sample-code',level:4},{value:'More sample programs',id:'more-sample-programs',level:3},{value:'Frequently Asked Questions',id:'frequently-asked-questions',level:2},{value:'API Reference',id:'api-reference',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_9__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` is the official Go language connector for TDengine. It implements the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},`database/sql`),` package, the generic Go language interface to SQL databases. Go developers can use it to develop applications that access TDengine cluster data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` provides two ways to establish connections. One is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`native connection`),`, which connects to TDengine instances natively through the TDengine client driver (taosc), supporting data writing, querying, subscriptions, schemaless writing, and bind interface. The other is the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`REST connection`),`, which connects to TDengine instances via the REST interface provided by taosAdapter. The set of features implemented by the REST connection differs slightly from those implemented by the native connection.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This article describes how to install `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` and connect to TDengine clusters and perform basic operations such as data query and data writing through `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The source code of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driver-go`),` is hosted on `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/driver-go"},`GitHub`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"supported-platforms"},`Supported platforms`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Native connections are supported on the same platforms as the TDengine client driver.
REST connections are supported on all platforms that can run Go.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"version-support"},`Version support`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/driver-go#remind"},`version support list`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"handling-exceptions"},`Handling exceptions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If it is a TDengine error, you can get the error code and error information in the following ways.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`// import "github.com/taosdata/driver-go/v3/errors"
    if err != nil {
        tError, is := err.(*errors.TaosError)
        if is {
            fmt.Println("errorCode:", int(tError.Code))
            fmt.Println("errorMessage:", tError.ErrStr)
        } else {
            fmt.Println(err.Error())
        }
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"tdengine-datatype-vs-go-datatype"},`TDengine DataType vs. Go DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`TDengine DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Go Type`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TIMESTAMP`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`time.Time`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TINYINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int8`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SMALLINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int16`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`INT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BIGINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`int64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TINYINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint8`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SMALLINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint16`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`INT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BIGINT UNSIGNED`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`uint64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`FLOAT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float32`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`DOUBLE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`float64`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BOOL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`bool`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`BINARY`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`NCHAR`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`string`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`JSON`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`[]byte`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`Note`),`: Only TAG supports JSON types`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"installation-steps"},`Installation Steps`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"pre-installation-preparation"},`Pre-installation preparation`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Install Go development environment (Go 1.14 and above, GCC 4.8.5 and above)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`If you use the native connector, please install the TDengine client driver. Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"/reference/connector/#install-client-driver"},`Install Client Driver`),` for specific steps`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Configure the environment variables and check the command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`go env`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`gcc -v`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"install-the-connectors"},`Install the connectors`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Initialize the project with the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go mod`),` command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go mod init taos-demo
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Introduce taosSql`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
  "database/sql"
  _ "github.com/taosdata/driver-go/v3/taosSql"
)
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Update the dependency packages with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go mod tidy`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go mod tidy
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Run the program with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go run taos-demo`),` or compile the binary with the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go build`),` command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`go run taos-demo
go build
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"establishing-a-connection"},`Establishing a connection`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Data source names have a standard format, e.g. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"http://pear.php.net/manual/en/package.database.db.intro-dsn.php"},`PEAR DB`),`, but no type prefix (square brackets indicate optionally):`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`DSN in full form.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`username:password@protocol(address)/dbname?param=value
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosSql`),` implements Go's `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` interface via cgo. You can use the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),` interface by simply introducing the driver.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosSql`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` and use a correct `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),`, DSN supports the following parameters.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`cfg specifies the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` directory`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosRestful`),` implements Go's `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` interface via `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http client`),`. You can use the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),` interface by simply introducing the driver.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosRestful`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` and use a correct `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),` with the following parameters supported by the DSN.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`disableCompression`),` whether to accept compressed data, default is true do not accept compressed data, set to false if transferring data using gzip compression.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`readBufferSize`),` The default size of the buffer for reading data is 4K (4096), which can be adjusted upwards when the query result has a lot of data.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"p"},`taosRestful`),` implements Go's `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`database/sql/driver`),` interface via `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http client`),`. You can use the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://golang.org/pkg/database/sql/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`database/sql`)),` interface by simply introducing the driver (driver-go minimum version 3.0.2).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosWS`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`driverName`),` and use a correct `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"#DSN"},`DSN`),` as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`dataSourceName`),` with the following parameters supported by the DSN.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`writeTimeout`),` The timeout to send data via WebSocket.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`readTimeout`),` The timeout to receive response data via WebSocket.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"specify-the-url-and-properties-to-get-the-connection"},`Specify the URL and Properties to get the connection`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The Go connector does not support this feature`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"priority-of-configuration-parameters"},`Priority of configuration parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The Go connector does not support this feature`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"usage-examples"},`Usage examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"create-database-and-tables"},`Create database and tables`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`var taosDSN = "root:taosdata@tcp(localhost:6030)/"
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"insert-data"},`Insert data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_03_insert_data_go_sql_mdx__WEBPACK_IMPORTED_MODULE_4__/* ["default"] */ .ZP,{mdxType:"GoInsert"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"querying-data"},`Querying data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_07_develop_04_query_data_go_mdx__WEBPACK_IMPORTED_MODULE_8__/* ["default"] */ .ZP,{mdxType:"GoQuery"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"execute-sql-with-reqid"},`execute SQL with reqId`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`This reqId can be used to request link tracing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`db, err := sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
if err != nil {
    panic(err)
}
defer db.Close()
ctx := context.WithValue(context.Background(), common.ReqIDKey, common.GetReqID())
_, err = db.ExecContext(ctx, "create database if not exists example_taos_sql")
if err != nil {
    panic(err)
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"writing-data-via-parameter-binding"},`Writing data via parameter binding`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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

`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"schemaless-writing"},`Schemaless Writing`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
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
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`import (
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
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"schemaless-with-reqid"},`Schemaless with reqId`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can get the unique id by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`common.GetReqID()`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"data-subscription"},`Data Subscription`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The TDengine Go Connector supports subscription functionality with the following application API.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"create-a-topic"},`Create a Topic`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    db, err := af.Open("", "root", "taosdata", "", 0)
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"create-a-consumer"},`Create a Consumer`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"subscribe-to-consume-data"},`Subscribe to consume data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    err = consumer.Subscribe("example_tmq_topic", nil)
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"assignment-subscription-offset"},`Assignment subscription Offset`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    partitions, err := consumer.Assignment()
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"close-subscriptions"},`Close subscriptions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`    err = consumer.Close()
    if err != nil {
        panic(err)
    }
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"full-sample-code"},`Full Sample Code`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",groupId:"connect",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"WebSocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-go"},`package main

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
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"more-sample-programs"},`More sample programs`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://github.com/taosdata/driver-go/tree/3.0/examples"},`sample program`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"frequently-asked-questions"},`Frequently Asked Questions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`bind interface in database/sql crashes`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`REST does not support parameter binding related interface. It is recommended to use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`db.Exec`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`db.Query`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`error `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`[0x217] Database not specified or available`),` after executing other statements with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` statement`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`The execution of SQL command in the REST interface is not contextual, so using `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` statement will not work, see the usage restrictions section above.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosSql`),` without error but use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosRestful`),` with error `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`[0x217] Database not specified or available`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Because the REST interface is stateless, using the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`use db`),` statement will not take effect. See the usage restrictions section above.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`readBufferSize`),` parameter has no significant effect after being increased`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Increasing `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`readBufferSize`),` will reduce the number of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`syscall`),` calls when fetching results. If the query result is smaller, modifying this parameter will not improve performance significantly. If you increase the parameter value too much, the bottleneck will be parsing JSON data. If you need to optimize the query speed, you must adjust the value based on the actual situation to achieve the best query performance.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`disableCompression`),` parameter is set to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`false`),` when the query efficiency is reduced`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`When set `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`disableCompression`),` parameter to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`false`),`, the query result will be compressed by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`gzip`),` and then transmitted, so you have to decompress the data by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`gzip`),` after getting it.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go get`),` command can't get the package, or timeout to get the package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Set Go proxy `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`go env -w GOPROXY=https://goproxy.cn,direct`),`.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"api-reference"},`API Reference`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Full API see `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://pkg.go.dev/github.com/taosdata/driver-go/v3"},`driver-go documentation`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);