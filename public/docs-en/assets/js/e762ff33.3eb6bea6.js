"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1346],{

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

/***/ 9680:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'TDengine Python Connector',sidebar_label:'Python',description:'This document describes taospy, the TDengine Python connector.'};const contentTitle=undefined;const metadata={"unversionedId":"reference/connector/python","id":"reference/connector/python","title":"TDengine Python Connector","description":"This document describes taospy, the TDengine Python connector.","source":"@site/docs/14-reference/03-connector/07-python.mdx","sourceDirName":"14-reference/03-connector","slug":"/reference/connector/python","permalink":"/docs-en/reference/connector/python","draft":false,"tags":[],"version":"current","sidebarPosition":7,"frontMatter":{"title":"TDengine Python Connector","sidebar_label":"Python","description":"This document describes taospy, the TDengine Python connector."},"sidebar":"defaultSidebar","previous":{"title":"Rust","permalink":"/docs-en/reference/connector/rust"},"next":{"title":"Node.js","permalink":"/docs-en/reference/connector/node"}};const assets={};const toc=[{value:'Supported platforms',id:'supported-platforms',level:2},{value:'Supported features',id:'supported-features',level:3},{value:'Version selection',id:'version-selection',level:2},{value:'Handling Exceptions',id:'handling-exceptions',level:2},{value:'TDengine DataType vs. Python DataType',id:'tdengine-datatype-vs-python-datatype',level:2},{value:'Installation Steps',id:'installation-steps',level:2},{value:'Pre-installation preparation',id:'pre-installation-preparation',level:3},{value:'Install via pip',id:'install-via-pip',level:3},{value:'Uninstalling an older version',id:'uninstalling-an-older-version',level:4},{value:'To install <code>taospy</code>',id:'to-install-taospy',level:4},{value:'Install <code>taos-ws-py</code> (Optional)',id:'install-taos-ws-py-optional',level:4},{value:'Install taos-ws-py with taospy',id:'install-taos-ws-py-with-taospy',level:5},{value:'Install taos-ws-py only',id:'install-taos-ws-py-only',level:5},{value:'Verify',id:'verify',level:3},{value:'Establishing a connection',id:'establishing-a-connection',level:2},{value:'Connectivity testing',id:'connectivity-testing',level:3},{value:'Specify the Host and Properties to get the connection',id:'specify-the-host-and-properties-to-get-the-connection',level:3},{value:'Priority of configuration parameters',id:'priority-of-configuration-parameters',level:3},{value:'Usage examples',id:'usage-examples',level:2},{value:'Create database and tables',id:'create-database-and-tables',level:3},{value:'Insert data',id:'insert-data',level:3},{value:'Basic Usage',id:'basic-usage',level:3},{value:'TaosConnection class',id:'taosconnection-class',level:5},{value:'Use of TaosResult class',id:'use-of-taosresult-class',level:5},{value:'Use of the TaosCursor class',id:'use-of-the-taoscursor-class',level:5},{value:'Use of TaosRestCursor class',id:'use-of-taosrestcursor-class',level:5},{value:'Use of the RestClient class',id:'use-of-the-restclient-class',level:5},{value:'Querying Data',id:'querying-data',level:3},{value:'Execute SQL with reqId',id:'execute-sql-with-reqid',level:3},{value:'TaosConnection class',id:'taosconnection-class-1',level:5},{value:'Use of TaosResult class',id:'use-of-taosresult-class-1',level:5},{value:'Use of the TaosCursor class',id:'use-of-the-taoscursor-class-1',level:5},{value:'Use of TaosRestCursor class',id:'use-of-taosrestcursor-class-1',level:5},{value:'Use of the RestClient class',id:'use-of-the-restclient-class-1',level:5},{value:'Used with pandas',id:'used-with-pandas',level:3},{value:'Writing data via parameter binding',id:'writing-data-via-parameter-binding',level:3},{value:'Create Stmt',id:'create-stmt',level:5},{value:'parameter binding',id:'parameter-binding',level:5},{value:'execute sql',id:'execute-sql',level:5},{value:'Close Stmt',id:'close-stmt',level:5},{value:'Example',id:'example',level:5},{value:'Create Stmt',id:'create-stmt-1',level:5},{value:'Prepare sql',id:'prepare-sql',level:5},{value:'parameter binding',id:'parameter-binding-1',level:5},{value:'execute sql',id:'execute-sql-1',level:5},{value:'Close Stmt',id:'close-stmt-1',level:5},{value:'Example',id:'example-1',level:5},{value:'Schemaless Writing',id:'schemaless-writing',level:3},{value:'Simple insert',id:'simple-insert',level:5},{value:'Insert with ttl argument',id:'insert-with-ttl-argument',level:5},{value:'Insert with req_id argument',id:'insert-with-req_id-argument',level:5},{value:'Simple insert',id:'simple-insert-1',level:5},{value:'Insert with ttl argument',id:'insert-with-ttl-argument-1',level:5},{value:'Insert with req_id argument',id:'insert-with-req_id-argument-1',level:5},{value:'Schemaless with reqId',id:'schemaless-with-reqid',level:3},{value:'Data Subscription',id:'data-subscription',level:3},{value:'Create a Topic',id:'create-a-topic',level:4},{value:'Create a Consumer',id:'create-a-consumer',level:4},{value:'Subscribe to a Topic',id:'subscribe-to-a-topic',level:4},{value:'Consume messages',id:'consume-messages',level:4},{value:'Assignment subscription Offset',id:'assignment-subscription-offset',level:4},{value:'Close subscriptions',id:'close-subscriptions',level:4},{value:'Full Sample Code',id:'full-sample-code',level:4},{value:'Other sample programs',id:'other-sample-programs',level:3},{value:'Other notes',id:'other-notes',level:2},{value:'About nanoseconds',id:'about-nanoseconds',level:3},{value:'Important Update',id:'important-update',level:2},{value:'API Reference',id:'api-reference',level:2},{value:'Frequently Asked Questions',id:'frequently-asked-questions',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),` is the official Python connector for TDengine. taospy provides a rich API that makes it easy for Python applications to use TDengine. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),` wraps both the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/connector/cpp"},`native interface`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/rest-api"},`REST interface`),` of TDengine, which correspond to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosrest`),` modules of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),` package, respectively.
In addition to wrapping the native and REST interfaces, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),` also provides a set of programming interfaces that conforms to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://peps.python.org/pep-0249/"},`Python Data Access Specification (PEP 249)`),`. It is easy to integrate `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),` with many third-party tools, such as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.sqlalchemy.org/"},`SQLAlchemy`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://pandas.pydata.org/"},`pandas`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos-ws-py`),` is an optional package to enable using WebSocket to connect TDengine.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The direct connection to the server using the native interface provided by the client driver is referred to hereinafter as a "native connection"; the connection to the server using the REST or WebSocket interface provided by taosAdapter is referred to hereinafter as a "REST connection" or "WebSocket connection".`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The source code for the Python connector is hosted on `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-python"},`GitHub`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"supported-platforms"},`Supported platforms`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"/reference/connector/#supported-platforms"},`supported platforms`),` for the native connection are the same as the ones supported by the TDengine client.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`REST connections are supported on all platforms that can run Python.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"supported-features"},`Supported features`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Native connections support all the core features of TDengine, including connection management, SQL execution, bind interface, subscriptions, and schemaless writing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`REST connections support features such as connection management and SQL execution. (SQL execution allows you to: manage databases, tables, and supertables, write data, query data, create continuous queries, etc.).`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"version-selection"},`Version selection`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`We recommend using the latest version of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),`, regardless of the version of TDengine.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Python Connector Version`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`major changes`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`2.7.9`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`support for getting assignment and seek function on subscription`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`2.7.8`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`add `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"td"},`execute_many`),` method`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Python Websocket Connector Version`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`major changes`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`0.2.5`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`1. support for getting assignment and seek function on subscription `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("br",null),` 2. support schemaless `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("br",null),` 3. support STMT`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`0.2.4`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`support `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"td"},`unsubscribe`),` on subscription`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"handling-exceptions"},`Handling Exceptions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There are 4 types of exception in python connector.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The exception of Python Connector itself.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The exception of native library.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The exception of websocket`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The exception of subscription.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The exception of other TDengine function modules.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Error Type`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Description`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Suggested Actions`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`InterfaceError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`the native library is too old that it cannot support the function`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`please check the TDengine client version`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`ConnectionError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`connection error`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`please check TDengine's status and the connection params`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`DatabaseError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`database error`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`please upgrade Python connector to latest`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`OperationalError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`operation error`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`ProgrammingError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`StatementError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`the exception of stmt`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`ResultError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`SchemalessError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`the exception of stmt schemaless`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TmqError`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`the exception of stmt tmq`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"})))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`It usually uses try-expect to handle exceptions in python. For exception handling, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.python.org/3/tutorial/errors.html"},`Python Errors and Exceptions Documentation`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`All exceptions from the Python Connector are thrown directly. Applications should handle these exceptions. For example:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos

try:
    conn = taos.connect()
    conn.execute("CREATE TABLE 123")  # wrong sql
except taos.Error as e:
    print(e)
    print("exception class: ", e.__class__.__name__)
    print("error number:", e.errno)
    print("error message:", e.msg)
except BaseException as other:
    print("exception occur")
    print(other)

# output:
# [0x0216]: syntax error near 'Incomplete SQL statement'
# exception class:  ProgrammingError
# error number: -2147483114
# error message: syntax error near 'Incomplete SQL statement'

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/handle_exception.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"tdengine-datatype-vs-python-datatype"},`TDengine DataType vs. Python DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine currently supports timestamp, number, character, Boolean type, and the corresponding type conversion with Python is as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`TDengine DataType`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":"center"},`Python DataType`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TIMESTAMP`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`datetime`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`INT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BIGINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`FLOAT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`float`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`DOUBLE`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`SMALLINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`TINYINT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`int`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BOOL`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`bool`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`BINARY`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`str`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`NCHAR`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`str`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`JSON`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":"center"},`str`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"installation-steps"},`Installation Steps`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"pre-installation-preparation"},`Pre-installation preparation`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Install Python. The recent taospy package requires Python 3.6.2+. The earlier versions of taospy require Python 3.7+.  The taos-ws-py package requires Python 3.7+. If Python is not available on your system, refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://wiki.python.org/moin/BeginnersGuide/Download"},`Python BeginnersGuide`),` to install it.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Install `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://pypi.org/project/pip/"},`pip`),`. In most cases, the Python installer comes with the pip utility. If not, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://pip.pypa.io/en/stable/installation/"},`pip documentation`),` to install it.
If you use a native connection, you will also need to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"/reference/connector#Install-Client-Driver"},`Install Client Driver`),`. The client install package includes the TDengine client dynamic link library (`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`libtaos.so`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.dll`),`) and the TDengine CLI.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"install-via-pip"},`Install via pip`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"uninstalling-an-older-version"},`Uninstalling an older version`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If you have installed an older version of the Python Connector, please uninstall it beforehand.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`pip3 uninstall taos taospy
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`Earlier TDengine client software includes the Python connector. If the Python connector is installed from the client package's installation directory, the corresponding Python package name is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),`. So the above uninstall command includes `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),`, and it doesn't matter if it doesn't exist.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"to-install-taospy"},`To install `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"h4"},`taospy`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{label:"Install from PyPI",value:"pypi",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Install the latest version of:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`pip3 install taospy
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can also specify a specific version to install:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`pip3 install taospy==2.3.0
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{label:"Install from GitHub",value:"github",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`pip3 install git+https://github.com/taosdata/taos-connector-python.git
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"install-taos-ws-py-optional"},`Install `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"h4"},`taos-ws-py`),` (Optional)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The taos-ws-py package provides the way to access TDengine via WebSocket.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"install-taos-ws-py-with-taospy"},`Install taos-ws-py with taospy`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`pip3 install taospy[ws]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"install-taos-ws-py-only"},`Install taos-ws-py only`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`pip3 install taos-ws-py
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"verify"},`Verify`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For native connection, you need to verify that both the client driver and the Python connector itself are installed correctly. The client driver and Python connector have been installed properly if you can successfully import the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` module. In the Python Interactive Shell, you can type.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For REST connections, verifying that the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosrest`),` module can be imported successfully can be done in the Python Interactive Shell by typing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taosrest
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"ws",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For WebSocket connection, verifying that the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosws`),` module can be imported successfully can be done in the Python Interactive Shell by typing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taosws
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`If you have multiple versions of Python on your system, you may have various `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pip`),` commands. Be sure to use the correct path for the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pip`),` command. Above, we installed the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pip3`),` command, which rules out the possibility of using the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pip`),` corresponding to Python 2.x versions. However, if you have more than one version of Python 3.x on your system, you still need to check that the installation path is correct. The easiest way to verify this is to type `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`pip3 install taospy`),` again in the command, and it will print out the exact location of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taospy`),`, for example, on Windows.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`C:\\> pip3 install taospy
Looking in indexes: https://pypi.tuna.tsinghua.edu.cn/simple
Requirement already satisfied: taospy in c:\\users\\username\\appdata\\local\\programs\\python\\python310\\lib\\site-packages (2.3.0)
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"establishing-a-connection"},`Establishing a connection`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"connectivity-testing"},`Connectivity testing`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Before establishing a connection with the connector, we recommend testing the connectivity of the local TDengine CLI to the TDengine cluster.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Ensure that the TDengine instance is up and that the FQDN of the machines in the cluster (the FQDN defaults to hostname if you are starting a stand-alone version) can be resolved locally, by testing with the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`ping`),` command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`ping <FQDN>
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then test if the cluster can be appropriately connected with TDengine CLI:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`taos -h <FQDN> -p <PORT>
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The FQDN above can be the FQDN of any dnode in the cluster, and the PORT is the serverPort corresponding to this dnode.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",groupId:"connect",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For REST connections, make sure the cluster and taosAdapter component, are running. This can be tested using the following `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`curl `),` command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`curl -u root:taosdata http://<FQDN>:<PORT>/rest/sql -d "select server_version()"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The FQDN above is the FQDN of the machine running taosAdapter, PORT is the port taosAdapter listening, default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`6041`),`.
If the test is successful, it will output the server version information, e.g.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`{
  "code": 0,
  "column_meta": [
    [
      "server_version()",
      "VARCHAR",
      7
    ]
  ],
  "data": [
    [
      "3.0.0.0"
    ]
  ],
  "rows": 1
}
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"ws",label:"WebSocket connection",groupId:"connect",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For WebSocket connection, make sure the cluster and taosAdapter component, are running. This can be testetd using the following `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`curl`),` command.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`curl -i -N -d "show databases" -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Host: <FQDN>:<PORT>" -H "Origin: http://<FQDN>:<PORT>" http://<FQDN>:<PORT>/rest/sql
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The FQDN above is the FQDN of the machine running taosAdapter, PORT is the port taosAdapter listening, default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`6041`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the test is successful, it will output the server version information, e.g.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-json"},`HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Tue, 21 Mar 2023 09:29:17 GMT
Transfer-Encoding: chunked

{"status":"succ","head":["server_version()"],"column_meta":[["server_version()",8,8]],"data":[["2.6.0.27"]],"rows":1}
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"specify-the-host-and-properties-to-get-the-connection"},`Specify the Host and Properties to get the connection`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following example code assumes that TDengine is installed locally and that the default configuration is used for both FQDN and serverPort.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",groupId:"connect",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos

conn: taos.TaosConnection = taos.connect(host="localhost",
                                         user="root",
                                         password="taosdata",
                                         database="test",
                                         port=6030,
                                         config="/etc/taos",  # for windows the default value is C:\\TDengine\\cfg
                                         timezone="Asia/Shanghai")  # default your host's timezone

server_version = conn.server_info
print("server_version", server_version)
client_version = conn.client_info
print("client_version", client_version)  # 3.0.0.0

conn.close()

# possible output:
# 3.0.0.0
# 3.0.0.0

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_native_reference.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`All arguments of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`connect()`),` function are optional keyword arguments. The following are the connection parameters specified.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`host`),` : The FQDN of the node to connect to. There is no default value. If this parameter is not provided, the firstEP in the client configuration file will be connected.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`user`),` : The TDengine user name. The default value is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`root`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`password`),` : TDengine user password. The default value is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taosdata`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`port`),` : The starting port of the data node to connect to, i.e., the serverPort configuration. The default value is 6030, which will only take effect if the host parameter is provided.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`config`),` : The path to the client configuration file. On Windows systems, the default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`C:\\TDengine\\cfg`),`. The default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/etc/taos/`),` on Linux/macOS.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`timezone`),` : The timezone used to convert the TIMESTAMP data in the query results to python `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`datetime`),` objects. The default is the local timezone.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"warning"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`config`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`timezone`),` are both process-level configurations. We recommend that all connections made by a process use the same parameter values. Otherwise, unpredictable errors may occur.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`connect()`),` function returns a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.TaosConnection`),` instance. In client-side multi-threaded scenarios, we recommend that each thread request a separate connection instance rather than sharing a connection between multiple threads.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from taosrest import connect, TaosRestConnection, TaosRestCursor

conn = connect(url="http://localhost:6041",
               user="root",
               password="taosdata",
               timeout=30)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_rest_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`All arguments to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`connect()`),` function are optional keyword arguments. The following are the connection parameters specified.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`url`),`: The URL of taosAdapter REST service. The default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"http://localhost:6041"},`http://localhost:6041`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`user`),`: TDengine user name. The default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`root`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`password`),`: TDengine user password. The default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taosdata`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`timeout`),`: HTTP request timeout. Enter a value in seconds. The default is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`socket._GLOBAL_DEFAULT_TIMEOUT`),`. Usually, no configuration is needed.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taosws

conn = taosws.connect("taosws://root:taosdata@localhost:6041")
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_websocket_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The parameter of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`connect()`),` is the url of TDengine, and the protocol is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taosws`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`ws`),`.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"priority-of-configuration-parameters"},`Priority of configuration parameters`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If the configuration parameters are duplicated in the parameters or client configuration file, the priority of the parameters, from highest to lowest, are as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Parameters in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`connect`),` function.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`the configuration file taos.cfg of the TDengine client driver when using a native connection.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"usage-examples"},`Usage examples`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"create-database-and-tables"},`Create database and tables`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn = taos.connect()
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test")
# change database. same as execute "USE db"
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn = taosrest.connect(url="http://localhost:6041")
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test")
conn.execute("USE test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn = taosws.connect("taosws://localhost:6041")
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test")
conn.execute("USE test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"insert-data"},`Insert data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn.execute("INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+1m, 23.5) (now+2m, 24.4)")
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`:::
now is an internal function. The default is the current time of the client's computer. now + 1s represents the current time of the client plus 1 second, followed by the number representing the unit of time: a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks), n (months), y (years).
:::`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"basic-usage"},`Basic Usage`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"taosconnection-class"},`TaosConnection class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class contains both an implementation of the PEP249 Connection interface (e.g., the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cursor()`),` method and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`close()`),` method) and many extensions (e.g., the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`execute()`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`query()`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`schemaless_insert()`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`subscribe()`),` methods).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"execute method\"","title":"\"execute","method\"":true},`conn = taos.connect()
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test")
# change database. same as execute "USE db"
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
affected_row = conn.execute("INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+1m, 23.5) (now+2m, 24.4)")
print("affected_row", affected_row)
# output:
# affected_row 3
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connection_usage_native_reference.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"query method\"","title":"\"query","method\"":true},`# Execute a sql and get its result set. It's useful for SELECT statement
result = conn.query("SELECT * from weather")

# Get fields from result
fields = result.fields
for field in fields:
    print(field)  # {name: ts, type: 9, bytes: 8}

# output:
# {name: ts, type: 9, bytes: 8}
# {name: temperature, type: 6, bytes: 4}
# {name: location, type: 4, bytes: 4}

# Get data from result as list of tuple
data = result.fetch_all()
print(data)
# output:
# [(datetime.datetime(2022, 4, 27, 9, 4, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 5, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 6, 25, 367000), 24.399999618530273, 1)]

# Or get data from result as a list of dict
# map_data = result.fetch_all_into_dict()
# print(map_data)
# output:
# [{'ts': datetime.datetime(2022, 4, 27, 9, 1, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 2, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 3, 15, 343000), 'temperature': 24.399999618530273, 'location': 1}]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connection_usage_native_reference.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`The queried results can only be fetched once. For example, only one of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all()`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all_into_dict()`),` can be used in the example above. Repeated fetches will result in an empty list.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-taosresult-class"},`Use of TaosResult class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In the above example of using the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class, we have shown two ways to get the result of a query: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all()`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all_into_dict()`),`. In addition, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosResult`),` also provides methods to iterate through the result set by rows (`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`rows_iter`),`) or by data blocks (`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`blocks_iter`),`). Using these two methods will be more efficient in scenarios where the query has a large amount of data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"blocks_iter method\"","title":"\"blocks_iter","method\"":true},`import taos

conn = taos.connect()
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test")
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
# prepare data
for i in range(2000):
    location = str(i % 10)
    tb = "t" + location
    conn.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)")

result: taos.TaosResult = conn.query("SELECT * FROM weather")

block_index = 0
blocks: taos.TaosBlocks = result.blocks_iter()
for rows, length in blocks:
    print("block ", block_index, " length", length)
    print("first row in this block:", rows[0])
    block_index += 1

conn.close()

# possible output:
# block  0  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 46000), 23.5, 0)
# block  1  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 76000), 23.5, 3)
# block  2  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 99000), 23.5, 6)
# block  3  length 400
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 122000), 23.5, 9)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/result_set_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-the-taoscursor-class"},`Use of the TaosCursor class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosResult`),` class already implement all the functionality of the native interface. If you are familiar with the interfaces in the PEP249 specification, you can also use the methods provided by the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosCursor`),` class.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of TaosCursor\"","title":"\"Use","of":true,"TaosCursor\"":true},`import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS test")
cursor.execute("CREATE DATABASE test")
cursor.execute("USE test")
cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")

for i in range(1000):
    location = str(i % 10)
    tb = "t" + location
    cursor.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)")

cursor.execute("SELECT count(*) FROM weather")
data = cursor.fetchall()
print("count:", data[0][0])
cursor.execute("SELECT tbname, * FROM weather LIMIT 2")
col_names = [meta[0] for meta in cursor.description]
print(col_names)
rows = cursor.fetchall()
print(rows)

cursor.close()
conn.close()

# output:
# count: 2000
# ['tbname', 'ts', 'temperature', 'location']
# row_count: -1
# [('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 392000), 23.5, 0), ('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 393000), 23.5, 0)]

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/cursor_usage_native_reference.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`The TaosCursor class uses native connections for write and query operations. In a client-side multi-threaded scenario, this cursor instance must remain thread exclusive and cannot be shared across threads for use, otherwise, it will result in errors in the returned results.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-taosrestcursor-class"},`Use of TaosRestCursor class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosRestCursor`),` class is an implementation of the PEP249 Cursor interface.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of TaosRestCursor\"","title":"\"Use","of":true,"TaosRestCursor\"":true},`# create STable
cursor = conn.cursor()
cursor.execute("DROP DATABASE IF EXISTS power")
cursor.execute("CREATE DATABASE power")
cursor.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")

# insert data
cursor.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)""")
print("inserted row count:", cursor.rowcount)

# query data
cursor.execute("SELECT * FROM power.meters LIMIT 3")
# get total rows
print("queried row count:", cursor.rowcount)
# get column names from cursor
column_names = [meta[0] for meta in cursor.description]
# get rows
data = cursor.fetchall()
print(column_names)
for row in data:
    print(row)

# output:
# inserted row count: 8
# queried row count: 3
# ['ts', 'current', 'voltage', 'phase', 'location', 'groupid']
# [datetime.datetime(2018, 10, 3, 14, 38, 5, 500000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 11.8, 221, 0.28, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 16, 600000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 13.4, 223, 0.29, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.8, 223, 0.29, 'california.losangeles', 3]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_rest_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.execute`),`: Used to execute arbitrary SQL statements.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.rowcount`),` : For write operations, returns the number of successful rows written. For query operations, returns the number of rows in the result set.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.description`),` : Returns the description of the field. Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.taosdata.com/api/taospy/taosrest/cursor.html"},`TaosRestCursor`),` for the specific format of the description information.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-the-restclient-class"},`Use of the RestClient class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`RestClient`),` class is a direct wrapper for the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/rest-api"},`REST API`),`. It contains only a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method for executing arbitrary SQL statements and returning the result.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of RestClient\"","title":"\"Use","of":true,"RestClient\"":true},`from taosrest import RestClient

client = RestClient("http://localhost:6041", user="root", password="taosdata")
res: dict = client.sql("SELECT ts, current FROM power.meters LIMIT 1")
print(res)

# output:
# {'status': 'succ', 'head': ['ts', 'current'], 'column_meta': [['ts', 9, 8], ['current', 6, 4]], 'data': [[datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.3]], 'rows': 1}


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/rest_client_example.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For a more detailed description of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.taosdata.com/api/taospy/taosrest/restclient.html"},`RestClient`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Connection`),` class contains both an implementation of the PEP249 Connection interface (e.g., the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cursor()`),` method and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`close()`),` method) and many extensions (e.g., the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`execute()`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`query()`),`, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`schemaless_insert()`),`, and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`subscribe()`),` methods).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn.execute("drop database if exists connwspy")
conn.execute("create database if not exists connwspy wal_retention_period 3600")
conn.execute("use connwspy")
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute("create table if not exists tb1 using stb tags (1)")
conn.execute("insert into tb1 values (now, 1)")
conn.execute("insert into tb1 values (now, 2)")
conn.execute("insert into tb1 values (now, 3)")

r = conn.execute("select * from stb")
result = conn.query("select * from stb")
num_of_fields = result.field_count
print(num_of_fields)

for row in result:
    print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_websocket_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`conn.execute`),`: can use to execute arbitrary SQL statements, and return the number of rows affected.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`conn.query`),`: can use to execute query SQL statements, and return the query results.`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"querying-data"},`Querying Data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`query`),` method of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class can be used to query data and return the result data of type `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosResult`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`# Execute a sql and get its result set. It's useful for SELECT statement
result = conn.query("SELECT * from weather")

# Get fields from result
fields = result.fields
for field in fields:
    print(field)  # {name: ts, type: 9, bytes: 8}

# output:
# {name: ts, type: 9, bytes: 8}
# {name: temperature, type: 6, bytes: 4}
# {name: location, type: 4, bytes: 4}

# Get data from result as list of tuple
data = result.fetch_all()
print(data)
# output:
# [(datetime.datetime(2022, 4, 27, 9, 4, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 5, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 6, 25, 367000), 24.399999618530273, 1)]

# Or get data from result as a list of dict
# map_data = result.fetch_all_into_dict()
# print(map_data)
# output:
# [{'ts': datetime.datetime(2022, 4, 27, 9, 1, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 2, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 3, 15, 343000), 'temperature': 24.399999618530273, 'location': 1}]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connection_usage_native_reference.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`The queried results can only be fetched once. For example, only one of `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all()`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fetch_all_into_dict()`),` can be used in the example above. Repeated fetches will result in an empty list.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`RestClient`),` class is a direct wrapper for the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/rest-api"},`REST API`),`. It contains only a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method for executing arbitrary SQL statements and returning the result.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from taosrest import RestClient

client = RestClient("http://localhost:6041", user="root", password="taosdata")
res: dict = client.sql("SELECT ts, current FROM power.meters LIMIT 1")
print(res)

# output:
# {'status': 'succ', 'head': ['ts', 'current'], 'column_meta': [['ts', 9, 8], ['current', 6, 4]], 'data': [[datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.3]], 'rows': 1}


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/rest_client_example.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For a more detailed description of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.taosdata.com/api/taospy/taosrest/restclient.html"},`RestClient`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`query`),` method of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class can be used to query data and return the result data of type `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosResult`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn.execute("drop database if exists connwspy")
conn.execute("create database if not exists connwspy wal_retention_period 3600")
conn.execute("use connwspy")
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute("create table if not exists tb1 using stb tags (1)")
conn.execute("insert into tb1 values (now, 1)")
conn.execute("insert into tb1 values (now, 2)")
conn.execute("insert into tb1 values (now, 3)")

r = conn.execute("select * from stb")
result = conn.query("select * from stb")
num_of_fields = result.field_count
print(num_of_fields)

for row in result:
    print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_websocket_examples.py"},`view source code`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"execute-sql-with-reqid"},`Execute SQL with reqId`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`By using the optional req_id parameter, you can specify a request ID that can be used for tracing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"taosconnection-class-1"},`TaosConnection class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As the way to connect introduced above but add `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`req_id`),` argument.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"execute method\"","title":"\"execute","method\"":true},`conn = taos.connect()
# Execute a sql, ignore the result set, just get affected rows. It's useful for DDL and DML statement.
conn.execute("DROP DATABASE IF EXISTS test", req_id=1)
conn.execute("CREATE DATABASE test", req_id=2)
# change database. same as execute "USE db"
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)", req_id=3)
affected_row = conn.execute("INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+1m, 23.5) (now+2m, 24.4)", req_id=4)
print("affected_row", affected_row)
# output:
# affected_row 3
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connection_usage_native_reference_with_req_id.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"query method\"","title":"\"query","method\"":true},`# Execute a sql and get its result set. It's useful for SELECT statement
result = conn.query("SELECT * from weather", req_id=5)

# Get fields from result
fields = result.fields
for field in fields:
    print(field)  # {name: ts, type: 9, bytes: 8}

# output:
# {name: ts, type: 9, bytes: 8}
# {name: temperature, type: 6, bytes: 4}
# {name: location, type: 4, bytes: 4}

# Get data from result as list of tuple
data = result.fetch_all()
print(data)
# output:
# [(datetime.datetime(2022, 4, 27, 9, 4, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 5, 25, 367000), 23.5, 1), (datetime.datetime(2022, 4, 27, 9, 6, 25, 367000), 24.399999618530273, 1)]

# Or get data from result as a list of dict
# map_data = result.fetch_all_into_dict()
# print(map_data)
# output:
# [{'ts': datetime.datetime(2022, 4, 27, 9, 1, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 2, 15, 343000), 'temperature': 23.5, 'location': 1}, {'ts': datetime.datetime(2022, 4, 27, 9, 3, 15, 343000), 'temperature': 24.399999618530273, 'location': 1}]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connection_usage_native_reference_with_req_id.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-taosresult-class-1"},`Use of TaosResult class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As the way to fetch data introduced above but add `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`req_id`),` argument.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"blocks_iter method\"","title":"\"blocks_iter","method\"":true},`import taos

conn = taos.connect()
conn.execute("DROP DATABASE IF EXISTS test", req_id=1)
conn.execute("CREATE DATABASE test", req_id=2)
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)", req_id=3)
# prepare data
for i in range(2000):
    location = str(i % 10)
    tb = "t" + location
    conn.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)", req_id=4+i)

result: taos.TaosResult = conn.query("SELECT * FROM weather", req_id=2004)

block_index = 0
blocks: taos.TaosBlocks = result.blocks_iter()
for rows, length in blocks:
    print("block ", block_index, " length", length)
    print("first row in this block:", rows[0])
    block_index += 1

conn.close()

# possible output:
# block  0  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 46000), 23.5, 0)
# block  1  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 76000), 23.5, 3)
# block  2  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 99000), 23.5, 6)
# block  3  length 400
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 122000), 23.5, 9)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/result_set_with_req_id_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-the-taoscursor-class-1"},`Use of the TaosCursor class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosConnection`),` class and the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosResult`),` class already implement all the functionality of the native interface. If you are familiar with the interfaces in the PEP249 specification, you can also use the methods provided by the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TaosCursor`),` class.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of TaosCursor\"","title":"\"Use","of":true,"TaosCursor\"":true},`import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS test", req_id=1)
cursor.execute("CREATE DATABASE test", req_id=2)
cursor.execute("USE test", req_id=3)
cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)", req_id=4)

for i in range(1000):
    location = str(i % 10)
    tb = "t" + location
    cursor.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)", req_id=5+i)

cursor.execute("SELECT count(*) FROM weather", req_id=1005)
data = cursor.fetchall()
print("count:", data[0][0])
cursor.execute("SELECT tbname, * FROM weather LIMIT 2", req_id=1006)
col_names = [meta[0] for meta in cursor.description]
print(col_names)
rows = cursor.fetchall()
print(rows)

cursor.close()
conn.close()

# output:
# count: 2000
# ['tbname', 'ts', 'temperature', 'location']
# row_count: -1
# [('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 392000), 23.5, 0), ('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 393000), 23.5, 0)]

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/cursor_usage_native_reference_with_req_id.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-taosrestcursor-class-1"},`Use of TaosRestCursor class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As the way to connect introduced above but add `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`req_id`),` argument.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of TaosRestCursor\"","title":"\"Use","of":true,"TaosRestCursor\"":true},`# create STable
cursor = conn.cursor()
cursor.execute("DROP DATABASE IF EXISTS power", req_id=1)
cursor.execute("CREATE DATABASE power", req_id=2)
cursor.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)", req_id=3)

# insert data
cursor.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)""", req_id=4)
print("inserted row count:", cursor.rowcount)

# query data
cursor.execute("SELECT * FROM power.meters LIMIT 3", req_id=5)
# get total rows
print("queried row count:", cursor.rowcount)
# get column names from cursor
column_names = [meta[0] for meta in cursor.description]
# get rows
data = cursor.fetchall()
print(column_names)
for row in data:
    print(row)

# output:
# inserted row count: 8
# queried row count: 3
# ['ts', 'current', 'voltage', 'phase', 'location', 'groupid']
# [datetime.datetime(2018, 10, 3, 14, 38, 5, 500000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 11.8, 221, 0.28, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 16, 600000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 13.4, 223, 0.29, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.8, 223, 0.29, 'california.losangeles', 3]
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_rest_with_req_id_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.execute`),`: Used to execute arbitrary SQL statements.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.rowcount`),` : For write operations, returns the number of successful rows written. For query operations, returns the number of rows in the result set.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`cursor.description`),` : Returns the description of the field. Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.taosdata.com/api/taospy/taosrest/cursor.html"},`TaosRestCursor`),` for the specific format of the description information.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"use-of-the-restclient-class-1"},`Use of the RestClient class`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`RestClient`),` class is a direct wrapper for the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/rest-api"},`REST API`),`. It contains only a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method for executing arbitrary SQL statements and returning the result.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Use of RestClient\"","title":"\"Use","of":true,"RestClient\"":true},`from taosrest import RestClient

client = RestClient("http://localhost:6041", user="root", password="taosdata")
res: dict = client.sql("SELECT ts, current FROM power.meters LIMIT 1", req_id=1)
print(res)

# output:
# {'status': 'succ', 'head': ['ts', 'current'], 'column_meta': [['ts', 9, 8], ['current', 6, 4]], 'data': [[datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.3]], 'rows': 1}


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/rest_client_with_req_id_example.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For a more detailed description of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql()`),` method, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.taosdata.com/api/taospy/taosrest/restclient.html"},`RestClient`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As the way to connect introduced above but add `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`req_id`),` argument.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`conn.execute("drop database if exists connwspy", req_id=1)
conn.execute("create database if not exists connwspy", req_id=2)
conn.execute("use connwspy", req_id=3)
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)", req_id=4)
conn.execute("create table if not exists tb1 using stb tags (1)", req_id=5)
conn.execute("insert into tb1 values (now, 1)", req_id=6)
conn.execute("insert into tb1 values (now, 2)", req_id=7)
conn.execute("insert into tb1 values (now, 3)", req_id=8)

r = conn.execute("select * from stb", req_id=9)
result = conn.query("select * from stb", req_id=10)
num_of_fields = result.field_count
print(num_of_fields)

for row in result:
    print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_websocket_with_req_id_examples.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`conn.execute`),`: can use to execute arbitrary SQL statements, and return the number of rows affected.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`conn.query`),`: can use to execute query SQL statements, and return the query results.`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"used-with-pandas"},`Used with pandas`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"rest",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import pandas
from sqlalchemy import create_engine, text

engine = create_engine("taos://root:taosdata@localhost:6030/power")
conn = engine.connect()
df = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()


# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                        ts  current  ...               location  groupid
# 0 2018-10-03 14:38:05.500     11.8  ...  california.losangeles        2
# 1 2018-10-03 14:38:16.600     13.4  ...  california.losangeles        2
# 2 2018-10-03 14:38:05.000     10.8  ...  california.losangeles        3

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/conn_native_pandas.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"rest",label:"REST connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import pandas
from sqlalchemy import create_engine, text

engine = create_engine("taosrest://root:taosdata@localhost:6041")
conn = engine.connect()
df: pandas.DataFrame = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()

# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                                 ts  current  ...               location  groupid
# 0 2018-10-03 06:38:05.500000+00:00     11.8  ...  california.losangeles        2
# 1 2018-10-03 06:38:16.600000+00:00     13.4  ...  california.losangeles        2
# 2        2018-10-03 06:38:05+00:00     10.8  ...  california.losangeles        3

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/conn_rest_pandas.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import pandas
from sqlalchemy import create_engine, text
import taos

taos_conn = taos.connect()
taos_conn.execute('drop database if exists power')
taos_conn.execute('create database if not exists power wal_retention_period 3600')
taos_conn.execute("use power")
taos_conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
# insert data
taos_conn.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) 
VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) 
('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) 
    VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) 
    VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) 
    VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)""")

engine = create_engine("taosws://root:taosdata@localhost:6041")
conn = engine.connect()
df: pandas.DataFrame = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()

# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                        ts  current  ...                 location  groupid
# 0 2018-10-03 14:38:05.000     10.3  ...  California.SanFrancisco        2
# 1 2018-10-03 14:38:15.000     12.6  ...  California.SanFrancisco        2
# 2 2018-10-03 14:38:16.800     12.3  ...  California.SanFrancisco        2

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/conn_websocket_pandas.py"},`view source code`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"writing-data-via-parameter-binding"},`Writing data via parameter binding`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The Python connector provides a parameter binding api for inserting data. Similar to most databases, TDengine currently only supports the question mark `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`?`),` to indicate the parameters to be bound.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"create-stmt"},`Create Stmt`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`statement`),` method in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Connection`),` to create the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`stmt`),` for parameter binding.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`import taos

conn = taos.connect()
stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"parameter-binding"},`parameter binding`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`new_multi_binds`),` function to create the parameter list for parameter bindings.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`params = new_multi_binds(16)
params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
params[1].bool((True, None, False))
params[2].tinyint([-128, -128, None])  # -128 is tinyint null
params[3].tinyint([0, 127, None])
params[4].smallint([3, None, 2])
params[5].int([3, 4, None])
params[6].bigint([3, 4, None])
params[7].tinyint_unsigned([3, 4, None])
params[8].smallint_unsigned([3, 4, None])
params[9].int_unsigned([3, 4, None])
params[10].bigint_unsigned([3, 4, None])
params[11].float([3, None, 1])
params[12].double([3, None, 1.2])
params[13].binary(["abc", "dddafadfadfadfadfa", None])
params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
params[15].timestamp([None, None, 1626861392591])
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`bind_param`),` (for a single row) method or the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`bind_param_batch`),` (for multiple rows) method to set the values.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.bind_param_batch(params)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"execute-sql"},`execute sql`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`execute`),` method to execute sql.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.execute()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"close-stmt"},`Close Stmt`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.close()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"example"},`Example`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`#!

import taosws

import taos

db_name = 'test_ws_stmt'


def before():
    taos_conn = taos.connect()
    taos_conn.execute("drop database if exists %s" % db_name)
    taos_conn.execute("create database %s" % db_name)
    taos_conn.select_db(db_name)
    taos_conn.execute("create table t1 (ts timestamp, a int, b float, c varchar(10))")
    taos_conn.execute(
        "create table stb1 (ts timestamp, a int, b float, c varchar(10)) tags (t1 int, t2 binary(10))")
    taos_conn.close()


def stmt_insert():
    before()

    conn = taosws.connect('taosws://root:taosdata@localhost:6041/%s' % db_name)

    while True:
        try:
            stmt = conn.statement()
            stmt.prepare("insert into t1 values (?, ?, ?, ?)")

            stmt.bind_param([
                taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
                taosws.ints_to_column([1, 2, 3, 4]),
                taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
                taosws.varchar_to_column(['a', 'b', 'c', 'd']),
            ])

            stmt.add_batch()
            rows = stmt.execute()
            print(rows)
            stmt.close()
        except Exception as e:
            if 'Retry needed' in e.args[0]:  # deal with [0x0125] Retry needed
                continue
            else:
                raise e

        break


def stmt_insert_into_stable():
    before()

    conn = taosws.connect("taosws://root:taosdata@localhost:6041/%s" % db_name)

    while True:
        try:
            stmt = conn.statement()
            stmt.prepare("insert into ? using stb1 tags (?, ?) values (?, ?, ?, ?)")
            stmt.set_tbname('stb1_1')
            stmt.set_tags([
                taosws.int_to_tag(1),
                taosws.varchar_to_tag('aaa'),
            ])
            stmt.bind_param([
                taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
                taosws.ints_to_column([1, 2, 3, 4]),
                taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
                taosws.varchar_to_column(['a', 'b', 'c', 'd']),
            ])

            stmt.add_batch()
            rows = stmt.execute()
            print(rows)
            stmt.close()
        except Exception as e:
            if 'Retry needed' in e.args[0]:  # deal with [0x0125] Retry needed
                continue
            else:
                raise e

        break

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/stmt_example.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"create-stmt-1"},`Create Stmt`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`statement`),` method in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Connection`),` to create the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`stmt`),` for parameter binding.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`import taosws

conn = taosws.connect('taosws://localhost:6041/test')
stmt = conn.statement()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"prepare-sql"},`Prepare sql`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`prepare`),` method in stmt to prepare sql.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.prepare("insert into t1 values (?, ?, ?, ?)")
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"parameter-binding-1"},`parameter binding`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`bind_param`),` method to bind parameters.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.bind_param([
    taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
    taosws.ints_to_column([1, 2, 3, 4]),
    taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
    taosws.varchar_to_column(['a', 'b', 'c', 'd']),
])
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`add_batch`),` method to add parameters to the batch.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.add_batch()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"execute-sql-1"},`execute sql`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Call `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`execute`),` method to execute sql.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.execute()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"close-stmt-1"},`Close Stmt`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`stmt.close()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"example-1"},`Example`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`#!
import time

import taosws

import taos


def before_test(db_name):
    taos_conn = taos.connect()
    taos_conn.execute("drop database if exists %s" % db_name)
    taos_conn.execute("create database %s" % db_name)
    taos_conn.select_db(db_name)
    taos_conn.execute("create table t1 (ts timestamp, a int, b float, c varchar(10))")
    taos_conn.execute(
        "create table stb1 (ts timestamp, a int, b float, c varchar(10)) tags (t1 int, t2 binary(10))")
    taos_conn.close()


def after_test(db_name):
    taos_conn = taos.connect()
    taos_conn.execute("drop database if exists %s" % db_name)
    taos_conn.close()


def stmt_insert():
    db_name = 'test_ws_stmt_{}'.format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect('taosws://root:taosdata@localhost:6041/%s' % db_name)

    stmt = conn.statement()
    stmt.prepare("insert into t1 values (?, ?, ?, ?)")

    stmt.bind_param([
        taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
        taosws.ints_to_column([1, 2, 3, 4]),
        taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
        taosws.varchar_to_column(['a', 'b', 'c', 'd']),
    ])

    stmt.add_batch()
    rows = stmt.execute()
    assert rows == 4
    stmt.close()
    after_test(db_name)


def stmt_insert_into_stable():
    db_name = 'test_ws_stmt_{}'.format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect("taosws://root:taosdata@localhost:6041/%s" % db_name)

    stmt = conn.statement()
    stmt.prepare("insert into ? using stb1 tags (?, ?) values (?, ?, ?, ?)")
    stmt.set_tbname('stb1_1')
    stmt.set_tags([
        taosws.int_to_tag(1),
        taosws.varchar_to_tag('aaa'),
    ])
    stmt.bind_param([
        taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
        taosws.ints_to_column([1, 2, 3, 4]),
        taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
        taosws.varchar_to_column(['a', 'b', 'c', 'd']),
    ])

    stmt.add_batch()
    rows = stmt.execute()
    assert rows == 4
    stmt.close()
    after_test(db_name)


if __name__ == '__main__':
    stmt_insert()
    stmt_insert_into_stable()

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/stmt_websocket_example.py"},`view source code`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"schemaless-writing"},`Schemaless Writing`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Connector support schemaless insert.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"list",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"list",label:"List Insert",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"simple-insert"},`Simple insert`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"insert-with-ttl-argument"},`Insert with ttl argument`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import SmlProtocol, SmlPrecision

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, ttl=1000)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, ttl=1000)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_ttl.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"insert-with-req_id-argument"},`Insert with req_id argument`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import SmlProtocol, SmlPrecision

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=1)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=2)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_req_id.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"raw",label:"Raw Insert",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"simple-insert-1"},`Simple insert`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        res = conn.schemaless_insert_raw(lines, 1, 0)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except SchemalessError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_raw.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"insert-with-ttl-argument-1"},`Insert with ttl argument`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    ttl = 1000
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    ttl = 1000
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        ttl = 1000
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_raw_ttl.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h5",{"id":"insert-with-req_id-argument-1"},`Insert with req_id argument`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_raw_req_id.py"},`view source code`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"schemaless-with-reqid"},`Schemaless with reqId`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`There is a optional parameter called `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`req_id`),` in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`schemaless_insert`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`schemaless_insert_raw`),` method. This reqId can be used to request link tracing.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import SmlProtocol, SmlPrecision

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=1)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=2)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_req_id.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/schemaless_insert_raw_req_id.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"data-subscription"},`Data Subscription`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Connector support data subscription. For more information about subscroption, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../develop/tmq/"},`Data Subscription`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"create-a-topic"},`Create a Topic`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`To create topic, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../develop/tmq/#create-a-topic"},`Data Subscription`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"create-a-consumer"},`Create a Consumer`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The consumer in the connector contains the subscription api. The syntax for creating a consumer is consumer = Consumer(configs). For more subscription api parameters, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../develop/tmq/#create-a-consumer"},`Data Subscription`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from taos.tmq import Consumer

consumer = Consumer({"group.id": "local", "td.connect.ip": "127.0.0.1"})
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In addition to native connections, the connector also supports subscriptions via websockets.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The syntax for creating a consumer is "consumer = consumer = Consumer(conf=configs)". You need to specify that the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`td.connect.websocket.scheme`),` parameter is set to "ws" in the configuration. For more subscription api parameters, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../develop/tmq/#create-a-consumer"},`Data Subscription`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taosws

consumer = taosws.(conf={"group.id": "local", "td.connect.websocket.scheme": "ws"})
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"subscribe-to-a-topic"},`Subscribe to a Topic`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`subscribe`),` function is used to subscribe to a list of topics.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`consumer.subscribe(['topic1', 'topic2'])
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`subscribe`),` function is used to subscribe to a list of topics.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`consumer.subscribe(['topic1', 'topic2'])
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"consume-messages"},`Consume messages`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`poll`),` function is used to consume data in tmq. The parameter of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`poll`),` function is a value of type float representing the timeout in seconds. It returns a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Message`),` before timing out, or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`None`),` on timing out. You have to handle error messages in response data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`while True:
    res = consumer.poll(1)
    if not res:
        continue
    err = res.error()
    if err is not None:
        raise err
    val = res.value()

    for block in val:
        print(block.fetchall())
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`poll`),` function is used to consume data in tmq. The parameter of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`poll`),` function is a value of type float representing the timeout in seconds. It returns a `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Message`),` before timing out, or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`None`),` on timing out. You have to handle error messages in response data.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`while True:
    res = consumer.poll(timeout=1.0)
    if not res:
        continue
    err = res.error()
    if err is not None:
        raise err
    for block in message:
        for row in block:
            print(row)
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"assignment-subscription-offset"},`Assignment subscription Offset`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`assignment`),` function is used to get the assignment of the topic. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`assignments = consumer.assignment()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`seek`),` function is used to reset the assignment of the topic.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`tp = TopicPartition(topic='topic1', partition=0, offset=0)
consumer.seek(tp)
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`assignment`),` function is used to get the assignment of the topic. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`assignments = consumer.assignment()
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`seek`),` function is used to reset the assignment of the topic.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`consumer.seek(topic='topic1', partition=0, offset=0)
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"close-subscriptions"},`Close subscriptions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You should unsubscribe to the topics and close the consumer after consuming.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`consumer.unsubscribe()
consumer.close()
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You should unsubscribe to the topics and close the consumer after consuming.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`consumer.unsubscribe()
consumer.close()
`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h4",{"id":"full-sample-code"},`Full Sample Code`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"native",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"native",label:"native connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`from taos.tmq import Consumer
import taos


def init_tmq_env(db, topic):
    conn = taos.connect()
    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))
    conn.execute("create database if not exists {} wal_retention_period 3600".format(db))
    conn.select_db(db)
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))")
    conn.execute("create table if not exists tb1 using stb1 tags(1, 't1')")
    conn.execute("create table if not exists tb2 using stb1 tags(2, 't2')")
    conn.execute("create table if not exists tb3 using stb1 tags(3, 't3')")
    conn.execute("create topic if not exists {} as select ts, c1, c2, c3 from stb1".format(topic))
    conn.execute("insert into tb1 values (now, 1, 1.0, 'tmq test')")
    conn.execute("insert into tb2 values (now, 2, 2.0, 'tmq test')")
    conn.execute("insert into tb3 values (now, 3, 3.0, 'tmq test')")


def cleanup(db, topic):
    conn = taos.connect()
    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))


if __name__ == '__main__':
    init_tmq_env("tmq_test", "tmq_test_topic")  # init env
    consumer = Consumer(
        {
            "group.id": "tg2",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
        }
    )
    consumer.subscribe(["tmq_test_topic"])

    try:
        while True:
            res = consumer.poll(1)
            if not res:
                break
            err = res.error()
            if err is not None:
                raise err
            val = res.value()

            for block in val:
                print(block.fetchall())
    finally:
        consumer.unsubscribe()
        consumer.close()
        cleanup("tmq_test", "tmq_test_topic")

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/tmq_example.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
from taos.tmq import Consumer
import taosws


def prepare():
    conn = taos.connect()
    conn.execute("drop topic if exists tmq_assignment_demo_topic")
    conn.execute("drop database if exists tmq_assignment_demo_db")
    conn.execute("create database if not exists tmq_assignment_demo_db wal_retention_period 3600")
    conn.select_db("tmq_assignment_demo_db")
    conn.execute(
        "create table if not exists tmq_assignment_demo_table (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
    conn.execute(
        "create topic if not exists tmq_assignment_demo_topic as select ts, c1, c2, c3 from tmq_assignment_demo_table")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-2s, 1, 1.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-1s, 2, 2.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now, 3, 3.0, 'tmq test')")


def taos_get_assignment_and_seek_demo():
    prepare()
    consumer = Consumer(
        {
            "group.id": "0",
            # should disable snapshot,
            # otherwise it will cause invalid params error
            "experimental.snapshot.enable": "false",
        }
    )
    consumer.subscribe(["tmq_assignment_demo_topic"])

    # get topic assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)

    # poll
    consumer.poll(1)
    consumer.poll(1)

    # get topic assignment again
    after_pool_assignments = consumer.assignment()
    for assignment in after_pool_assignments:
        print(assignment)

    # seek to the beginning
    for assignment in assignments:
        consumer.seek(assignment)

    # now the assignment should be the same as before poll
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)


if __name__ == '__main__':
    taos_get_assignment_and_seek_demo()

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/tmq_assignment_example.py"},`view source code`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"websocket",label:"WebSocket connection",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`#!/usr/bin/python3
from taosws import Consumer

conf = {
    "td.connect.websocket.scheme": "ws",
    "group.id": "0",
}
consumer = Consumer(conf)

consumer.subscribe(["test"])

while True:
    message = consumer.poll(timeout=1.0)
    if message:
        id = message.vgroup()
        topic = message.topic()
        database = message.database()

        for block in message:
            nrows = block.nrows()
            ncols = block.ncols()
            for row in block:
                print(row)
            values = block.fetchall()
            print(nrows, ncols)

        # consumer.commit(message)
    else:
        break

consumer.close()

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/tmq_websocket_example.py"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import taos
import taosws


def prepare():
    conn = taos.connect()
    conn.execute("drop topic if exists tmq_assignment_demo_topic")
    conn.execute("drop database if exists tmq_assignment_demo_db")
    conn.execute("create database if not exists tmq_assignment_demo_db wal_retention_period 3600")
    conn.select_db("tmq_assignment_demo_db")
    conn.execute(
        "create table if not exists tmq_assignment_demo_table (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
    conn.execute(
        "create topic if not exists tmq_assignment_demo_topic as select ts, c1, c2, c3 from tmq_assignment_demo_table")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-2s, 1, 1.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-1s, 2, 2.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now, 3, 3.0, 'tmq test')")


def taosws_get_assignment_and_seek_demo():
    prepare()
    consumer = taosws.Consumer(conf={
        "td.connect.websocket.scheme": "ws",
        # should disable snapshot,
        # otherwise it will cause invalid params error
        "experimental.snapshot.enable": "false",
        "group.id": "0",
    })
    consumer.subscribe(["tmq_assignment_demo_topic"])

    # get topic assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment.to_string())

    # poll
    consumer.poll(1)
    consumer.poll(1)

    # get topic assignment again
    after_poll_assignments = consumer.assignment()
    for assignment in after_poll_assignments:
        print(assignment.to_string())

    # seek to the beginning
    for assignment in assignments:
        for a in assignment.assignments():
            consumer.seek(assignment.topic(), a.vg_id(), a.offset())

    # now the assignment should be the same as before poll
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment.to_string())


if __name__ == '__main__':
    taosws_get_assignment_and_seek_demo()

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/tmq_websocket_assgnment_example.py"},`view source code`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"other-sample-programs"},`Other sample programs`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Example program links`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`Example program content`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-python/blob/main/examples/bind-multi.py"},`bind_multi.py`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`parameter binding, bind multiple rows at once`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-python/blob/main/examples/bind-row.py"},`bind_row.py`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`parameter binding, bind one row at once`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-python/blob/main/examples/insert-lines.py"},`insert_lines.py`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`InfluxDB line protocol writing`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-python/blob/main/examples/json-tag.py"},`json_tag.py`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`Use JSON type tags`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"td","href":"https://github.com/taosdata/taos-connector-python/blob/main/examples/tmq_consumer.py"},`tmq_consumer.py`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`TMQ subscription`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"other-notes"},`Other notes`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"about-nanoseconds"},`About nanoseconds`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Due to the current imperfection of Python's nanosecond support (see link below), the current implementation returns integers at nanosecond precision instead of the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`datetime`),` type produced by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`ms`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`us`),`, which application developers will need to handle on their own. And it is recommended to use pandas' to_datetime(). The Python Connector may modify the interface in the future if Python officially supports nanoseconds in full.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds"},`https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://www.python.org/dev/peps/pep-0564/"},`https://www.python.org/dev/peps/pep-0564/`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"important-update"},`Important Update`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`[`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`Release Notes`),`]`,` (`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-python/releases"},`https://github.com/taosdata/taos-connector-python/releases`),`)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"api-reference"},`API Reference`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.taosdata.com/api/taospy/taos/"},`taos`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://docs.taosdata.com/api/taospy/taosrest"},`taosrest`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"frequently-asked-questions"},`Frequently Asked Questions`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Welcome to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-python/issues"},`ask questions or report questions`),`.`));};MDXContent.isMDXComponent=true;

/***/ })

}]);