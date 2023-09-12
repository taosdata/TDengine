"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[3165],{

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

/***/ 7553:
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
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'Grafana',sidebar_label:'Grafana',description:'This document describes how to integrate TDengine with Grafana.'};const contentTitle=undefined;const metadata={"unversionedId":"third-party/grafana","id":"third-party/grafana","title":"Grafana","description":"This document describes how to integrate TDengine with Grafana.","source":"@site/docs/20-third-party/01-grafana.mdx","sourceDirName":"20-third-party","slug":"/third-party/grafana","permalink":"/docs-en/third-party/grafana","draft":false,"tags":[],"version":"current","sidebarPosition":1,"frontMatter":{"title":"Grafana","sidebar_label":"Grafana","description":"This document describes how to integrate TDengine with Grafana."},"sidebar":"defaultSidebar","previous":{"title":"Third Party Tools","permalink":"/docs-en/third-party/"},"next":{"title":"Prometheus","permalink":"/docs-en/third-party/prometheus"}};const assets={};const toc=[{value:'Prerequisites',id:'prerequisites',level:2},{value:'Installing Grafana',id:'installing-grafana',level:2},{value:'Configuring Grafana',id:'configuring-grafana',level:2},{value:'Install Grafana Plugin and Configure Data Source',id:'install-grafana-plugin-and-configure-data-source',level:3},{value:'Create Dashboard',id:'create-dashboard',level:3},{value:'Importing the Dashboard',id:'importing-the-dashboard',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine can be quickly integrated with the open-source data visualization system `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.grafana.com/"},`Grafana`),` to build a data monitoring and alerting system. The whole process does not require any code development. And you can visualize the contents of the data tables in TDengine on a dashboard. You can learn more about using the TDengine plugin on `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/grafanaplugin/blob/master/README.md"},`GitHub`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"prerequisites"},`Prerequisites`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In order for Grafana to add the TDengine data source successfully, the following preparations are required:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`The TDengine cluster is deployed and functioning properly`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`taosAdapter is installed and running properly. Please refer to the taosAdapter manual for details.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Record these values:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TDengine REST API url: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`http://tdengine.local:6041`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TDengine cluster authorization, with user + password.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"installing-grafana"},`Installing Grafana`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine currently supports Grafana versions 7.5 and above. Users can go to the Grafana official website to download the installation package and execute the installation according to the current operating system. The download address is as follows: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/grafana/download"},`https://grafana.com/grafana/download`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"configuring-grafana"},`Configuring Grafana`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"install-grafana-plugin-and-configure-data-source"},`Install Grafana Plugin and Configure Data Source`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"script",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"gui",label:"With GUI",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Under Grafana 8, plugin catalog allows you to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/grafana/next/administration/plugin-management/#plugin-catalog"},`browse and manage plugins within Grafana`),` (but for Grafana 7.x, use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`With Script`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`Install & Configure Manually`),`). Find the page at `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`Configurations > Plugins`),`, search `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`TDengine`),` and click it to install.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"Search tdengine in grafana plugins",src:(__webpack_require__(7089)/* ["default"] */ .Z),width:"1264",height:"420"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Installation may cost some minutes, then you can `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`Create a TDengine data source`),`:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"Install and configure Grafana data source",src:(__webpack_require__(3265)/* ["default"] */ .Z),width:"1313",height:"623"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Then you can add a TDengine data source by filling up the configuration options.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database Grafana plugin add data source",src:(__webpack_require__(2359)/* ["default"] */ .Z),width:"1256",height:"411"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can create dashboards with TDengine now.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"script",label:"With Script",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`On a server with Grafana installed, run `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`install.sh`),` with TDengine url and username/passwords will install TDengine data source plugin and add a data source named TDengine. This is the recommended way for Grafana 7.x or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/grafana/latest/administration/provisioning/"},`Grafana provisioning`),` users.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sh"},`bash -c "$(curl -fsSL \\
  https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)" -- \\
  -a http://localhost:6041 \\
  -u root \\
  -p taosdata
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Restart Grafana service and open Grafana in web-browser, usually `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"http://localhost:3000"},`http://localhost:3000`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Save the script and type `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`./install.sh --help`),` for the full usage of the script.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"manual",label:"Install & Configure Manually",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Follow the installation steps in `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/grafana/plugins/tdengine-datasource/?tab=installation"},`Grafana`),` with the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/grafana/latest/administration/cli/"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"a"},`grafana-cli`),` command-line tool`),` for plugin installation.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`grafana-cli plugins install tdengine-datasource
# with sudo
sudo -u grafana grafana-cli plugins install tdengine-datasource
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can also download zip files from `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/grafanaplugin/releases/latest"},`GitHub`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/grafana/plugins/tdengine-datasource/?tab=installation"},`Grafana`),` and install manually. The commands are as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`GF_VERSION=3.3.1
# from GitHub
wget https://github.com/taosdata/grafanaplugin/releases/download/v$GF_VERSION/tdengine-datasource-$GF_VERSION.zip
# from Grafana
wget -O tdengine-datasource-$GF_VERSION.zip https://grafana.com/api/plugins/tdengine-datasource/versions/$GF_VERSION/download
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Take CentOS 7.2 for example, extract the plugin package to /var/lib/grafana/plugins directory, and restart grafana.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`sudo unzip tdengine-datasource-$GF_VERSION.zip -d /var/lib/grafana/plugins/
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`If Grafana is running in a Docker environment, the TDengine plugin can be automatically installed and set up using the following environment variable settings:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`GF_INSTALL_PLUGINS=tdengine-datasource
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Now users can log in to the Grafana server (username/password: admin/admin) directly through the URL `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http://localhost:3000`),` and add a datasource through `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Configuration -> Data Sources`),` on the left side, as shown in the following figure.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin add datasource 1",src:(__webpack_require__(782)/* ["default"] */ .Z),width:"1478",height:"547"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Click `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Add data source`),` to enter the Add data source page, and enter TDengine in the query box to add it, as shown in the following figure.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin add datasource 2",src:(__webpack_require__(2594)/* ["default"] */ .Z),width:"1602",height:"444"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Enter the datasource configuration page, and follow the default prompts to modify the corresponding configuration.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin add database 3",src:(__webpack_require__(7495)/* ["default"] */ .Z),width:"1540",height:"1020"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Host: IP address of the server where the components of the TDengine cluster provide REST service (offered by taosd before 2.4 and by taosAdapter since 2.4) and the port number of the TDengine REST service (6041), by default use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`http://localhost:6041`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`User: TDengine user name.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Password: TDengine user password.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Click `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Save & Test`),` to test. You should see a success message if the test worked.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin add database 4",src:(__webpack_require__(608)/* ["default"] */ .Z),width:"971",height:"555"}))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{value:"container",label:"Container",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/grafana/next/setup-grafana/installation/docker/#install-plugins-in-the-docker-container"},`Install plugins in the Docker container`),`. This will install `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`tdengine-datasource`),` plugin when Grafana container starts:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-bash"},`docker run -d \\
  -p 3000:3000 \\
  --name=grafana \\
  -e "GF_INSTALL_PLUGINS=tdengine-datasource" \\
  grafana/grafana
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can setup a zero-configuration stack for TDengine + Grafana by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.docker.com/compose/"},`docker-compose`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/grafana/latest/administration/provisioning/"},`Grafana provisioning`),` file:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Save the provisioning configuration file to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`tdengine.yml`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-yml"},`apiVersion: 1
datasources:
- name: TDengine
  type: tdengine-datasource
  orgId: 1
  url: "$TDENGINE_API"
  isDefault: true
  secureJsonData:
    url: "$TDENGINE_URL"
    basicAuth: "$TDENGINE_BASIC_AUTH"
    token: "$TDENGINE_CLOUD_TOKEN"
  version: 1
  editable: true
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Write `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`docker-compose.yml`),` with `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://hub.docker.com/r/tdengine/tdengine"},`TDengine`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://hub.docker.com/r/grafana/grafana"},`Grafana`),` image.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-yml"},`version: "3.7"

services:
  tdengine:
    image: tdengine/tdengine:3.0.2.4
    environment:
      TAOS_FQDN: tdengine
    volumes:
      - tdengine-data:/var/lib/taos/
  grafana:
    image: grafana/grafana:9.3.6
    volumes:
      - ./tdengine.yml/:/etc/grafana/provisioning/tdengine.yml
      - grafana-data:/var/lib/grafana
    environment:
      # install tdengine plugin at start
      GF_INSTALL_PLUGINS: "tdengine-datasource"
      TDENGINE_URL: "http://tdengine:6041"
      #printf "$TDENGINE_USER:$TDENGINE_PASSWORD" | base64
      TDENGINE_BASIC_AUTH: "cm9vdDp0YmFzZTEyNQ=="
    ports:
      - 3000:3000
volumes:
  grafana-data:
  tdengine-data:
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Start TDengine and Grafana services: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`docker-compose up -d`),`.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Open Grafana `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"http://localhost:3000"},`http://localhost:3000`),`, and you can add dashboard with TDengine now.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"create-dashboard"},`Create Dashboard`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Go back to the main interface to create a dashboard and click Add Query to enter the panel query page:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin create dashboard 1",src:(__webpack_require__(2241)/* ["default"] */ .Z),width:"2145",height:"1027"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`As shown above, select the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDengine`),` data source in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Query`),` and enter the corresponding SQL in the query box below for query.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`INPUT SQL: Enter the desired query (the results being two columns and multiple rows), such as `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`select _wstart, avg(mem_system) from log.dnodes_info where ts >= $from and ts < $to interval($interval)`),`. In this statement, $from, $to, and $interval are variables that Grafana replaces with the query time range and interval. In addition to the built-in variables, custom template variables are also supported.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`ALIAS BY: This allows you to set the current query alias.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`GENERATE SQL: Clicking this button will automatically replace the corresponding variables and generate the final executed statement.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Group by column name(s): `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`group by`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`partition by`),` columns name split by comma. By setting `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`Group by column name(s)`),`, it can show multi-dimension data if Sql is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`group by`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`partition by`),`. Such as, it can show data by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`dnode_ep`),` if sql is  `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`select _wstart as ts, avg(mem_system), dnode_ep from log.dnodes_info where ts>=$from and ts<=$to partition by dnode_ep interval($interval)`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`Group by column name(s)`),` is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`dnode_ep`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Format to: format legend for `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`group by`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`partition by`),`. Such as it can display series data by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`dnode_ep`),` if sql is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`select _wstart as ts, avg(mem_system), dnode_ep from log.dnodes_info where ts>=$from and ts<=$to partition by dnode_ep interval($interval)`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`Group by column name(s)`),` is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`dnode_ep`),` and `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`Format to`),` is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`mem_system_{{dnode_ep}}`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`Since the REST connection because is stateless. Grafana plugin can use <db_name>.<table_name> in the SQL command to specify the database name.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Follow the default prompt to query the average system memory usage for the specified interval on the server where the current TDengine deployment is located as follows.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin create dashboard 2",src:(__webpack_require__(8380)/* ["default"] */ .Z),width:"2139",height:"1012"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The example to query the average system memory usage for the specified interval on each server as follows.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database TDinsight plugin create dashboard 2",src:(__webpack_require__(5843)/* ["default"] */ .Z),width:"2135",height:"1033"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("blockquote",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"blockquote"},`For more information on how to use Grafana to create the appropriate monitoring interface and for more details on using Grafana, refer to the official Grafana `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/docs/"},`documentation`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"importing-the-dashboard"},`Importing the Dashboard`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`You can install TDinsight dashboard in data source configuration page (like `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http://localhost:3000/datasources/edit/1/dashboards`),`) as a monitoring visualization tool for TDengine cluster. Ensure that you use TDinsight for 3.x. Please note TDinsight for 3.x needs to configure and run taoskeeper correctly. Check the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/tdinsight/"},`TDinsight User Manual`),` for the details.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database Grafana plugine import dashboard",src:(__webpack_require__(6669)/* ["default"] */ .Z),width:"1000",height:"286"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`A dashboard for TDengine 2.x has been published on Grafana: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/grafana/dashboards/15167"},`Dashboard 15167 - TDinsight`),`).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For more dashboards using TDengine data source, `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://grafana.com/grafana/dashboards/?dataSource=tdengine-datasource"},`search here in Grafana`),`. Here is a sub list:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://grafana.com/grafana/dashboards/15146"},`15146`),`: Monitor multiple TDengine clusters.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://grafana.com/grafana/dashboards/15155"},`15155`),`: TDengine alert demo.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://grafana.com/grafana/dashboards/15167"},`15167`),`: TDinsight.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"https://grafana.com/grafana/dashboards/16388"},`16388`),`: Telegraf node metrics dashboard using TDengine data source.`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 782:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add_datasource1-ed5f565506c91dac6510f3bad4ed7edc.webp");

/***/ }),

/***/ 2594:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add_datasource2-c124c6abc3e9663b5714ca69de81daf8.webp");

/***/ }),

/***/ 7495:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add_datasource3-ff06d727da5a64b98fc83dbefe5c79cf.webp");

/***/ }),

/***/ 608:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add_datasource4-47e819417e125f6d4585bb6eb5fc80fe.webp");

/***/ }),

/***/ 2241:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create_dashboard1-cae9636d62f4e7403c47dcef72fc47c8.webp");

/***/ }),

/***/ 8380:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create_dashboard2-3e0de476d448138ead986e3fcaeca179.webp");

/***/ }),

/***/ 5843:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create_dashboard3-967eb6660d5c9beac12dd8707261229a.webp");

/***/ }),

/***/ 2359:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/grafana-data-source-5c702178748f0e194ad9fec8765be09f.png");

/***/ }),

/***/ 3265:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/grafana-install-and-config-17271069c1361110cbb2ba1be50be2a3.png");

/***/ }),

/***/ 7089:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/grafana-plugin-search-tdengine-cc5d6c92b8bf4f4587b0e223244a4f10.png");

/***/ }),

/***/ 6669:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ })

}]);