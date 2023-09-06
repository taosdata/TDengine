"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[7786],{

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

/***/ 9276:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _theme_Tabs__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4866);
/* harmony import */ var _theme_TabItem__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5162);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle='高效写入';const metadata={"unversionedId":"develop/insert-data/high-volume","id":"develop/insert-data/high-volume","title":"高效写入","description":"本节介绍如何高效地向 TDengine 写入数据。","source":"@site/docs/07-develop/03-insert-data/60-high-volume.md","sourceDirName":"07-develop/03-insert-data","slug":"/develop/insert-data/high-volume","permalink":"/docs/develop/insert-data/high-volume","draft":false,"tags":[],"version":"current","sidebarPosition":60,"frontMatter":{},"sidebar":"defaultSidebar","previous":{"title":"OpenTSDB JSON 格式协议","permalink":"/docs/develop/insert-data/opentsdb-json"},"next":{"title":"查询数据","permalink":"/docs/develop/query-data/"}};const assets={};const toc=[{value:'高效写入原理',id:'principle',level:2},{value:'客户端程序的角度',id:'application-view',level:3},{value:'数据源的角度',id:'datasource-view',level:3},{value:'服务器配置的角度',id:'setting-view',level:3},{value:'高效写入示例',id:'sample-code',level:2},{value:'场景设计',id:'scenario',level:3},{value:'示例代码',id:'code',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_4__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h1",{"id":"高效写入"},`高效写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本节介绍如何高效地向 TDengine 写入数据。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"principle"},`高效写入原理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"application-view"},`客户端程序的角度`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`从客户端程序的角度来说，高效写入数据要考虑以下几个因素：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`单次写入的数据量。一般来讲，每批次写入的数据量越大越高效（但超过一定阈值其优势会消失）。使用 SQL 写入 TDengine 时，尽量在一条 SQL 中拼接更多数据。目前，TDengine 支持的一条 SQL 的最大长度为 1,048,576（1MB）个字符`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`并发连接数。一般来讲，同时写入数据的并发连接数越多写入越高效（但超过一定阈值反而会下降，取决于服务端处理能力）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`数据在不同表（或子表）之间的分布，即要写入数据的相邻性。一般来说，每批次只向同一张表（或子表）写入数据比向多张表（或子表）写入数据要更高效`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`写入方式。一般来讲：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`参数绑定写入比 SQL 写入更高效。因参数绑定方式避免了 SQL 解析。（但增加了 C 接口的调用次数，对于连接器也有性能损耗）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SQL 写入不自动建表比自动建表更高效。因自动建表要频繁检查表是否存在`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`SQL 写入比无模式写入更高效。因无模式写入会自动建表且支持动态更改表结构`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`客户端程序要充分且恰当地利用以上几个因素。在单次写入中尽量只向同一张表（或子表）写入数据，每批次写入的数据量经过测试和调优设定为一个最适合当前系统处理能力的数值，并发写入的连接数同样经过测试和调优后设定为一个最适合当前系统处理能力的数值，以实现在当前系统中的最佳写入速度。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"datasource-view"},`数据源的角度`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`客户端程序通常需要从数据源读数据再写入 TDengine。从数据源角度来说，以下几种情况需要在读线程和写线程之间增加队列：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`有多个数据源，单个数据源生成数据的速度远小于单线程写入的速度，但数据量整体比较大。此时队列的作用是把多个数据源的数据汇聚到一起，增加单次写入的数据量。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`单个数据源生成数据的速度远大于单线程写入的速度。此时队列的作用是增加写入的并发度。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`单张表的数据分散在多个数据源。此时队列的作用是将同一张表的数据提前汇聚到一起，提高写入时数据的相邻性。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`如果写应用的数据源是 Kafka, 写应用本身即 Kafka 的消费者，则可利用 Kafka 的特性实现高效写入。比如：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`将同一张表的数据写到同一个 Topic 的同一个 Partition，增加数据的相邻性`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`通过订阅多个 Topic 实现数据汇聚`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`通过增加 Consumer 线程数增加写入的并发度`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`通过增加每次 Fetch 的最大数据量来增加单次写入的最大数据量`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"setting-view"},`服务器配置的角度`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`从服务端配置的角度，要根据系统中磁盘的数量，磁盘的 I/O 能力，以及处理器能力在创建数据库时设置适当的 vgroups 数量以充分发挥系统性能。如果 vgroups 过少，则系统性能无法发挥；如果 vgroups 过多，会造成无谓的资源竞争。常规推荐 vgroups 数量为 CPU 核数的 2 倍，但仍然要结合具体的系统资源配置进行调优。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`更多调优参数，请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../taos-sql/database"},`数据库管理`),` 和 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../../reference/config"},`服务端配置`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"sample-code"},`高效写入示例`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"scenario"},`场景设计`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`下面的示例程序展示了如何高效写入数据，场景设计如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TDengine 客户端程序从其它数据源不断读入数据，在示例程序中采用生成模拟数据的方式来模拟读取数据源`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`单个连接向 TDengine 写入的速度无法与读数据的速度相匹配，因此客户端程序启动多个线程，每个线程都建立了与 TDengine 的连接，每个线程都有一个独占的固定大小的消息队列`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`客户端程序将接收到的数据根据所属的表名（或子表名）HASH 到不同的线程，即写入该线程所对应的消息队列，以此确保属于某个表（或子表）的数据一定会被一个固定的线程处理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`各个子线程在将所关联的消息队列中的数据读空后或者读取数据量达到一个预定的阈值后将该批数据写入 TDengine，并继续处理后面接收到的数据`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine 高效写入示例场景的线程模型",src:(__webpack_require__(4924)/* ["default"] */ .Z),width:"830",height:"236"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"code"},`示例代码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`这一部分是针对以上场景的示例代码。对于其它场景高效写入原理相同，不过代码需要适当修改。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本示例代码假设源数据属于同一张超级表（meters）的不同子表。程序在开始写入数据之前已经在 test 库创建了这个超级表。对于子表，将根据收到的数据，由应用程序自动创建。如果实际场景是多个超级表，只需修改写任务自动建表的代码。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_Tabs__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`程序清单`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`类名`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`功能说明`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`FastWriteExample`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`主程序`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`ReadTask`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`从模拟源中读取数据，将表名经过 Hash 后得到 Queue 的 Index，写入对应的 Queue`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`WriteTask`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`从 Queue 中获取数据，组成一个 Batch，写入 TDengine`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`MockDataSource`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`模拟生成一定数量 meters 子表的数据`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SQLWriter`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`WriteTask 依赖这个类完成 SQL 拼接、自动建表、 SQL 写入、SQL 长度检查`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`StmtWriter`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`实现参数绑定方式批量写入（暂未完成）`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`DataBaseMonitor`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`统计写入速度，并每隔 10 秒把当前写入速度打印到控制台`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`以下是各类的完整代码和更详细的功能说明。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"FastWriteExample"),"\u4E3B\u7A0B\u5E8F\u8D1F\u8D23\uFF1A",(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`创建消息队列`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`启动写线程`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`启动读线程`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`每隔 10 秒统计一次写入速度`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`主程序默认暴露了 4 个参数，每次启动程序都可调节，用于测试和调优：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`读线程个数。默认为 1。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`写线程个数。默认为 3。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`模拟生成的总表数。默认为 1,000。将会平分给各个读线程。如果总表数较大，建表需要花费较长，开始统计的写入速度可能较慢。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`每批最多写入记录数量。默认为 3,000。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`队列容量（taskQueueCapacity）也是与性能有关的参数，可通过修改程序调节。一般来讲，队列容量越大，入队被阻塞的概率越小，队列的吞吐量越大，但是内存占用也会越大。示例程序默认值已经设置地足够大。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class FastWriteExample {
    final static Logger logger = LoggerFactory.getLogger(FastWriteExample.class);

    final static int taskQueueCapacity = 1000000;
    final static List<BlockingQueue<String>> taskQueues = new ArrayList<>();
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();
    final static DataBaseMonitor databaseMonitor = new DataBaseMonitor();

    public static void stopAll() {
        logger.info("shutting down");
        readTasks.forEach(task -> task.stop());
        writeTasks.forEach(task -> task.stop());
        databaseMonitor.close();
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        int readTaskCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int writeTaskCount = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int tableCount = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        int maxBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 3000;

        logger.info("readTaskCount={}, writeTaskCount={} tableCount={} maxBatchSize={}",
                readTaskCount, writeTaskCount, tableCount, maxBatchSize);

        databaseMonitor.init().prepareDatabase();

        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(taskQueueCapacity);
            taskQueues.add(queue);
            WriteTask task = new WriteTask(queue, maxBatchSize);
            Thread t = new Thread(task);
            t.setName("WriteThread-" + i);
            t.start();
        }

        // create reading tasks and start reading threads
        int tableCountPerTask = tableCount / readTaskCount;
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, taskQueues, tableCountPerTask);
            Thread t = new Thread(task);
            t.setName("ReadThread-" + i);
            t.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::stopAll));

        long lastCount = 0;
        while (true) {
            Thread.sleep(10000);
            long numberOfTable = databaseMonitor.getTableCount();
            long count = databaseMonitor.count();
            logger.info("numberOfTable={} count={} speed={}", numberOfTable, count, (count - lastCount) / 10);
            lastCount = count;
        }
    }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"ReadTask"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`读任务负责从数据源读数据。每个读任务都关联了一个模拟数据源。每个模拟数据源可生成一点数量表的数据。不同的模拟数据源生成不同表的数据。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`读任务采用阻塞的方式写消息队列。也就是说，一旦队列满了，写操作就会阻塞。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class ReadTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadTask.class);
    private final int taskId;
    private final List<BlockingQueue<String>> taskQueues;
    private final int queueCount;
    private final int tableCount;
    private boolean active = true;

    public ReadTask(int readTaskId, List<BlockingQueue<String>> queues, int tableCount) {
        this.taskId = readTaskId;
        this.taskQueues = queues;
        this.queueCount = queues.size();
        this.tableCount = tableCount;
    }

    /**
     * Assign data received to different queues.
     * Here we use the suffix number in table name.
     * You are expected to define your own rule in practice.
     *
     * @param line record received
     * @return which queue to use
     */
    public int getQueueId(String line) {
        String tbName = line.substring(0, line.indexOf(',')); // For example: tb1_101
        String suffixNumber = tbName.split("_")[1];
        return Integer.parseInt(suffixNumber) % this.queueCount;
    }

    @Override
    public void run() {
        logger.info("started");
        Iterator<String> it = new MockDataSource("tb" + this.taskId, tableCount);
        try {
            while (it.hasNext() && active) {
                String line = it.next();
                int queueId = getQueueId(line);
                taskQueues.get(queueId).put(line);
            }
        } catch (Exception e) {
            logger.error("Read Task Error", e);
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/ReadTask.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"WriteTask"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

class WriteTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteTask.class);
    private final int maxBatchSize;

    // the queue from which this writing task get raw data.
    private final BlockingQueue<String> queue;

    // A flag indicate whether to continue.
    private boolean active = true;

    public WriteTask(BlockingQueue<String> taskQueue, int maxBatchSize) {
        this.queue = taskQueue;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void run() {
        logger.info("started");
        String line = null; // data getting from the queue just now.
        SQLWriter writer = new SQLWriter(maxBatchSize);
        try {
            writer.init();
            while (active) {
                line = queue.poll();
                if (line != null) {
                    // parse raw data and buffer the data.
                    writer.processLine(line);
                } else if (writer.hasBufferedValues()) {
                    // write data immediately if no more data in the queue
                    writer.flush();
                } else {
                    // sleep a while to avoid high CPU usage if no more data in the queue and no buffered records, .
                    Thread.sleep(100);
                }
            }
            if (writer.hasBufferedValues()) {
                writer.flush();
            }
        } catch (Exception e) {
            String msg = String.format("line=%s, bufferedCount=%s", line, writer.getBufferedCount());
            logger.error(msg, e);
        } finally {
            writer.close();
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/WriteTask.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"MockDataSource"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import java.util.Iterator;

/**
 * Generate test data
 */
class MockDataSource implements Iterator {
    private String tbNamePrefix;
    private int tableCount;
    private long maxRowsPerTable = 1000000000L;

    // 100 milliseconds between two neighbouring rows.
    long startMs = System.currentTimeMillis() - maxRowsPerTable * 100;
    private int currentRow = 0;
    private int currentTbId = -1;

    // mock values
    String[] location = {"California.LosAngeles", "California.SanDiego", "California.SanJose", "California.Campbell", "California.SanFrancisco"};
    float[] current = {8.8f, 10.7f, 9.9f, 8.9f, 9.4f};
    int[] voltage = {119, 116, 111, 113, 118};
    float[] phase = {0.32f, 0.34f, 0.33f, 0.329f, 0.141f};

    public MockDataSource(String tbNamePrefix, int tableCount) {
        this.tbNamePrefix = tbNamePrefix;
        this.tableCount = tableCount;
    }

    @Override
    public boolean hasNext() {
        currentTbId += 1;
        if (currentTbId == tableCount) {
            currentTbId = 0;
            currentRow += 1;
        }
        return currentRow < maxRowsPerTable;
    }

    @Override
    public String next() {
        long ts = startMs + 100 * currentRow;
        int groupId = currentTbId % 5 == 0 ? currentTbId / 5 : currentTbId / 5 + 1;
        StringBuilder sb = new StringBuilder(tbNamePrefix + "_" + currentTbId + ","); // tbName
        sb.append(ts).append(','); // ts
        sb.append(current[currentRow % 5]).append(','); // current
        sb.append(voltage[currentRow % 5]).append(','); // voltage
        sb.append(phase[currentRow % 5]).append(','); // phase
        sb.append(location[currentRow % 5]).append(','); // location
        sb.append(groupId); // groupID

        return sb.toString();
    }
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/MockDataSource.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"SQLWriter"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`SQLWriter 类封装了拼 SQL 和写数据的逻辑。注意，所有的表都没有提前创建，而是在 catch 到表不存在异常的时候，再以超级表为模板批量建表，然后重新执行 INSERT 语句。对于其它异常，这里简单地记录当时执行的 SQL 语句到日志中，你也可以记录更多线索到日志，已便排查错误和故障恢复。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class encapsulate the logic of writing using SQL.
 * <p>
 * The main interfaces are two methods:
 * <ol>
 *     <li>{@link SQLWriter#processLine}, which receive raw lines from WriteTask and group them by table names.</li>
 *     <li>{@link  SQLWriter#flush}, which assemble INSERT statement and execute it.</li>
 * </ol>
 * <p>
 * There is a technical skill worth mentioning: we create table as needed when "table does not exist" error occur instead of creating table automatically using syntax "INSET INTO tb USING stb".
 * This ensure that checking table existence is a one-time-only operation.
 * </p>
 *
 * </p>
 */
public class SQLWriter {
    final static Logger logger = LoggerFactory.getLogger(SQLWriter.class);

    private Connection conn;
    private Statement stmt;

    /**
     * current number of buffered records
     */
    private int bufferedCount = 0;
    /**
     * Maximum number of buffered records.
     * Flush action will be triggered if bufferedCount reached this value,
     */
    private int maxBatchSize;


    /**
     * Maximum SQL length.
     */
    private int maxSQLLength = 800_000;

    /**
     * Map from table name to column values. For example:
     * "tb001" -> "(1648432611249,2.1,114,0.09) (1648432611250,2.2,135,0.2)"
     */
    private Map<String, String> tbValues = new HashMap<>();

    /**
     * Map from table name to tag values in the same order as creating stable.
     * Used for creating table.
     */
    private Map<String, String> tbTags = new HashMap<>();

    public SQLWriter(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    /**
     * Get Database Connection
     *
     * @return Connection
     * @throws SQLException
     */
    private static Connection getConnection() throws SQLException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        return DriverManager.getConnection(jdbcURL);
    }

    /**
     * Create Connection and Statement
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        conn = getConnection();
        stmt = conn.createStatement();
        stmt.execute("use test");
    }

    /**
     * Convert raw data to SQL fragments, group them by table name and cache them in a HashMap.
     * Trigger writing when number of buffered records reached maxBachSize.
     *
     * @param line raw data get from task queue in format: tbName,ts,current,voltage,phase,location,groupId
     */
    public void processLine(String line) throws SQLException {
        bufferedCount += 1;
        int firstComma = line.indexOf(',');
        String tbName = line.substring(0, firstComma);
        int lastComma = line.lastIndexOf(',');
        int secondLastComma = line.lastIndexOf(',', lastComma - 1);
        String value = "(" + line.substring(firstComma + 1, secondLastComma) + ") ";
        if (tbValues.containsKey(tbName)) {
            tbValues.put(tbName, tbValues.get(tbName) + value);
        } else {
            tbValues.put(tbName, value);
        }
        if (!tbTags.containsKey(tbName)) {
            String location = line.substring(secondLastComma + 1, lastComma);
            String groupId = line.substring(lastComma + 1);
            String tagValues = "('" + location + "'," + groupId + ')';
            tbTags.put(tbName, tagValues);
        }
        if (bufferedCount == maxBatchSize) {
            flush();
        }
    }


    /**
     * Assemble INSERT statement using buffered SQL fragments in Map {@link SQLWriter#tbValues} and execute it.
     * In case of "Table does not exit" exception, create all tables in the sql and retry the sql.
     */
    public void flush() throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (Map.Entry<String, String> entry : tbValues.entrySet()) {
            String tableName = entry.getKey();
            String values = entry.getValue();
            String q = tableName + " values " + values + " ";
            if (sb.length() + q.length() > maxSQLLength) {
                executeSQL(sb.toString());
                logger.warn("increase maxSQLLength or decrease maxBatchSize to gain better performance");
                sb = new StringBuilder("INSERT INTO ");
            }
            sb.append(q);
        }
        executeSQL(sb.toString());
        tbValues.clear();
        bufferedCount = 0;
    }

    private void executeSQL(String sql) throws SQLException {
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // convert to error code defined in taoserror.h
            int errorCode = e.getErrorCode() & 0xffff;
            if (errorCode == 0x2603) {
                // Table does not exist
                createTables();
                executeSQL(sql);
            } else {
                logger.error("Execute SQL: {}", sql);
                throw e;
            }
        } catch (Throwable throwable) {
            logger.error("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    /**
     * Create tables in batch using syntax:
     * <p>
     * CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
     * </p>
     */
    private void createTables() throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        for (String tbName : tbValues.keySet()) {
            String tagValues = tbTags.get(tbName);
            sb.append("IF NOT EXISTS ").append(tbName).append(" USING meters TAGS ").append(tagValues).append(" ");
        }
        String sql = sb.toString();
        try {
            stmt.executeUpdate(sql);
        } catch (Throwable throwable) {
            logger.error("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    public boolean hasBufferedValues() {
        return bufferedCount > 0;
    }

    public int getBufferedCount() {
        return bufferedCount;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/SQLWriter.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"DataBaseMonitor"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example.highvolume;

import java.sql.*;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
public class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;

    public DataBaseMonitor init() throws SQLException {
        if (conn == null) {
            String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
        }
        return this;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    public void prepareDatabase() throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS test");
        stmt.execute("CREATE DATABASE test");
        stmt.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    }

    public long count() throws SQLException {
        try (ResultSet result = stmt.executeQuery("SELECT count(*) from test.meters")) {
            result.next();
            return result.getLong(1);
        }
    }

    public long getTableCount() throws SQLException {
        try (ResultSet result = stmt.executeQuery("select count(*) from information_schema.ins_tables where db_name = 'test';")) {
            result.next();
            return result.getLong(1);
        }
    }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/DataBaseMonitor.java"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`执行步骤`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"\u6267\u884C Java \u793A\u4F8B\u7A0B\u5E8F"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`执行程序前需配置环境变量 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`TDENGINE_JDBC_URL`),`。如果 TDengine Server 部署在本机，且用户名、密码和端口都是默认值，那么可配置：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`本地集成开发环境执行示例程序`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`clone TDengine 仓库`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`git clone git@github.com:taosdata/TDengine.git --depth 1
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`用集成开发环境打开 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`docs/examples/java`),` 目录。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在开发环境中配置环境变量 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`TDENGINE_JDBC_URL`),`。如果已配置了全局的环境变量 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`TDENGINE_JDBC_URL`),` 可跳过这一步。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`运行类 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`com.taos.example.highvolume.FastWriteExample`),`。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`远程服务器上执行示例程序`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`若要在服务器上执行示例程序，可按照下面的步骤操作：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`打包示例代码。在目录 TDengine/docs/examples/java 下执行：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`mvn package
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`远程服务器上创建 examples 目录：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`mkdir -p examples/java
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`复制依赖到服务器指定目录：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`复制依赖包，只用复制一次`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`scp -r .\\target\\lib <user>@<host>:~/examples/java
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`复制本程序的 jar 包，每次更新代码都需要复制`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`scp -r .\\target\\javaexample-1.0.jar <user>@<host>:~/examples/java
`))))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`配置环境变量。
编辑 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`~/.bash_profile`),` 或 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`~/.bashrc`),` 添加如下内容例如：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`以上使用的是本地部署 TDengine Server 时默认的 JDBC URL。你需要根据自己的实际情况更改。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`用 Java 命令启动示例程序，命令模板：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count> <total_table_count> <max_batch_size>
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`结束测试程序。测试程序不会自动结束，在获取到当前配置下稳定的写入速度后，按 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("kbd",null,`CTRL`),` + `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("kbd",null,`C`),` 结束程序。
下面是一次实际运行的日志输出，机器配置 16 核 + 64G + 固态硬盘。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`root@vm85$ java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample 2 12
18:56:35.896 [main] INFO  c.t.e.highvolume.FastWriteExample - readTaskCount=2, writeTaskCount=12 tableCount=1000 maxBatchSize=3000
18:56:36.011 [WriteThread-0] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.015 [WriteThread-0] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.021 [WriteThread-1] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.022 [WriteThread-1] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.031 [WriteThread-2] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.032 [WriteThread-2] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.041 [WriteThread-3] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.042 [WriteThread-3] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.093 [WriteThread-4] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.094 [WriteThread-4] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.099 [WriteThread-5] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.100 [WriteThread-5] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.100 [WriteThread-6] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.101 [WriteThread-6] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.103 [WriteThread-7] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.104 [WriteThread-7] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.105 [WriteThread-8] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.107 [WriteThread-8] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.108 [WriteThread-9] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.109 [WriteThread-9] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.156 [WriteThread-10] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.157 [WriteThread-11] INFO  c.taos.example.highvolume.WriteTask - started
18:56:36.158 [WriteThread-10] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:36.158 [ReadThread-0] INFO  com.taos.example.highvolume.ReadTask - started
18:56:36.158 [ReadThread-1] INFO  com.taos.example.highvolume.ReadTask - started
18:56:36.158 [WriteThread-11] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
18:56:46.369 [main] INFO  c.t.e.highvolume.FastWriteExample - count=18554448 speed=1855444
18:56:56.946 [main] INFO  c.t.e.highvolume.FastWriteExample - count=39059660 speed=2050521
18:57:07.322 [main] INFO  c.t.e.highvolume.FastWriteExample - count=59403604 speed=2034394
18:57:18.032 [main] INFO  c.t.e.highvolume.FastWriteExample - count=80262938 speed=2085933
18:57:28.432 [main] INFO  c.t.e.highvolume.FastWriteExample - count=101139906 speed=2087696
18:57:38.921 [main] INFO  c.t.e.highvolume.FastWriteExample - count=121807202 speed=2066729
18:57:49.375 [main] INFO  c.t.e.highvolume.FastWriteExample - count=142952417 speed=2114521
18:58:00.689 [main] INFO  c.t.e.highvolume.FastWriteExample - count=163650306 speed=2069788
18:58:11.646 [main] INFO  c.t.e.highvolume.FastWriteExample - count=185019808 speed=2136950
`)))))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_theme_TabItem__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`程序清单`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Python 示例程序中采用了多进程的架构，并使用了跨进程的消息队列。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("table",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("thead",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"thead"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`函数或类`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("th",{parentName:"tr","align":null},`功能说明`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tbody",{parentName:"table"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`main 函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`程序入口， 创建各个子进程和消息队列`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`run_monitor_process 函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`创建数据库，超级表，统计写入速度并定时打印到控制台`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`run_read_task 函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`读进程主要逻辑，负责从其它数据系统读数据，并分发数据到为之分配的队列`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`MockDataSource 类`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`模拟数据源, 实现迭代器接口，每次批量返回每张表的接下来 1,000 条数据`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`run_write_task 函数`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`写进程主要逻辑。每次从队列中取出尽量多的数据，并批量写入`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SQLWriter 类`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`SQL 写入和自动建表`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("tr",{parentName:"tbody"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`StmtWriter 类`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("td",{parentName:"tr","align":null},`实现参数绑定方式批量写入（暂未完成）`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"main \u51FD\u6570"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`main 函数负责创建消息队列和启动子进程，子进程有 3 类：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`1 个监控进程，负责数据库初始化和统计写入速度`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`n 个读进程，负责从其它数据系统读数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`m 个写进程，负责写数据库`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`main 函数可以接收 5 个启动参数，依次是：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`读任务（进程）数, 默认为 1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`写任务（进程）数, 默认为 1`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`模拟生成的总表数，默认为 1,000`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`队列大小（单位字节），默认为 1,000,000`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`每批最多写入记录数量， 默认为 3,000`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def main(infinity):
    set_global_config()
    logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, "
                 f"TABLE_COUNT={TABLE_COUNT}, QUEUE_SIZE={QUEUE_SIZE}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

    conn = get_connection()
    conn.execute("DROP DATABASE IF EXISTS test")
    conn.execute("CREATE DATABASE IF NOT EXISTS test")
    conn.execute("CREATE STABLE IF NOT EXISTS test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")
    conn.close()

    done_queue = Queue()
    monitor_process = Process(target=run_monitor_process, args=(done_queue,))
    monitor_process.start()
    logging.debug(f"monitor task started with pid {monitor_process.pid}")

    task_queues: List[Queue] = []
    write_processes = []
    read_processes = []

    # create task queues
    for i in range(WRITE_TASK_COUNT):
        queue = Queue()
        task_queues.append(queue)

    # create write processes
    for i in range(WRITE_TASK_COUNT):
        p = Process(target=run_write_task, args=(i, task_queues[i], done_queue))
        p.start()
        logging.debug(f"WriteTask-{i} started with pid {p.pid}")
        write_processes.append(p)

    # create read processes
    for i in range(READ_TASK_COUNT):
        queues = assign_queues(i, task_queues)
        p = Process(target=run_read_task, args=(i, queues, infinity))
        p.start()
        logging.debug(f"ReadTask-{i} started with pid {p.pid}")
        read_processes.append(p)

    try:
        monitor_process.join()
        for p in read_processes:
            p.join()
        for p in write_processes:
            p.join()
        time.sleep(1)
        return
    except KeyboardInterrupt:
        monitor_process.terminate()
        [p.terminate() for p in read_processes]
        [p.terminate() for p in write_processes]
        [q.close() for q in task_queues]


def assign_queues(read_task_id, task_queues):
    """
    Compute target queues for a specific read task.
    """
    ratio = WRITE_TASK_COUNT / READ_TASK_COUNT
    from_index = math.floor(read_task_id * ratio)
    end_index = math.ceil((read_task_id + 1) * ratio)
    return task_queues[from_index:end_index]


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    main(False)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"run_monitor_process"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`监控进程负责初始化数据库，并监控当前的写入速度。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def run_monitor_process(done_queue: Queue):
    log = logging.getLogger("DataBaseMonitor")
    conn = None
    try:
        conn = get_connection()

        def get_count():
            res = conn.query("SELECT count(*) FROM test.meters")
            rows = res.fetch_all()
            return rows[0][0] if rows else 0

        last_count = 0
        while True:
            try:
                done = done_queue.get_nowait()
                if done == _DONE_MESSAGE:
                    break
            except Empty:
                pass
            time.sleep(10)
            count = get_count()
            log.info(f"count={count} speed={(count - last_count) / 10}")
            last_count = count
    finally:
        conn.close()


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"run_read_task \u51FD\u6570"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`读进程，负责从其它数据系统读数据，并分发数据到为之分配的队列。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`
def run_read_task(task_id: int, task_queues: List[Queue], infinity):
    table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
    data_source = MockDataSource(f"tb{task_id}", table_count_per_task, infinity)
    try:
        for batch in data_source:
            if isinstance(batch, tuple):
                batch = [batch]
            for table_id, rows in batch:
                # hash data to different queue
                i = table_id % len(task_queues)
                # block putting forever when the queue is full
                for row in rows:
                    task_queues[i].put(row)
        if not infinity:
            for queue in task_queues:
                queue.put(_DONE_MESSAGE)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info('read task over')


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"MockDataSource"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`以下是模拟数据源的实现，我们假设数据源生成的每一条数据都带有目标表名信息。实际中你可能需要一定的规则确定目标表名。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import time


class MockDataSource:
    samples = [
        "8.8,119,0.32,California.LosAngeles,0",
        "10.7,116,0.34,California.SanDiego,1",
        "9.9,111,0.33,California.SanJose,2",
        "8.9,113,0.329,California.Campbell,3",
        "9.4,118,0.141,California.SanFrancisco,4"
    ]

    def __init__(self, tb_name_prefix, table_count, infinity=True):
        self.table_name_prefix = tb_name_prefix + "_"
        self.table_count = table_count
        self.max_rows = 10000000
        self.current_ts = round(time.time() * 1000) - self.max_rows * 100
        # [(tableId, tableName, values),]
        self.data = self._init_data()
        self.infinity = infinity

    def _init_data(self):
        lines = self.samples * (self.table_count // 5 + 1)
        data = []
        for i in range(self.table_count):
            table_name = self.table_name_prefix + str(i)
            data.append((i, table_name, lines[i]))  # tableId, row
        return data

    def __iter__(self):
        self.row = 0
        if not self.infinity:
            return iter(self._iter_data())
        else:
            return self

    def __next__(self):
        """
        next 1000 rows for each table.
        return: {tableId:[row,...]}
        """
        return self._iter_data()

    def _iter_data(self):
        ts = []
        for _ in range(1000):
            self.current_ts += 100
            ts.append(str(self.current_ts))
        # add timestamp to each row
        # [(tableId, ["tableName,ts,current,voltage,phase,location,groupId"])]
        result = []
        for table_id, table_name, values in self.data:
            rows = [table_name + ',' + t + ',' + values for t in ts]
            result.append((table_id, rows))
        return result


if __name__ == '__main__':
    datasource = MockDataSource('t', 10, False)
    for data in datasource:
        print(data)

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/mockdatasource.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"run_write_task \u51FD\u6570"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`写进程每次从队列中取出尽量多的数据，并批量写入。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`def run_write_task(task_id: int, queue: Queue, done_queue: Queue):
    from sql_writer import SQLWriter
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = SQLWriter(get_connection)
    lines = None
    try:
        while True:
            over = False
            lines = []
            for _ in range(MAX_BATCH_SIZE):
                try:
                    line = queue.get_nowait()
                    if line == _DONE_MESSAGE:
                        over = True
                        break
                    if line:
                        lines.append(line)
                except Empty:
                    time.sleep(0.1)
            if len(lines) > 0:
                writer.process_lines(lines)
            if over:
                done_queue.put(_DONE_MESSAGE)
                break
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        log.debug(f"lines={lines}")
        raise e
    finally:
        writer.close()
        log.debug('write task over')


`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`SQLWriter 类封装了拼 SQL 和写数据的逻辑。所有的表都没有提前创建，而是在发生表不存在错误的时候，再以超级表为模板批量建表，然后重新执行 INSERT 语句。对于其它错误会记录当时执行的 SQL， 以便排查错误和故障恢复。这个类也对 SQL 是否超过最大长度限制做了检查，根据 TDengine 3.0 的限制由输入参数 maxSQLLength 传入了支持的最大 SQL 长度，即 1,048,576 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"SQLWriter"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-python"},`import logging
import taos


class SQLWriter:
    log = logging.getLogger("SQLWriter")

    def __init__(self, get_connection_func):
        self._tb_values = {}
        self._tb_tags = {}
        self._conn = get_connection_func()
        self._max_sql_length = self.get_max_sql_length()
        self._conn.execute("create database if not exists test")
        self._conn.execute("USE test")

    def get_max_sql_length(self):
        rows = self._conn.query("SHOW variables").fetch_all()
        for r in rows:
            name = r[0]
            if name == "maxSQLLength":
                return int(r[1])
        return 1024 * 1024

    def process_lines(self, lines: [str]):
        """
        :param lines: [[tbName,ts,current,voltage,phase,location,groupId]]
        """
        for line in lines:
            ps = line.split(",")
            table_name = ps[0]
            value = '(' + ",".join(ps[1:-2]) + ') '
            if table_name in self._tb_values:
                self._tb_values[table_name] += value
            else:
                self._tb_values[table_name] = value

            if table_name not in self._tb_tags:
                location = ps[-2]
                group_id = ps[-1]
                tag_value = f"('{location}',{group_id})"
                self._tb_tags[table_name] = tag_value
        self.flush()

    def flush(self):
        """
        Assemble INSERT statement and execute it.
        When the sql length grows close to MAX_SQL_LENGTH, the sql will be executed immediately, and a new INSERT statement will be created.
        In case of "Table does not exit" exception, tables in the sql will be created and the sql will be re-executed.
        """
        sql = "INSERT INTO "
        sql_len = len(sql)
        buf = []
        for tb_name, values in self._tb_values.items():
            q = tb_name + " VALUES " + values
            if sql_len + len(q) >= self._max_sql_length:
                sql += " ".join(buf)
                self.execute_sql(sql)
                sql = "INSERT INTO "
                sql_len = len(sql)
                buf = []
            buf.append(q)
            sql_len += len(q)
        sql += " ".join(buf)
        self.create_tables()
        self.execute_sql(sql)
        self._tb_values.clear()

    def execute_sql(self, sql):
        try:
            self._conn.execute(sql)
        except taos.Error as e:
            error_code = e.errno & 0xffff
            # Table does not exit
            if error_code == 9731:
                self.create_tables()
            else:
                self.log.error("Execute SQL: %s", sql)
                raise e
        except BaseException as baseException:
            self.log.error("Execute SQL: %s", sql)
            raise baseException

    def create_tables(self):
        sql = "CREATE TABLE "
        for tb in self._tb_values.keys():
            tag_values = self._tb_tags[tb]
            sql += "IF NOT EXISTS " + tb + " USING meters TAGS " + tag_values + " "
        try:
            self._conn.execute(sql)
        except BaseException as e:
            self.log.error("Execute SQL: %s", sql)
            raise e

    def close(self):
        if self._conn:
            self._conn.close()


if __name__ == '__main__':
    def get_connection_func():
        conn = taos.connect()
        return conn


    writer = SQLWriter(get_connection_func=get_connection_func)
    writer.execute_sql(
        "create stable if not exists meters (ts timestamp, current float, voltage int, phase float) "
        "tags (location binary(64), groupId int)")
    writer.execute_sql(
        "INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) "
        "VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32)")

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/sql_writer.py"},`查看源码`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("strong",{parentName:"p"},`执行步骤`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("details",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("summary",null,"\u6267\u884C Python \u793A\u4F8B\u7A0B\u5E8F"),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`前提条件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`已安装 TDengine 客户端驱动`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`已安装 Python3， 推荐版本 >= 3.8`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`已安装 taospy`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`安装 faster-fifo 代替 python 内置的 multiprocessing.Queue`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`pip3 install faster-fifo
`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`点击上面的“查看源码”链接复制 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`fast_write_example.py`),` 、 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sql_writer.py`),` 和 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`mockdatasource.py`),` 三个文件。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`执行示例程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`python3  fast_write_example.py <READ_TASK_COUNT> <WRITE_TASK_COUNT> <TABLE_COUNT> <QUEUE_SIZE> <MAX_BATCH_SIZE>
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`下面是一次实际运行的输出, 机器配置 16 核 + 64G + 固态硬盘。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`root@vm85$ python3 fast_write_example.py  8 8
2022-07-14 19:13:45,869 [root] - READ_TASK_COUNT=8, WRITE_TASK_COUNT=8, TABLE_COUNT=1000, QUEUE_SIZE=1000000, MAX_BATCH_SIZE=3000
2022-07-14 19:13:48,882 [root] - WriteTask-0 started with pid 718347
2022-07-14 19:13:48,883 [root] - WriteTask-1 started with pid 718348
2022-07-14 19:13:48,884 [root] - WriteTask-2 started with pid 718349
2022-07-14 19:13:48,884 [root] - WriteTask-3 started with pid 718350
2022-07-14 19:13:48,885 [root] - WriteTask-4 started with pid 718351
2022-07-14 19:13:48,885 [root] - WriteTask-5 started with pid 718352
2022-07-14 19:13:48,886 [root] - WriteTask-6 started with pid 718353
2022-07-14 19:13:48,886 [root] - WriteTask-7 started with pid 718354
2022-07-14 19:13:48,887 [root] - ReadTask-0 started with pid 718355
2022-07-14 19:13:48,888 [root] - ReadTask-1 started with pid 718356
2022-07-14 19:13:48,889 [root] - ReadTask-2 started with pid 718357
2022-07-14 19:13:48,889 [root] - ReadTask-3 started with pid 718358
2022-07-14 19:13:48,890 [root] - ReadTask-4 started with pid 718359
2022-07-14 19:13:48,891 [root] - ReadTask-5 started with pid 718361
2022-07-14 19:13:48,892 [root] - ReadTask-6 started with pid 718364
2022-07-14 19:13:48,893 [root] - ReadTask-7 started with pid 718365
2022-07-14 19:13:56,042 [DataBaseMonitor] - count=6676310 speed=667631.0
2022-07-14 19:14:06,196 [DataBaseMonitor] - count=20004310 speed=1332800.0
2022-07-14 19:14:16,366 [DataBaseMonitor] - count=32290310 speed=1228600.0
2022-07-14 19:14:26,527 [DataBaseMonitor] - count=44438310 speed=1214800.0
2022-07-14 19:14:36,673 [DataBaseMonitor] - count=56608310 speed=1217000.0
2022-07-14 19:14:46,834 [DataBaseMonitor] - count=68757310 speed=1214900.0
2022-07-14 19:14:57,280 [DataBaseMonitor] - count=80992310 speed=1223500.0
2022-07-14 19:15:07,689 [DataBaseMonitor] - count=93805310 speed=1281300.0
2022-07-14 19:15:18,020 [DataBaseMonitor] - count=106111310 speed=1230600.0
2022-07-14 19:15:28,356 [DataBaseMonitor] - count=118394310 speed=1228300.0
2022-07-14 19:15:38,690 [DataBaseMonitor] - count=130742310 speed=1234800.0
2022-07-14 19:15:49,000 [DataBaseMonitor] - count=143051310 speed=1230900.0
2022-07-14 19:15:59,323 [DataBaseMonitor] - count=155276310 speed=1222500.0
2022-07-14 19:16:09,649 [DataBaseMonitor] - count=167603310 speed=1232700.0
2022-07-14 19:16:19,995 [DataBaseMonitor] - count=179976310 speed=1237300.0
`))))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`使用 Python 连接器多进程连接 TDengine 的时候，有一个限制：不能在父进程中建立连接，所有连接只能在子进程中创建。
如果在父进程中创建连接，子进程再创建连接就会一直阻塞。这是个已知问题。`)))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 4924:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ })

}]);