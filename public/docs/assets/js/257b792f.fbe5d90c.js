"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[1549],{

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

/***/ 8958:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "ws://";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    taos.exec_many([
        "DROP DATABASE IF EXISTS power",
        "CREATE DATABASE power",
        "USE power",
        "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
    ]).await?;

    let inserted = taos.exec("INSERT INTO 
    power.d1001 USING power.meters TAGS('California.SanFrancisco', 2)
     VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) 
     ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3)
     VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) 
     VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) 
     VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)").await?;

    assert_eq!(inserted, 8);
    Ok(())
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/restexample/examples/insert_example.rs"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 9900:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    taos.create_database("power").await?;
    taos.use_database("power").await?;
    taos.exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)").await?;

    let mut stmt = Stmt::init(&taos)?;
    stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")?;
    // bind table name and tags
    stmt.set_tbname_tags(
        "d1001",
        &[
            Value::VarChar("California.SanFransico".into()),
            Value::Int(2),
        ],
    )?;
    // bind values.
    let values = vec![
        ColumnView::from_millis_timestamp(vec![1648432611249]),
        ColumnView::from_floats(vec![10.3]),
        ColumnView::from_ints(vec![219]),
        ColumnView::from_floats(vec![0.31]),
    ];
    stmt.bind(&values)?;
    // bind one more row
    let values2 = vec![
        ColumnView::from_millis_timestamp(vec![1648432611749]),
        ColumnView::from_floats(vec![12.6]),
        ColumnView::from_ints(vec![218]),
        ColumnView::from_floats(vec![0.33]),
    ];
    stmt.bind(&values2)?;

    stmt.add_batch()?;

    // execute.
    let rows = stmt.execute()?;
    assert_eq!(rows, 2);
    Ok(())
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/stmt_example.rs"},`查看源码`)));};MDXContent.isMDXComponent=true;

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

/***/ 7545:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _26_rust_contentTitle),
  "default": () => (/* binding */ _26_rust_MDXContent),
  "frontMatter": () => (/* binding */ _26_rust_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _26_rust_toc)
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
// EXTERNAL MODULE: ./docs/08-connector/_preparation.mdx
var _preparation = __webpack_require__(8462);
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_rust_sql.mdx
var _rust_sql = __webpack_require__(8958);
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_rust_stmt.mdx
var _rust_stmt = __webpack_require__(9900);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_rust_schemaless.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use taos_query::common::SchemalessPrecision;
use taos_query::common::SchemalessProtocol;
use taos_query::common::SmlDataBuilder;

use crate::AsyncQueryable;
use crate::AsyncTBuilder;
use crate::TaosBuilder;

async fn put_line() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "taos=trace");
    std::env::set_var("RUST_LOG", "taos=debug");
    pretty_env_logger::init();

    let dsn =
        std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
    log::debug!("dsn: {:?}", &dsn);

    let client = TaosBuilder::from_dsn(dsn)?.build().await?;

    let db = "demo_schemaless_ws";

    client.exec(format!("drop database if exists {db}")).await?;

    client
        .exec(format!("create database if not exists {db}"))
        .await?;

    // should specify database before insert
    client.exec(format!("use {db}")).await?;

    let data = [
        "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
        "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
    ]
    .map(String::from)
    .to_vec();

    // demo with all fields
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .ttl(1000)
        .req_id(100u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .req_id(101u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default ttl and req_id 
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data.clone())
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    // demo with default precision
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .data(data)
        .req_id(103u64)
        .build()?;
    assert_eq!(client.put(&sml_data).await?, ());

    client.exec(format!("drop database if exists {db}")).await?;

    Ok(())
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/schemaless_insert_line.rs"},`查看源码`)));};MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_rust.mdx
var _rust = __webpack_require__(5682);
;// CONCATENATED MODULE: ./docs/08-connector/26-rust.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _26_rust_frontMatter={toc_max_heading_level:4,sidebar_label:'Rust',title:'TDengine Rust Connector'};const _26_rust_contentTitle=undefined;const metadata={"unversionedId":"connector/rust","id":"connector/rust","title":"TDengine Rust Connector","description":"Crates.io Crates.io docs.rs","source":"@site/docs/08-connector/26-rust.mdx","sourceDirName":"08-connector","slug":"/connector/rust","permalink":"/docs/connector/rust","draft":false,"tags":[],"version":"current","sidebarPosition":26,"frontMatter":{"toc_max_heading_level":4,"sidebar_label":"Rust","title":"TDengine Rust Connector"},"sidebar":"defaultSidebar","previous":{"title":"Java","permalink":"/docs/connector/java"},"next":{"title":"Python","permalink":"/docs/connector/python"}};const assets={};const _26_rust_toc=[{value:'支持的平台',id:'支持的平台',level:2},{value:'版本历史',id:'版本历史',level:2},{value:'处理错误',id:'处理错误',level:2},{value:'TDengine DataType 和 Rust DataType',id:'tdengine-datatype-和-rust-datatype',level:2},{value:'安装步骤',id:'安装步骤',level:2},{value:'安装前准备',id:'安装前准备',level:3},{value:'安装连接器',id:'安装连接器',level:3},{value:'建立连接',id:'建立连接',level:2},{value:'使用示例',id:'使用示例',level:2},{value:'创建数据库和表',id:'创建数据库和表',level:3},{value:'插入数据',id:'插入数据',level:3},{value:'查询数据',id:'查询数据',level:3},{value:'执行带有 req_id 的 SQL',id:'执行带有-req_id-的-sql',level:3},{value:'通过参数绑定写入数据',id:'通过参数绑定写入数据',level:3},{value:'无模式写入',id:'无模式写入',level:3},{value:'执行带有 req_id 的无模式写入',id:'执行带有-req_id-的无模式写入',level:3},{value:'数据订阅',id:'数据订阅',level:3},{value:'创建 Topic',id:'创建-topic',level:4},{value:'创建 Consumer',id:'创建-consumer',level:4},{value:'订阅消费数据',id:'订阅消费数据',level:4},{value:'指定订阅 Offset',id:'指定订阅-offset',level:4},{value:'关闭订阅',id:'关闭订阅',level:4},{value:'完整示例',id:'完整示例',level:4},{value:'与连接池使用',id:'与连接池使用',level:3},{value:'更多示例程序',id:'更多示例程序',level:3},{value:'常见问题',id:'常见问题',level:2},{value:'API 参考',id:'api-参考',level:2}];const _26_rust_layoutProps={toc: _26_rust_toc};const _26_rust_MDXLayout="wrapper";function _26_rust_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_26_rust_MDXLayout,(0,esm_extends/* default */.Z)({},_26_rust_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://crates.io/crates/taos"},(0,esm/* mdx */.kt)("img",{parentName:"a","src":"https://img.shields.io/crates/v/taos","alt":"Crates.io"})),` `,(0,esm/* mdx */.kt)("img",{parentName:"p","src":"https://img.shields.io/crates/d/taos","alt":"Crates.io"}),` `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos"},(0,esm/* mdx */.kt)("img",{parentName:"a","src":"https://img.shields.io/docsrs/taos","alt":"docs.rs"}))),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` 是 TDengine 的官方 Rust 语言连接器。Rust 开发人员可以通过它开发存取 TDengine 数据库的应用软件。`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` 提供两种建立连接的方式。一种是`,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`原生连接`),`，它通过 TDengine 客户端驱动程序（taosc）连接 TDengine 运行实例。另外一种是 `,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Websocket 连接`),`，它通过 taosAdapter 的 Websocket 接口连接 TDengine 运行实例。你可以通过不同的 “特性（即 Cargo 关键字 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`features`),`）” 来指定使用哪种连接器（默认同时支持）。Websocket 连接支持任何平台，原生连接支持所有 TDengine 客户端能运行的平台。`),(0,esm/* mdx */.kt)("p",null,`该 Rust 连接器的源码托管在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`GitHub`),`。`),(0,esm/* mdx */.kt)("h2",{"id":"支持的平台"},`支持的平台`),(0,esm/* mdx */.kt)("p",null,`原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
Websocket 连接支持所有能运行 Rust 的平台。`),(0,esm/* mdx */.kt)("h2",{"id":"版本历史"},`版本历史`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`Rust 连接器版本`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`TDengine 版本`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`主要功能`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.9.2`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.7.0 or later`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`STMT：ws 下获取 tag_fields、col_fields。`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.8.12`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.5.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`消息订阅：获取消费进度及按照指定进度开始消费。`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.8.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.4.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`支持无模式写入。`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.7.6`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.3.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`支持在请求中使用 req_id。`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.6.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.0.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`基础功能。`)))),(0,esm/* mdx */.kt)("p",null,`Rust 连接器仍然在快速开发中，1.0 之前无法保证其向后兼容。建议使用 3.0 版本以上的 TDengine，以避免已知问题。`),(0,esm/* mdx */.kt)("h2",{"id":"处理错误"},`处理错误`),(0,esm/* mdx */.kt)("p",null,`在报错后，可以获取到错误的具体信息：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`match conn.exec(sql) {
    Ok(_) => {
        Ok(())
    }
    Err(e) => {
        eprintln!("ERROR: {:?}", e);
        Err(e)
    }
}
`)),(0,esm/* mdx */.kt)("h2",{"id":"tdengine-datatype-和-rust-datatype"},`TDengine DataType 和 Rust DataType`),(0,esm/* mdx */.kt)("p",null,`TDengine 目前支持时间戳、数字、字符、布尔类型，与 Rust 对应类型转换如下：`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`TDengine DataType`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`Rust DataType`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`TIMESTAMP`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`Timestamp`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`INT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i32`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BIGINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i64`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`FLOAT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`f32`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`DOUBLE`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`f64`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`SMALLINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i16`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`TINYINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i8`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BOOL`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`bool`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BINARY`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`Vec<u8`,`>`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`NCHAR`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`String`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`JSON`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`serde_json::Value`)))),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`注意`),`：JSON 类型仅在 tag 中支持。`),(0,esm/* mdx */.kt)("h2",{"id":"安装步骤"},`安装步骤`),(0,esm/* mdx */.kt)("h3",{"id":"安装前准备"},`安装前准备`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`安装 Rust 开发工具链`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`如果使用原生连接，请安装 TDengine 客户端驱动，具体步骤请参考`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"../#%E5%AE%89%E8%A3%85%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%A9%B1%E5%8A%A8"},`安装客户端驱动`))),(0,esm/* mdx */.kt)("h3",{"id":"安装连接器"},`安装连接器`),(0,esm/* mdx */.kt)("p",null,`根据选择的连接方式，按照如下说明在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://rust-lang.org"},`Rust`),` 项目中添加 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`taos`),` 依赖：`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"default",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"default",label:"\u540C\u65F6\u652F\u6301",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`在 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Cargo.toml`),` 文件中添加 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`taos`),`：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
# use default feature
taos = "*"
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"rest",label:"\u4EC5 Websocket",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`在 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Cargo.toml`),` 文件中添加 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`taos`),`，并启用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ws`),` 特性。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
taos = { version = "*", default-features = false, features = ["ws"] }
`)),(0,esm/* mdx */.kt)("p",null,`当仅启用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ws`),` 特性时，可同时指定 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`r2d2`),` 使得在同步（blocking/sync）模式下使用 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://crates.io/crates/r2d2"},`r2d2`),` 作为连接池：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
taos = { version = "*", default-features = false, features = ["r2d2", "ws"] }
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"native",label:"\u4EC5\u539F\u751F\u8FDE\u63A5",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`在 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Cargo.toml`),` 文件中添加 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`taos`),`，并启用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`native`),` 特性：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
taos = { version = "*", default-features = false, features = ["native"] }
`)))),(0,esm/* mdx */.kt)("h2",{"id":"建立连接"},`建立连接`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.TaosBuilder.html"},`TaosBuilder`),` 通过 DSN 连接描述字符串创建一个连接构造器。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let builder = TaosBuilder::from_dsn("taos://")?;
`)),(0,esm/* mdx */.kt)("p",null,`现在您可以使用该对象创建连接：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let conn = builder.build()?;
`)),(0,esm/* mdx */.kt)("p",null,`连接对象可以创建多个：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let conn1 = builder.build()?;
let conn2 = builder.build()?;
`)),(0,esm/* mdx */.kt)("p",null,`DSN 描述字符串基本结构如下：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-text"},`<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
`)),(0,esm/* mdx */.kt)("p",null,`各部分意义见下表：`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`driver`),`: 必须指定驱动名以便连接器选择何种方式创建连接，支持如下驱动名：`,(0,esm/* mdx */.kt)("ul",{parentName:"li"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`taos`),`: 表名使用 TDengine 连接器驱动。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`tmq`),`: 使用 TMQ 订阅数据。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`http/ws`),`: 使用 Websocket 创建连接。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`https/wss`),`: 在 Websocket 连接方式下显示启用 SSL/TLS 连接。`))),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`protocol`),`: 显示指定以何种方式建立连接，例如：`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`taos+ws://localhost:6041`),` 指定以 Websocket 方式建立连接。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`username/password`),`: 用于创建连接的用户名及密码。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`host/port`),`: 指定创建连接的服务器及端口，当不指定服务器地址及端口时（`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`taos://`),`），原生连接默认为 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`localhost:6030`),`，Websocket 连接默认为 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`localhost:6041`),` 。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`database`),`: 指定默认连接的数据库名，可选参数。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`params`),`：其他可选参数。`)),(0,esm/* mdx */.kt)("p",null,`一个完整的 DSN 描述字符串示例如下：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-text"},`taos+ws://localhost:6041/test
`)),(0,esm/* mdx */.kt)("p",null,`表示使用 Websocket（`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ws`),`）方式通过 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`6041`),` 端口连接服务器 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`localhost`),`，并指定默认数据库为 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`test`),`。`),(0,esm/* mdx */.kt)("p",null,`这使得用户可以通过 DSN 指定连接方式：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

// use native protocol.
let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
let conn1 = builder.build();

//  use websocket protocol.
let builder2 = TaosBuilder::from_dsn("taos+ws://localhost:6041")?;
let conn2 = builder2.build();
`)),(0,esm/* mdx */.kt)("p",null,`建立连接后，您可以进行相关数据库操作：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`async fn demo(taos: &Taos, db: &str) -> Result<(), Error> {
    // prepare database
    taos.exec_many([
        format!("DROP DATABASE IF EXISTS \`{db}\`"),
        format!("CREATE DATABASE \`{db}\`"),
        format!("USE \`{db}\`"),
    ])
    .await?;

    let inserted = taos.exec_many([
        // create super table
        "CREATE TABLE \`meters\` (\`ts\` TIMESTAMP, \`current\` FLOAT, \`voltage\` INT, \`phase\` FLOAT) \\
         TAGS (\`groupid\` INT, \`location\` BINARY(24))",
        // create child table
        "CREATE TABLE \`d0\` USING \`meters\` TAGS(0, 'California.LosAngles')",
        // insert into child table
        "INSERT INTO \`d0\` values(now - 10s, 10, 116, 0.32)",
        // insert with NULL values
        "INSERT INTO \`d0\` values(now - 8s, NULL, NULL, NULL)",
        // insert and automatically create table with tags if not exists
        "INSERT INTO \`d1\` USING \`meters\` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119, 0.33)",
        // insert many records in a single sql
        "INSERT INTO \`d1\` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
    ]).await?;

    assert_eq!(inserted, 6);
    let mut result = taos.query("select * from \`meters\`").await?;

    for field in result.fields() {
        println!("got field: {}", field.name());
    }

    let values = result.
}
`)),(0,esm/* mdx */.kt)("p",null,`查询数据可以通过两种方式：使用内建类型或 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://serde.rs"},`serde`),` 序列化框架。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`    // Query option 1, use rows stream.
    let mut rows = result.rows();
    while let Some(row) = rows.try_next().await? {
        for (name, value) in row {
            println!("got value of {}: {}", name, value);
        }
    }

    // Query options 2, use deserialization with serde.
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        // deserialize timestamp to chrono::DateTime<Local>
        ts: DateTime<Local>,
        // float to f32
        current: Option<f32>,
        // int to i32
        voltage: Option<i32>,
        phase: Option<f32>,
        groupid: i32,
        // binary/varchar to String
        location: String,
    }

    let records: Vec<Record> = taos
        .query("select * from \`meters\`")
        .await?
        .deserialize()
        .try_collect()
        .await?;

    dbg!(records);
    Ok(())
`)),(0,esm/* mdx */.kt)("h2",{"id":"使用示例"},`使用示例`),(0,esm/* mdx */.kt)("h3",{"id":"创建数据库和表"},`创建数据库和表`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build()?;

    let db = "query";

    // create database
    taos.exec_many([
        format!("DROP DATABASE IF EXISTS \`{db}\`"),
        format!("CREATE DATABASE \`{db}\`"),
        format!("USE \`{db}\`"),
    ])
    .await?;

    // create table
    taos.exec_many([
        // create super table
        "CREATE TABLE \`meters\` (\`ts\` TIMESTAMP, \`current\` FLOAT, \`voltage\` INT, \`phase\` FLOAT) \\
         TAGS (\`groupid\` INT, \`location\` BINARY(16))",
        // create child table
        "CREATE TABLE \`d0\` USING \`meters\` TAGS(0, 'Los Angles')",
    ]).await?;
}
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},(0,esm/* mdx */.kt)("strong",{parentName:"p"},`注意`),`：如果不使用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`use db`),` 指定数据库，则后续对表的操作都需要增加数据库名称作为前缀，如 db.tb。`)),(0,esm/* mdx */.kt)("h3",{"id":"插入数据"},`插入数据`),(0,esm/* mdx */.kt)(_rust_sql/* default */.ZP,{mdxType:"RustInsert"}),(0,esm/* mdx */.kt)("h3",{"id":"查询数据"},`查询数据`),(0,esm/* mdx */.kt)(_rust/* default */.ZP,{mdxType:"RustQuery"}),(0,esm/* mdx */.kt)("h3",{"id":"执行带有-req_id-的-sql"},`执行带有 req_id 的 SQL`),(0,esm/* mdx */.kt)("p",null,`此 req_id 可用于请求链路追踪。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let rs = taos.query_with_req_id("select * from stable where tag1 is null", 1)?;
`)),(0,esm/* mdx */.kt)("h3",{"id":"通过参数绑定写入数据"},`通过参数绑定写入数据`),(0,esm/* mdx */.kt)("p",null,`TDengine 的 Rust 连接器实现了参数绑定方式对数据写入（INSERT）场景的支持。采用这种方式写入数据时，能避免 SQL 语法解析的资源消耗，从而在很多情况下显著提升写入性能。`),(0,esm/* mdx */.kt)("p",null,`参数绑定接口详见`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"#stmt-api"},`API参考`)),(0,esm/* mdx */.kt)(_rust_stmt/* default */.ZP,{mdxType:"RustBind"}),(0,esm/* mdx */.kt)("h3",{"id":"无模式写入"},`无模式写入`),(0,esm/* mdx */.kt)("p",null,`TDengine 支持无模式写入功能。无模式写入兼容 InfluxDB 的 行协议（Line Protocol）、OpenTSDB 的 telnet 行协议和 OpenTSDB 的 JSON 格式协议。详情请参见`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../reference/schemaless/"},`无模式写入`),`。`),(0,esm/* mdx */.kt)(MDXContent,{mdxType:"RustSml"}),(0,esm/* mdx */.kt)("h3",{"id":"执行带有-req_id-的无模式写入"},`执行带有 req_id 的无模式写入`),(0,esm/* mdx */.kt)("p",null,`此 req_id 可用于请求链路追踪。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let sml_data = SmlDataBuilder::default()
    .protocol(SchemalessProtocol::Line)
    .data(data)
    .req_id(100u64)
    .build()?;

client.put(&sml_data)?
`)),(0,esm/* mdx */.kt)("h3",{"id":"数据订阅"},`数据订阅`),(0,esm/* mdx */.kt)("p",null,`TDengine 通过消息队列 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../taos-sql/tmq/"},`TMQ`),` 启动一个订阅。`),(0,esm/* mdx */.kt)("h4",{"id":"创建-topic"},`创建 Topic`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`taos.exec_many([
    // create topic for subscription
    format!("CREATE TOPIC tmq_meters with META AS DATABASE {db}")
])
.await?;
`)),(0,esm/* mdx */.kt)("h4",{"id":"创建-consumer"},`创建 Consumer`),(0,esm/* mdx */.kt)("p",null,`从 DSN 开始，构建一个 TMQ 连接器。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let tmq = TmqBuilder::from_dsn("taos://localhost:6030/?group.id=test")?;
`)),(0,esm/* mdx */.kt)("p",null,`创建消费者：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut consumer = tmq.build()?;
`)),(0,esm/* mdx */.kt)("h4",{"id":"订阅消费数据"},`订阅消费数据`),(0,esm/* mdx */.kt)("p",null,`消费者可订阅一个或多个 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`TOPIC`),`。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.subscribe(["tmq_meters"]).await?;
`)),(0,esm/* mdx */.kt)("p",null,`TMQ 消息队列是一个 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/futures/latest/futures/stream/index.html"},`futures::Stream`),` 类型，可以使用相应 API 对每个消息进行消费，并通过 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.commit`),` 进行已消费标记。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`{
    let mut stream = consumer.stream();

    while let Some((offset, message)) = stream.try_next().await? {
        // get information from offset

        // the topic
        let topic = offset.topic();
        // the vgroup id, like partition id in kafka.
        let vgroup_id = offset.vgroup_id();
        println!("* in vgroup id {vgroup_id} of topic {topic}\\n");

        if let Some(data) = message.into_data() {
            while let Some(block) = data.fetch_raw_block().await? {
                // one block for one table, get table name if needed
                let name = block.table_name();
                let records: Vec<Record> = block.deserialize().try_collect()?;
                println!(
                    "** table: {}, got {} records: {:#?}\\n",
                    name.unwrap(),
                    records.len(),
                    records
                );
            }
        }
        consumer.commit(offset).await?;
    }
}
`)),(0,esm/* mdx */.kt)("p",null,`获取消费进度：`),(0,esm/* mdx */.kt)("p",null,`版本要求 connector-rust >= v0.8.8， TDengine >= 3.0.5.0`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let assignments = consumer.assignments().await.unwrap();
`)),(0,esm/* mdx */.kt)("h4",{"id":"指定订阅-offset"},`指定订阅 Offset`),(0,esm/* mdx */.kt)("p",null,`按照指定的进度消费：`),(0,esm/* mdx */.kt)("p",null,`版本要求 connector-rust >= v0.8.8， TDengine >= 3.0.5.0`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.offset_seek(topic, vgroup_id, offset).await;
`)),(0,esm/* mdx */.kt)("h4",{"id":"关闭订阅"},`关闭订阅`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.unsubscribe().await;
`)),(0,esm/* mdx */.kt)("p",null,`对于 TMQ DSN, 有以下配置项可以进行设置，需要注意的是，`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`group.id`),` 是必须的。`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`group.id`),`: 同一个消费者组，将以至少消费一次的方式进行消息负载均衡。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`client.id`),`: 可选的订阅客户端识别项。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`auto.offset.reset`),`: 可选初始化订阅起点， `,(0,esm/* mdx */.kt)("em",{parentName:"li"},`earliest`),` 为从头开始订阅， `,(0,esm/* mdx */.kt)("em",{parentName:"li"},`latest`),` 为仅从最新数据开始订阅，默认为从头订阅。注意，此选项在同一个 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`group.id`),` 中仅生效一次。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`enable.auto.commit`),`: 当设置为 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`true`),` 时，将启用自动标记模式，当对数据一致性不敏感时，可以启用此方式。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`auto.commit.interval.ms`),`: 自动标记的时间间隔。`)),(0,esm/* mdx */.kt)("h4",{"id":"完整示例"},`完整示例`),(0,esm/* mdx */.kt)("p",null,`完整订阅示例参见 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/3.0/docs/examples/rust/nativeexample/examples/subscribe_demo.rs"},`GitHub 示例文件`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"与连接池使用"},`与连接池使用`),(0,esm/* mdx */.kt)("p",null,`在复杂应用中，建议启用连接池。`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`taos`),` 的连接池默认（异步模式）使用 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://crates.io/crates/deadpool"},`deadpool`),` 实现。`),(0,esm/* mdx */.kt)("p",null,`如下，可以生成一个默认参数的连接池。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let pool: Pool<TaosBuilder> = TaosBuilder::from_dsn("taos:///")
    .unwrap()
    .pool()
    .unwrap();
`)),(0,esm/* mdx */.kt)("p",null,`同样可以使用连接池的构造器，对连接池参数进行设置：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let pool: Pool<TaosBuilder> = Pool::builder(Manager::from_dsn(self.dsn.clone()).unwrap().0)
    .max_size(88)  // 最大连接数
    .build()
    .unwrap();
`)),(0,esm/* mdx */.kt)("p",null,`在应用代码中，使用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`pool.get()?`),` 来获取一个连接对象 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/rust-connector-taos"},`Taos`),`。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let taos = pool.get()?;
`)),(0,esm/* mdx */.kt)("h3",{"id":"更多示例程序"},`更多示例程序`),(0,esm/* mdx */.kt)("p",null,`示例程序源码位于 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`TDengine/examples/rust`),` 下:`),(0,esm/* mdx */.kt)("p",null,`请参考：`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/tree/3.0/examples/rust"},`rust example`)),(0,esm/* mdx */.kt)("h2",{"id":"常见问题"},`常见问题`),(0,esm/* mdx */.kt)("p",null,`请参考 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../train-faq/faq"},`FAQ`)),(0,esm/* mdx */.kt)("h2",{"id":"api-参考"},`API 参考`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` 对象提供了多个数据库操作的 API：`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`exec`),`: 执行某个非查询类 SQL 语句，例如 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`CREATE`),`，`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ALTER`),`，`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),` 等。`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let affected_rows = taos.exec("INSERT INTO tb1 VALUES(now, NULL)").await?;
`))),(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`exec_many`),`: 同时（顺序）执行多个 SQL 语句。`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`taos.exec_many([
    "CREATE DATABASE test",
    "USE test",
    "CREATE TABLE \`tb1\` (\`ts\` TIMESTAMP, \`val\` INT)",
]).await?;
`))),(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`query`),`：执行查询语句，返回 `,`[ResultSet]`,` 对象。`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut q = taos.query("select * from log.logs").await?;
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`[ResultSet]`,` 对象存储了查询结果数据和返回的列的基本信息（列名，类型，长度）：`),(0,esm/* mdx */.kt)("p",{parentName:"li"},`列信息使用 `,`[.fields()]`,` 方法获取：`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let cols = q.fields();
for col in cols {
    println!("name: {}, type: {:?} , bytes: {}", col.name(), col.ty(), col.bytes());
}
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`逐行获取数据：`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut rows = result.rows();
let mut nrows = 0;
while let Some(row) = rows.try_next().await? {
    for (col, (name, value)) in row.enumerate() {
        println!(
            "[{}] got value in col {} (named \`{:>8}\`): {}",
            nrows, col, name, value
        );
    }
    nrows += 1;
}
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`或使用 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://serde.rs"},`serde`),` 序列化框架。`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`#[derive(Debug, Deserialize)]
struct Record {
    // deserialize timestamp to chrono::DateTime<Local>
    ts: DateTime<Local>,
    // float to f32
    current: Option<f32>,
    // int to i32
    voltage: Option<i32>,
    phase: Option<f32>,
    groupid: i32,
    // binary/varchar to String
    location: String,
}

let records: Vec<Record> = taos
    .query("select * from \`meters\`")
    .await?
    .deserialize()
    .try_collect()
    .await?;
`)))),(0,esm/* mdx */.kt)("p",null,`需要注意的是，需要使用 Rust 异步函数和异步运行时。`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` 提供部分 SQL 的 Rust 方法化以减少 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`format!`),` 代码块的频率：`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.describe(table: &str)`),`: 执行 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`DESCRIBE`),` 并返回一个 Rust 数据结构。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.create_database(database: &str)`),`: 执行 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`CREATE DATABASE`),` 语句。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.use_database(database: &str)`),`: 执行 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`USE`),` 语句。`)),(0,esm/* mdx */.kt)("p",null,`除此之外，该结构也是参数绑定和行协议接口的入口，使用方法请参考具体的 API 说明。`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{id:"stmt-api",style:{color:'#141414'}},"\u53C2\u6570\u7ED1\u5B9A\u63A5\u53E3")),(0,esm/* mdx */.kt)("p",null,`与 C 接口类似，Rust 提供参数绑定接口。首先，通过 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` 对象创建一个 SQL 语句的参数绑定对象 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Stmt.html"},`Stmt`),`：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = Stmt::init(&taos).await?;
stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")?;
`)),(0,esm/* mdx */.kt)("p",null,`参数绑定对象提供了一组接口用于实现参数绑定：`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.set_tbname(name)`)),(0,esm/* mdx */.kt)("p",null,`用于绑定表名。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = taos.stmt("insert into ? values(? ,?)")?;
stmt.set_tbname("d0")?;
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.set_tags(&[tag])`)),(0,esm/* mdx */.kt)("p",null,`当 SQL 语句使用超级表时，用于绑定子表表名和标签值：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = taos.stmt("insert into ? using stb0 tags(?) values(? ,?)")?;
stmt.set_tbname("d0")?;
stmt.set_tags(&[Value::VarChar("涛思".to_string())])?;
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.bind(&[column])`)),(0,esm/* mdx */.kt)("p",null,`用于绑定值类型。使用 `,`[ColumnView]`,` 结构体构建需要的类型并绑定：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let params = vec![
    ColumnView::from_millis_timestamp(vec![164000000000]),
    ColumnView::from_bools(vec![true]),
    ColumnView::from_tiny_ints(vec![i8::MAX]),
    ColumnView::from_small_ints(vec![i16::MAX]),
    ColumnView::from_ints(vec![i32::MAX]),
    ColumnView::from_big_ints(vec![i64::MAX]),
    ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
    ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
    ColumnView::from_unsigned_ints(vec![u32::MAX]),
    ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
    ColumnView::from_floats(vec![f32::MAX]),
    ColumnView::from_doubles(vec![f64::MAX]),
    ColumnView::from_varchar(vec!["ABC"]),
    ColumnView::from_nchar(vec!["涛思数据"]),
];
let rows = stmt.bind(&params)?.add_batch()?.execute()?;
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.execute()`)),(0,esm/* mdx */.kt)("p",null,`执行 SQL。`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Stmt.html"},`Stmt`),` 对象可以复用，在执行后可以重新绑定并执行。执行前请确保所有数据已通过 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.add_batch`),` 加入到执行队列中。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`stmt.execute()?;

// next bind cycle.
//stmt.set_tbname()?;
//stmt.bind()?;
//stmt.execute()?;
`)),(0,esm/* mdx */.kt)("p",null,`一个可运行的示例请见 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust/blob/main/examples/bind.rs"},`GitHub 上的示例`),`。`),(0,esm/* mdx */.kt)("p",null,`其他相关结构体 API 使用说明请移步 Rust 文档托管网页：`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos"},`https://docs.rs/taos`),`。`));};_26_rust_MDXContent.isMDXComponent=true;

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