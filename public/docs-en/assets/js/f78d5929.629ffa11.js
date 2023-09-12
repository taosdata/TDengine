"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[2632],{

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

/***/ 8958:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/restexample/examples/insert_example.rs"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 9900:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/stmt_example.rs"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5682:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::sync::*;

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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/restexample/examples/query_example.rs"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2437:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  assets: () => (/* binding */ assets),
  contentTitle: () => (/* binding */ _06_rust_contentTitle),
  "default": () => (/* binding */ _06_rust_MDXContent),
  frontMatter: () => (/* binding */ _06_rust_frontMatter),
  metadata: () => (/* binding */ metadata),
  toc: () => (/* binding */ _06_rust_toc)
});

// EXTERNAL MODULE: ./node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(7462);
// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/@mdx-js/react/dist/esm.js
var esm = __webpack_require__(3905);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/Tabs/index.js + 2 modules
var Tabs = __webpack_require__(4866);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/TabItem/index.js + 1 modules
var TabItem = __webpack_require__(5162);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_preparation.mdx
var _preparation = __webpack_require__(3181);
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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/schemaless_insert_line.rs"},`view source code`)));};MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/04-query-data/_rust.mdx
var _rust = __webpack_require__(5682);
;// CONCATENATED MODULE: ./docs/14-reference/03-connector/06-rust.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _06_rust_frontMatter={title:'TDengine Rust Connector',sidebar_label:'Rust',description:'This document describes the TDengine Rust connector.',toc_max_heading_level:4};const _06_rust_contentTitle=undefined;const metadata={"unversionedId":"reference/connector/rust","id":"reference/connector/rust","title":"TDengine Rust Connector","description":"This document describes the TDengine Rust connector.","source":"@site/docs/14-reference/03-connector/06-rust.mdx","sourceDirName":"14-reference/03-connector","slug":"/reference/connector/rust","permalink":"/docs-en/reference/connector/rust","draft":false,"tags":[],"version":"current","sidebarPosition":6,"frontMatter":{"title":"TDengine Rust Connector","sidebar_label":"Rust","description":"This document describes the TDengine Rust connector.","toc_max_heading_level":4},"sidebar":"defaultSidebar","previous":{"title":"Go","permalink":"/docs-en/reference/connector/go"},"next":{"title":"Python","permalink":"/docs-en/reference/connector/python"}};const assets={};const _06_rust_toc=[{value:'Supported platforms',id:'supported-platforms',level:2},{value:'Version history',id:'version-history',level:2},{value:'Handling exceptions',id:'handling-exceptions',level:2},{value:'TDengine DataType vs. Rust DataType',id:'tdengine-datatype-vs-rust-datatype',level:2},{value:'Installation Steps',id:'installation-steps',level:2},{value:'Pre-installation preparation',id:'pre-installation-preparation',level:3},{value:'Install the connectors',id:'install-the-connectors',level:3},{value:'Establishing a connection',id:'establishing-a-connection',level:2},{value:'Usage examples',id:'usage-examples',level:2},{value:'Create database and tables',id:'create-database-and-tables',level:3},{value:'Insert data',id:'insert-data',level:3},{value:'Query data',id:'query-data',level:3},{value:'execute SQL with req_id',id:'execute-sql-with-req_id',level:3},{value:'Writing data via parameter binding',id:'writing-data-via-parameter-binding',level:3},{value:'Schemaless Writing',id:'schemaless-writing',level:3},{value:'Schemaless with req_id',id:'schemaless-with-req_id',level:3},{value:'Data Subscription',id:'data-subscription',level:3},{value:'Create a Topic',id:'create-a-topic',level:4},{value:'Create a Consumer',id:'create-a-consumer',level:4},{value:'Subscribe to consume data',id:'subscribe-to-consume-data',level:4},{value:'Assignment subscription Offset',id:'assignment-subscription-offset',level:4},{value:'Close subscriptions',id:'close-subscriptions',level:4},{value:'Full Sample Code',id:'full-sample-code',level:4},{value:'Use with connection pool',id:'use-with-connection-pool',level:3},{value:'More sample programs',id:'more-sample-programs',level:3},{value:'Frequently Asked Questions',id:'frequently-asked-questions',level:2},{value:'API Reference',id:'api-reference',level:2}];const _06_rust_layoutProps={toc: _06_rust_toc};const _06_rust_MDXLayout="wrapper";function _06_rust_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_06_rust_MDXLayout,(0,esm_extends/* default */.Z)({},_06_rust_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://crates.io/crates/taos"},(0,esm/* mdx */.kt)("img",{parentName:"a","src":"https://img.shields.io/crates/v/taos","alt":"Crates.io"})),` `,(0,esm/* mdx */.kt)("img",{parentName:"p","src":"https://img.shields.io/crates/d/taos","alt":"Crates.io"}),` `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos"},(0,esm/* mdx */.kt)("img",{parentName:"a","src":"https://img.shields.io/docsrs/taos","alt":"docs.rs"}))),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` is the official Rust connector for TDengine. Rust developers can develop applications to access the TDengine instance data.`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` provides two ways to establish connections. One is the `,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Native Connection`),`, which connects to TDengine instances via the TDengine client driver (taosc). The other is the `,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`WebSocket connection`),`, which connects to TDengine instances via the WebSocket interface provided by taosAdapter. You can specify a connection type with Cargo features. By default, both types are supported. The Websocket connection can be used on any platform. The native connection can be used on any platform that the TDengine Client supports.`),(0,esm/* mdx */.kt)("p",null,`The source code for the Rust connectors is located on `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`GitHub`),`.`),(0,esm/* mdx */.kt)("h2",{"id":"supported-platforms"},`Supported platforms`),(0,esm/* mdx */.kt)("p",null,`Native connections are supported on the same platforms as the TDengine client driver.
Websocket connections are supported on all platforms that can run Go.`),(0,esm/* mdx */.kt)("h2",{"id":"version-history"},`Version history`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`connector-rust version`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`TDengine version`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`major features`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.9.2`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.7.0 or later`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`STMT: Get tag_fields and col_fields under ws.`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.8.12`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.5.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`TMQ: Get consuming progress and seek offset to consume.`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.8.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.4.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`Support schemaless insert.`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.7.6`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.3.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`Support req_id in query.`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`v0.6.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`3.0.0.0`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`Base features.`)))),(0,esm/* mdx */.kt)("p",null,`The Rust Connector is still under rapid development and is not guaranteed to be backward compatible before 1.0. We recommend using TDengine version 3.0 or higher to avoid known issues.`),(0,esm/* mdx */.kt)("h2",{"id":"handling-exceptions"},`Handling exceptions`),(0,esm/* mdx */.kt)("p",null,`After the error is reported, the specific information of the error can be obtained:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`match conn.exec(sql) {
    Ok(_) => {
        Ok(())
    }
    Err(e) => {
        eprintln!("ERROR: {:?}", e);
        Err(e)
    }
}
`)),(0,esm/* mdx */.kt)("h2",{"id":"tdengine-datatype-vs-rust-datatype"},`TDengine DataType vs. Rust DataType`),(0,esm/* mdx */.kt)("p",null,`TDengine currently supports timestamp, number, character, Boolean type, and the corresponding type conversion with Rust is as follows:`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`TDengine DataType`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`Rust DataType`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`TIMESTAMP`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`Timestamp`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`INT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i32`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BIGINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i64`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`FLOAT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`f32`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`DOUBLE`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`f64`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`SMALLINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i16`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`TINYINT`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`i8`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BOOL`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`bool`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`BINARY`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`Vec<u8`,`>`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`NCHAR`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`String`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`JSON`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`serde_json::Value`)))),(0,esm/* mdx */.kt)("p",null,`Note: Only TAG supports JSON types`),(0,esm/* mdx */.kt)("h2",{"id":"installation-steps"},`Installation Steps`),(0,esm/* mdx */.kt)("h3",{"id":"pre-installation-preparation"},`Pre-installation preparation`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Install the Rust development toolchain`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`If using the native connection, please install the TDengine client driver. Please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/connector#install-client-driver"},`install client driver`))),(0,esm/* mdx */.kt)("h3",{"id":"install-the-connectors"},`Install the connectors`),(0,esm/* mdx */.kt)("p",null,`Depending on the connection method, add the `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`taos`),` dependency in your Rust project as follows:`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"default",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"default",label:"Support Both",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`In `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`cargo.toml`),`, add `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`taos`),`:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
# use default feature
taos = "*"
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"rest",label:"Websocket only",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`In `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`cargo.toml`),`, add `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`taos`),` and enable the ws feature:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
taos = { version = "*", default-features = false, features = ["ws"] }
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"native",label:"native connection only",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`In `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`cargo.toml`),`, add `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`taos`),` and enable the native feature:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`[dependencies]
taos = { version = "*", default-features = false, features = ["native"] }
`)))),(0,esm/* mdx */.kt)("h2",{"id":"establishing-a-connection"},`Establishing a connection`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.TaosBuilder.html"},`TaosBuilder`),` creates a connection constructor through the DSN connection description string.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let builder = TaosBuilder::from_dsn("taos://")?;
`)),(0,esm/* mdx */.kt)("p",null,`You can now use this object to create the connection.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let conn = builder.build()?;
`)),(0,esm/* mdx */.kt)("p",null,`The connection object can create more than one.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let conn1 = builder.build()?;
let conn2 = builder.build()?;
`)),(0,esm/* mdx */.kt)("p",null,`The structure of the DSN description string is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-text"},`<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
`)),(0,esm/* mdx */.kt)("p",null,`The parameters are described as follows:`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`driver`),`: Specify a driver name so that the connector can choose which method to use to establish the connection. Supported driver names are as follows:`,(0,esm/* mdx */.kt)("ul",{parentName:"li"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`taos`),`: Table names use the TDengine connector driver.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`tmq`),`: Use the TMQ to subscribe to data.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`http/ws`),`: Use Websocket to establish connections.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`https/wss`),`: Use Websocket to establish connections, and enable SSL/TLS.`))),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`protocol`),`: Specify which connection method to use. For example, `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`taos+ws://localhost:6041`),` uses Websocket to establish connections.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`username/password`),`: Username and password used to create connections.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`host/port`),`: Specifies the server and port to establish a connection. If you do not specify a hostname or port, native connections default to `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`localhost:6030`),` and Websocket connections default to `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`localhost:6041`),`.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`database`),`: Specify the default database to connect to. It's optional.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("strong",{parentName:"li"},`params`),`: Optional parameters.`)),(0,esm/* mdx */.kt)("p",null,`A sample DSN description string is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-text"},`taos+ws://localhost:6041/test
`)),(0,esm/* mdx */.kt)("p",null,`This indicates that the Websocket connection method is used on port 6041 to connect to the server localhost and use the database `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`test`),` by default.`),(0,esm/* mdx */.kt)("p",null,`You can create DSNs to connect to servers in your environment.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

// use native protocol.
let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
let conn1 = builder.build();

//  use websocket protocol.
let builder2 = TaosBuilder::from_dsn("taos+ws://localhost:6041")?;
let conn2 = builder2.build();
`)),(0,esm/* mdx */.kt)("p",null,`After the connection is established, you can perform operations on your database.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`async fn demo(taos: &Taos, db: &str) -> Result<(), Error> {
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
`)),(0,esm/* mdx */.kt)("p",null,`There are two ways to query data: Using built-in types or the `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://serde.rs"},`serde`),` deserialization framework.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`    // Query option 1, use rows stream.
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
`)),(0,esm/* mdx */.kt)("h2",{"id":"usage-examples"},`Usage examples`),(0,esm/* mdx */.kt)("h3",{"id":"create-database-and-tables"},`Create database and tables`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

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
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},`The query is consistent with operating a relational database. When using subscripts to get the contents of the returned fields, you have to start from 1. However, we recommend using the field names to get the values of the fields in the result set.`)),(0,esm/* mdx */.kt)("h3",{"id":"insert-data"},`Insert data`),(0,esm/* mdx */.kt)(_rust_sql/* default */.ZP,{mdxType:"RustInsert"}),(0,esm/* mdx */.kt)("h3",{"id":"query-data"},`Query data`),(0,esm/* mdx */.kt)(_rust/* default */.ZP,{mdxType:"RustQuery"}),(0,esm/* mdx */.kt)("h3",{"id":"execute-sql-with-req_id"},`execute SQL with req_id`),(0,esm/* mdx */.kt)("p",null,`This req_id can be used to request link tracing.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let rs = taos.query_with_req_id("select * from stable where tag1 is null", 1)?;
`)),(0,esm/* mdx */.kt)("h3",{"id":"writing-data-via-parameter-binding"},`Writing data via parameter binding`),(0,esm/* mdx */.kt)("p",null,`TDengine has significantly improved the bind APIs to support data writing (INSERT) scenarios. Writing data in this way avoids the resource consumption of SQL syntax parsing, resulting in significant write performance improvements in many cases.`),(0,esm/* mdx */.kt)("p",null,`Parameter binding details see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"#stmt-api"},`API Reference`)),(0,esm/* mdx */.kt)(_rust_stmt/* default */.ZP,{mdxType:"RustBind"}),(0,esm/* mdx */.kt)("h3",{"id":"schemaless-writing"},`Schemaless Writing`),(0,esm/* mdx */.kt)("p",null,`TDengine supports schemaless writing. It is compatible with InfluxDB's Line Protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. For more information, see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../schemaless"},`Schemaless Writing`),`.`),(0,esm/* mdx */.kt)(MDXContent,{mdxType:"RustSml"}),(0,esm/* mdx */.kt)("h3",{"id":"schemaless-with-req_id"},`Schemaless with req_id`),(0,esm/* mdx */.kt)("p",null,`This req_id can be used to request link tracing.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let sml_data = SmlDataBuilder::default()
    .protocol(SchemalessProtocol::Line)
    .data(data)
    .req_id(100u64)
    .build()?;

client.put(&sml_data)?
`)),(0,esm/* mdx */.kt)("h3",{"id":"data-subscription"},`Data Subscription`),(0,esm/* mdx */.kt)("p",null,`TDengine starts subscriptions through `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../../taos-sql/tmq/"},`TMQ`),`.`),(0,esm/* mdx */.kt)("h4",{"id":"create-a-topic"},`Create a Topic`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`taos.exec_many([
    // create topic for subscription
    format!("CREATE TOPIC tmq_meters with META AS DATABASE {db}")
])
.await?;
`)),(0,esm/* mdx */.kt)("h4",{"id":"create-a-consumer"},`Create a Consumer`),(0,esm/* mdx */.kt)("p",null,`You create a TMQ connector by using a DSN.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let tmq = TmqBuilder::from_dsn("taos://localhost:6030/?group.id=test")?;
`)),(0,esm/* mdx */.kt)("p",null,`Create a consumer:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut consumer = tmq.build()?;
`)),(0,esm/* mdx */.kt)("h4",{"id":"subscribe-to-consume-data"},`Subscribe to consume data`),(0,esm/* mdx */.kt)("p",null,`A single consumer can subscribe to one or more topics.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.subscribe(["tmq_meters"]).await?;
`)),(0,esm/* mdx */.kt)("p",null,`The TMQ is of `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/futures/latest/futures/stream/index.html"},`futures::Stream`),` type. You can use the corresponding API to consume each message in the queue and then use `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.commit`),` to mark them as consumed.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`{
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
`)),(0,esm/* mdx */.kt)("p",null,`Get assignments：`),(0,esm/* mdx */.kt)("p",null,`Version requirements connector-rust >= v0.8.8, TDengine >= 3.0.5.0`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let assignments = consumer.assignments().await.unwrap();
`)),(0,esm/* mdx */.kt)("h4",{"id":"assignment-subscription-offset"},`Assignment subscription Offset`),(0,esm/* mdx */.kt)("p",null,`Seek offset：`),(0,esm/* mdx */.kt)("p",null,`Version requirements connector-rust >= v0.8.8, TDengine >= 3.0.5.0`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.offset_seek(topic, vgroup_id, offset).await;
`)),(0,esm/* mdx */.kt)("h4",{"id":"close-subscriptions"},`Close subscriptions`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.unsubscribe().await;
`)),(0,esm/* mdx */.kt)("p",null,`The following parameters can be configured for the TMQ DSN. Only `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`group.id`),` is mandatory.`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`group.id`),`: Within a consumer group, load balancing is implemented by consuming messages on an at-least-once basis.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`client.id`),`: Subscriber client ID.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`auto.offset.reset`),`: Initial point of subscription. `,(0,esm/* mdx */.kt)("em",{parentName:"li"},`earliest`),` subscribes from the beginning, and `,(0,esm/* mdx */.kt)("em",{parentName:"li"},`latest`),` subscribes from the newest message. The default is earliest. Note: This parameter is set per consumer group.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`enable.auto.commit`),`: Automatically commits. This can be enabled when data consistency is not essential.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`auto.commit.interval.ms`),`: Interval for automatic commits.`)),(0,esm/* mdx */.kt)("h4",{"id":"full-sample-code"},`Full Sample Code`),(0,esm/* mdx */.kt)("p",null,`For more information, see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/3.0/docs/examples/rust/nativeexample/examples/subscribe_demo.rs"},`GitHub sample file`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"use-with-connection-pool"},`Use with connection pool`),(0,esm/* mdx */.kt)("p",null,`In complex applications, we recommend enabling connection pools. `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`taos`),` implements connection pools based on `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://crates.io/crates/r2d2"},`r2d2`),`.`),(0,esm/* mdx */.kt)("p",null,`As follows, a connection pool with default parameters can be generated.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
`)),(0,esm/* mdx */.kt)("p",null,`You can set the same connection pool parameters using the connection pool's constructor.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let dsn = "taos://localhost:6030";

let opts = PoolBuilder::new()
    .max_size(5000) // max connections
    .max_lifetime(Some(Duration::from_secs(60 * 60))) // lifetime of each connection
    .min_idle(Some(1000)) // minimal idle connections
    .connection_timeout(Duration::from_secs(2));

let pool = TaosBuilder::from_dsn(dsn)?.with_pool_builder(opts)?;
`)),(0,esm/* mdx */.kt)("p",null,`In the application code, use `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`pool.get()? `),` to get a connection object `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust"},`Taos`),`.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let taos = pool.get()?;
`)),(0,esm/* mdx */.kt)("h3",{"id":"more-sample-programs"},`More sample programs`),(0,esm/* mdx */.kt)("p",null,`The source code of the sample application is under `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`TDengine/examples/rust`),` :`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/tree/3.0/examples/rust"},`rust example`)),(0,esm/* mdx */.kt)("h2",{"id":"frequently-asked-questions"},`Frequently Asked Questions`),(0,esm/* mdx */.kt)("p",null,`For additional troubleshooting, see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../../train-faq/faq"},`FAQ`),`.`),(0,esm/* mdx */.kt)("h2",{"id":"api-reference"},`API Reference`),(0,esm/* mdx */.kt)("p",null,`The `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` object provides an API to perform operations on multiple databases.`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`exec`),`: Execute some non-query SQL statements, such as `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`CREATE`),`, `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ALTER`),`, `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),`, etc.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let affected_rows = taos.exec("INSERT INTO tb1 VALUES(now, NULL)").await?;
`))),(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`exec_many`),`: Run multiple SQL statements simultaneously or in order.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`taos.exec_many([
    "CREATE DATABASE test",
    "USE test",
    "CREATE TABLE \`tb1\` (\`ts\` TIMESTAMP, \`val\` INT)",
]).await?;
`))),(0,esm/* mdx */.kt)("li",{parentName:"ol"},(0,esm/* mdx */.kt)("p",{parentName:"li"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`query`),`: Run a query statement and return a `,`[ResultSet]`,` object.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut q = taos.query("select * from log.logs").await?;
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`The `,`[ResultSet]`,` object stores query result data and the names, types, and lengths of returned columns`),(0,esm/* mdx */.kt)("p",{parentName:"li"},`You can obtain column information by using `,`[.fields()]`,`.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let cols = q.fields();
for col in cols {
    println!("name: {}, type: {:?} , bytes: {}", col.name(), col.ty(), col.bytes());
}
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`It fetches data line by line.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut rows = result.rows();
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
`)),(0,esm/* mdx */.kt)("p",{parentName:"li"},`Or use the `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://serde.rs"},`serde`),` deserialization framework.`),(0,esm/* mdx */.kt)("pre",{parentName:"li"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`#[derive(Debug, Deserialize)]
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
`)))),(0,esm/* mdx */.kt)("p",null,`Note that Rust asynchronous functions and an asynchronous runtime are required.`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` provides Rust methods for some SQL statements to reduce the number of `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`format!`),`s.`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.describe(table: &str)`),`: Executes `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`DESCRIBE`),` and returns a Rust data structure.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.create_database(database: &str)`),`: Executes the `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`CREATE DATABASE`),` statement.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`.use_database(database: &str)`),`: Executes the `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`USE`),` statement.`)),(0,esm/* mdx */.kt)("p",null,`In addition, this structure is also the entry point for Parameter Binding and Line Protocol Interface. Please refer to the specific API descriptions for usage.`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{id:"stmt-api",style:{color:'#141414'}},"Bind Interface")),(0,esm/* mdx */.kt)("p",null,`Similar to the C interface, Rust provides the bind interface's wrapping. First, the `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Taos.html"},`Taos`),` object creates a parameter binding object `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Stmt.html"},`Stmt`),` for an SQL statement.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = Stmt::init(&taos).await?;
stmt.prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")?;
`)),(0,esm/* mdx */.kt)("p",null,`The bind object provides a set of interfaces for implementing parameter binding.`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.set_tbname(name)`)),(0,esm/* mdx */.kt)("p",null,`To bind table names.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = taos.stmt("insert into ? values(? ,?)")?;
stmt.set_tbname("d0")?;
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.set_tags(&[tag])`)),(0,esm/* mdx */.kt)("p",null,`Bind sub-table table names and tag values when the SQL statement uses a super table.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut stmt = taos.stmt("insert into ? using stb0 tags(?) values(? ,?)")?;
stmt.set_tbname("d0")?;
stmt.set_tags(&[Value::VarChar("taos".to_string())])?;
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.bind(&[column])`)),(0,esm/* mdx */.kt)("p",null,`Bind value types. Use the `,`[ColumnView]`,` structure to create and bind the required types.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let params = vec![
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
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.execute()`)),(0,esm/* mdx */.kt)("p",null,`Execute SQL. `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos/latest/taos/struct.Stmt.html"},`Stmt`),` objects can be reused, re-binded, and executed after execution. Before execution, ensure that all data has been added to the queue with `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`.add_batch`),`.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`stmt.execute()?;

// next bind cycle.
//stmt.set_tbname()?;
//stmt.bind()?;
//stmt.execute()?;
`)),(0,esm/* mdx */.kt)("p",null,`For a working example, see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/taos-connector-rust/blob/main/taos/examples/bind.rs"},`GitHub`),`.`),(0,esm/* mdx */.kt)("p",null,`For information about other structure APIs, see the `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos"},`Rust documentation`),`.`));};_06_rust_MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3181:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* unused harmony exports frontMatter, contentTitle, toc, default */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Client driver installed (mandatory for native connections, not required for REST connections)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"info"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`Since the TDengine client driver is written in C, using the native connection requires loading the client driver shared library file, which is usually included in the TDengine installer. You can install either standard TDengine server installation package or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/get-started/"},`TDengine client installation package`),`. For Windows development, you need to install the corresponding Windows client, please refer to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../get-started/package"},`Install TDengine`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`libtaos.so: After successful installation of TDengine on a Linux system, the dependent Linux version of the client driver `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`libtaos.so`),` file will be automatically linked to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/usr/lib/libtaos.so`),`, which is included in the Linux scannable path and does not need to be specified separately.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`taos.dll: After installing the client on Windows, the dependent Windows version of the client driver taos.dll file will be automatically copied to the system default search path C:/Windows/System32, again without the need to specify it separately.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`libtaos.dylib: After successful installation of TDengine on a mac system, the dependent macOS version of the client driver `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`libtaos.dylib`),` file will be automatically linked to `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/usr/local/lib/libtaos.dylib`),`, which is included in the macOS scannable path and does not need to be specified separately.`))));};MDXContent.isMDXComponent=true;

/***/ })

}]);