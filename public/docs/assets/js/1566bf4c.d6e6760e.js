"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[2822],{

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

/***/ 3184:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _40_opentsdb_telnet_contentTitle),
  "default": () => (/* binding */ _40_opentsdb_telnet_MDXContent),
  "frontMatter": () => (/* binding */ _40_opentsdb_telnet_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _40_opentsdb_telnet_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_java_opts_telnet.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TelnetLineProtocolExample {
    // format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
    private static String[] lines = { "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
            "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
            "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
            "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
            "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
            "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
            "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
            "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
    };

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void createDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // the default precision is ms (microsecond), but we use us(microsecond) here.
            stmt.execute("CREATE DATABASE IF NOT EXISTS test precision 'us'");
            stmt.execute("USE test");
        }
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = getConnection()) {
            createDatabase(conn);
            SchemalessWriter writer = new SchemalessWriter(conn);
            writer.write(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.NOT_CONFIGURED);
        }
    }

}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/TelnetLineProtocolExample.java"},`查看源码`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_py_opts_telnet.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _py_opts_telnet_frontMatter={};const _py_opts_telnet_contentTitle=(/* unused pure expression or super */ null && (undefined));const _py_opts_telnet_toc=[];const _py_opts_telnet_layoutProps={toc: _py_opts_telnet_toc};const _py_opts_telnet_MDXLayout="wrapper";function _py_opts_telnet_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_py_opts_telnet_MDXLayout,(0,esm_extends/* default */.Z)({},_py_opts_telnet_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`import taos
from taos import SmlProtocol, SmlPrecision

# format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
lines = ["meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
         "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
         "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
         "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
         "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
         ]


# create connection use firstEp in taos.cfg.
def get_connection():
    return taos.connect()


def create_database(conn):
    conn.execute("CREATE DATABASE test")
    conn.execute("USE test")


def insert_lines(conn):
    affected_rows = conn.schemaless_insert(
        lines, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
    print(affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    try:
        create_database(connection)
        insert_lines(connection)
    finally:
        connection.close()

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/telnet_line_protocol_example.py"},`查看源码`)));};_py_opts_telnet_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_go_opts_telnet.mdx
var _go_opts_telnet = __webpack_require__(5185);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_rust_opts_telnet.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _rust_opts_telnet_frontMatter={};const _rust_opts_telnet_contentTitle=(/* unused pure expression or super */ null && (undefined));const _rust_opts_telnet_toc=[];const _rust_opts_telnet_layoutProps={toc: _rust_opts_telnet_toc};const _rust_opts_telnet_MDXLayout="wrapper";function _rust_opts_telnet_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_rust_opts_telnet_MDXLayout,(0,esm_extends/* default */.Z)({},_rust_opts_telnet_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},``)));};_rust_opts_telnet_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_js_opts_telnet.mdx
var _js_opts_telnet = __webpack_require__(7041);
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_cs_opts_telnet.mdx
var _cs_opts_telnet = __webpack_require__(9036);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_c_opts_telnet.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _c_opts_telnet_frontMatter={};const _c_opts_telnet_contentTitle=(/* unused pure expression or super */ null && (undefined));const _c_opts_telnet_toc=[];const _c_opts_telnet_layoutProps={toc: _c_opts_telnet_toc};const _c_opts_telnet_MDXLayout="wrapper";function _c_opts_telnet_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_c_opts_telnet_MDXLayout,(0,esm_extends/* default */.Z)({},_c_opts_telnet_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 6030);
  if (taos == NULL) {
    printf("failed to connect to server\\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS test");
  executeSQL(taos, "CREATE DATABASE test");
  executeSQL(taos, "USE test");
  char *lines[] = {
      "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
      "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
      "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
      "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
      "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
      "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
      "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
      "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
  };
  TAOS_RES *res = taos_schemaless_insert(taos, lines, 8, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\\n", taos_errstr(res));
  } else {
    int affectedRow = taos_affected_rows(res);
    printf("successfully inserted %d rows\\n", affectedRow);
  }

  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
// output:
// successfully inserted 8 rows
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/telnet_line_example.c"},`查看源码`)));};_c_opts_telnet_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/40-opentsdb-telnet.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _40_opentsdb_telnet_frontMatter={sidebar_label:'OpenTSDB 行协议',title:'OpenTSDB 行协议'};const _40_opentsdb_telnet_contentTitle=undefined;const metadata={"unversionedId":"develop/insert-data/opentsdb-telnet","id":"develop/insert-data/opentsdb-telnet","title":"OpenTSDB 行协议","description":"协议介绍","source":"@site/docs/07-develop/03-insert-data/40-opentsdb-telnet.mdx","sourceDirName":"07-develop/03-insert-data","slug":"/develop/insert-data/opentsdb-telnet","permalink":"/docs/develop/insert-data/opentsdb-telnet","draft":false,"tags":[],"version":"current","sidebarPosition":40,"frontMatter":{"sidebar_label":"OpenTSDB 行协议","title":"OpenTSDB 行协议"},"sidebar":"defaultSidebar","previous":{"title":"InfluxDB 行协议","permalink":"/docs/develop/insert-data/influxdb-line"},"next":{"title":"OpenTSDB JSON 格式协议","permalink":"/docs/develop/insert-data/opentsdb-json"}};const assets={};const _40_opentsdb_telnet_toc=[{value:'协议介绍',id:'协议介绍',level:2},{value:'示例代码',id:'示例代码',level:2},{value:'SQL 查询示例',id:'sql-查询示例',level:2}];const _40_opentsdb_telnet_layoutProps={toc: _40_opentsdb_telnet_toc};const _40_opentsdb_telnet_MDXLayout="wrapper";function _40_opentsdb_telnet_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_40_opentsdb_telnet_MDXLayout,(0,esm_extends/* default */.Z)({},_40_opentsdb_telnet_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h2",{"id":"协议介绍"},`协议介绍`),(0,esm/* mdx */.kt)("p",null,`OpenTSDB 行协议同样采用一行字符串来表示一行数据。OpenTSDB 采用的是单列模型，因此一行只能包含一个普通数据列。标签列依然可以有多个。分为四部分，具体格式约定如下：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-txt"},`<metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
`)),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`metric 将作为超级表名；`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`timestamp 本行数据对应的时间戳。根据时间戳的长度自动识别时间精度。支持秒和毫秒两种时间精度；`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`value 度量值，必须为一个数值。对应的列名是 “`,`_`,`value”；`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`最后一部分是标签集， 用空格分隔不同标签， 所有标签自动转化为 NCHAR 数据类型。`)),(0,esm/* mdx */.kt)("p",null,`例如：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-txt"},`meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3
`)),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`默认生产的子表名是根据规则生成的唯一 ID 值。用户也可以通过在client端的 taos.cfg 里配置 smlChildTableName 参数来指定某个标签值作为子表名。该标签值应该具有全局唯一性。举例如下：假设有个标签名为tname, 配置 smlChildTableName=tname, 插入数据为 meters.current 1648432611250 11.3 tname=cpu1 location=California.LosAngeles groupid=3 则创建的表名为 cpu1，注意如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。
参考 `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"http://opentsdb.net/docs/build/html/api_telnet/put.html"},`OpenTSDB Telnet API 文档`),`。`)),(0,esm/* mdx */.kt)("h2",{"id":"示例代码"},`示例代码`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"JavaTelnet"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_py_opts_telnet_MDXContent,{mdxType:"PyTelnet"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_go_opts_telnet/* default */.ZP,{mdxType:"GoTelnet"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"nodejs",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_js_opts_telnet/* default */.ZP,{mdxType:"NodeTelnet"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_cs_opts_telnet/* default */.ZP,{mdxType:"CsTelnet"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_c_opts_telnet_MDXContent,{mdxType:"CTelnet"}))),(0,esm/* mdx */.kt)("p",null,`以上示例代码会自动创建 2 个超级表， 每个超级表有 4 条数据。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-cmd"},`taos> USE test;
Database changed.

taos> SHOW STABLES;
              name              |
=================================
 meters_current                 |
 meters_voltage                 |
Query OK, 2 row(s) in set (0.002544s)

taos> SELECT TBNAME, * FROM \`meters_current\`;
             tbname             |           _ts            |           _value           | groupid |            location            |
==================================================================================================================================
 t_0e7bcfa21a02331c06764f275... | 2022-03-28 09:56:51.249 |              10.800000000 | 3       | California.LosAngeles                |
 t_0e7bcfa21a02331c06764f275... | 2022-03-28 09:56:51.250 |              11.300000000 | 3       | California.LosAngeles                |
 t_7e7b26dd860280242c6492a16... | 2022-03-28 09:56:51.249 |              10.300000000 | 2       | California.SanFrancisco               |
 t_7e7b26dd860280242c6492a16... | 2022-03-28 09:56:51.250 |              12.600000000 | 2       | California.SanFrancisco               |
Query OK, 4 row(s) in set (0.005399s)
`)),(0,esm/* mdx */.kt)("h2",{"id":"sql-查询示例"},`SQL 查询示例`),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`meters_current`),` 是插入数据的超级表名。`),(0,esm/* mdx */.kt)("p",null,`可以通过超级表的 TAG 来过滤数据，比如查询 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`location=California.LosAngeles groupid=3`),` 可以通过如下 SQL：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`SELECT * FROM \`meters_current\` WHERE location = "California.LosAngeles" AND groupid = 3;
`)));};_40_opentsdb_telnet_MDXContent.isMDXComponent=true;

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

/***/ 7041:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-js"},`const taos = require("@tdengine/client");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();

function createDatabase() {
  cursor.execute("CREATE DATABASE test");
  cursor.execute("USE test");
}

function insertData() {
  const lines = [
    "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
    "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
    "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
    "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
    "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
    "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
  ];
  cursor.schemalessInsert(
    lines,
    taos.SCHEMALESS_PROTOCOL.TSDB_SML_TELNET_PROTOCOL,
    taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NOT_CONFIGURED
  );
}

try {
  createDatabase();
  insertData();
} finally {
  cursor.close();
  conn.close();
}

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/opentsdb_telnet_example.js"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);