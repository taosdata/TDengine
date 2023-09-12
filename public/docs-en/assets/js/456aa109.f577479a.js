"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[3854],{

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

/***/ 3045:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  assets: () => (/* binding */ assets),
  contentTitle: () => (/* binding */ _01_sql_writing_contentTitle),
  "default": () => (/* binding */ _01_sql_writing_MDXContent),
  frontMatter: () => (/* binding */ _01_sql_writing_frontMatter),
  metadata: () => (/* binding */ metadata),
  toc: () => (/* binding */ _01_sql_writing_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_java_sql.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;


public class RestInsertExample {
    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static List<String> getRawData() {
        return Arrays.asList(
                "d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2",
                "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2",
                "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2",
                "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3",
                "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2",
                "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2",
                "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3",
                "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3"
        );
    }


    /**
     * The generated SQL is:
     * INSERT INTO power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000)
     * power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:15.000',12.60000,218,0.33000)
     * power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:16.800',12.30000,221,0.31000)
     * power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES('2018-10-03 14:38:16.650',10.30000,218,0.25000)
     * power.d1003 USING power.meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:05.500',11.80000,221,0.28000)
     * power.d1003 USING power.meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:16.600',13.40000,223,0.29000)
     * power.d1004 USING power.meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:05.000',10.80000,223,0.29000)
     * power.d1004 USING power.meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:06.500',11.50000,221,0.35000)
     */
    private static String getSQL() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (String line : getRawData()) {
            String[] ps = line.split(",");
            sb.append("power." + ps[0]).append(" USING power.meters TAGS(")
                    .append(ps[5]).append(", ") // tag: location
                    .append(ps[6]) // tag: groupId
                    .append(") VALUES(")
                    .append('\\'').append(ps[1]).append('\\'').append(",") // ts
                    .append(ps[2]).append(",") // current
                    .append(ps[3]).append(",") // voltage
                    .append(ps[4]).append(") "); // phase
        }
        return sb.toString();
    }

    public static void insertData() throws SQLException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE DATABASE power KEEP 3650");
                stmt.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                        "TAGS (location BINARY(64), groupId INT)");
                String sql = getSQL();
                int rowCount = stmt.executeUpdate(sql);
                System.out.println("rowCount=" + rowCount); // rowCount=8
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        insertData();
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/RestInsertExample.java"},`view source code`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_java_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _java_stmt_frontMatter={};const _java_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _java_stmt_toc=[];const _java_stmt_layoutProps={toc: _java_stmt_toc};const _java_stmt_MDXLayout="wrapper";function _java_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_java_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_java_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class StmtInsertExample {
    private static String datePattern = "yyyy-MM-dd HH:mm:ss.SSS";
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);

    private static List<String> getRawData(int size) {
        SimpleDateFormat format = new SimpleDateFormat(datePattern);
        List<String> result = new ArrayList<>();
        long current = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            String time = format.format(current + i);
            int id = random.nextInt(10);
            result.add("d" + id + "," + time + ",10.30000,219,0.31000,California.SanFrancisco,2");
        }
        return result.stream()
                .sorted(Comparator.comparing(s -> s.split(",")[0])).collect(Collectors.toList());
    }

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void createTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE if not exists power KEEP 3650");
            stmt.executeUpdate("use power");
            stmt.execute("CREATE STABLE if not exists meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                    "TAGS (location BINARY(64), groupId INT)");
        }
    }

    private static void insertData() throws SQLException {
        try (Connection conn = getConnection()) {
            createTable(conn);
            String psql = "INSERT INTO ? USING power.meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
            try (TSDBPreparedStatement pst = (TSDBPreparedStatement) conn.prepareStatement(psql)) {
                String tableName = null;
                ArrayList<Long> ts = new ArrayList<>();
                ArrayList<Float> current = new ArrayList<>();
                ArrayList<Integer> voltage = new ArrayList<>();
                ArrayList<Float> phase = new ArrayList<>();
                for (String line : getRawData(100000)) {
                    String[] ps = line.split(",");
                    if (tableName == null) {
                        // bind table name and tags
                        tableName = "power." + ps[0];
                        pst.setTableName(ps[0]);
                        pst.setTagString(0, ps[5]);
                        pst.setTagInt(1, Integer.valueOf(ps[6]));
                    } else {
                        if (!tableName.equals(ps[0])) {
                            pst.setTimestamp(0, ts);
                            pst.setFloat(1, current);
                            pst.setInt(2, voltage);
                            pst.setFloat(3, phase);
                            pst.columnDataAddBatch();
                            pst.columnDataExecuteBatch();

                            // bind table name and tags
                            tableName = ps[0];
                            pst.setTableName(ps[0]);
                            pst.setTagString(0, ps[5]);
                            pst.setTagInt(1, Integer.valueOf(ps[6]));
                            ts.clear();
                            current.clear();
                            voltage.clear();
                            phase.clear();
                        }
                    }
                    // bind values
                    // ps[1] looks like: 2018-10-03 14:38:05.000
                    LocalDateTime localDateTime = LocalDateTime.parse(ps[1], formatter);
                    ts.add(localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
                    current.add(Float.valueOf(ps[2]));
                    voltage.add(Integer.valueOf(ps[3]));
                    phase.add(Float.valueOf(ps[4]));
                }
                pst.setTimestamp(0, ts);
                pst.setFloat(1, current);
                pst.setInt(2, voltage);
                pst.setFloat(3, phase);
                pst.columnDataAddBatch();
                pst.columnDataExecuteBatch();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        insertData();
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/StmtInsertExample.java"},`view source code`)));};_java_stmt_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_py_sql.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _py_sql_frontMatter={};const _py_sql_contentTitle=(/* unused pure expression or super */ null && (undefined));const _py_sql_toc=[];const _py_sql_layoutProps={toc: _py_sql_toc};const _py_sql_MDXLayout="wrapper";function _py_sql_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_py_sql_MDXLayout,(0,esm_extends/* default */.Z)({},_py_sql_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`import taos

lines = ["d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2",
         "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3",
         "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2",
         "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3",
         "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3",
         "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2",
         "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2",
         "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2"]


def get_connection() -> taos.TaosConnection:
    """
    create connection use firstEp in taos.cfg and use default user and password.
    """
    return taos.connect()


def create_stable(conn: taos.TaosConnection):
    conn.execute("CREATE DATABASE power")
    conn.execute("USE power")
    conn.execute("CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")


# The generated SQL is:
# INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
#             d1002 USING meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
#             d1003 USING meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
#             d1004 USING meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)

def get_sql():
    global lines
    lines = map(lambda line: line.split(','), lines)  # [['d1001', ...]...]
    lines = sorted(lines, key=lambda ls: ls[0])  # sort by table name
    sql = "INSERT INTO "
    tb_name = None
    for ps in lines:
        tmp_tb_name = ps[0]
        if tb_name != tmp_tb_name:
            tb_name = tmp_tb_name
            sql += f"{tb_name} USING meters TAGS({ps[5]}, {ps[6]}) VALUES "
        sql += f"('{ps[1]}', {ps[2]}, {ps[3]}, {ps[4]}) "
    return sql


def insert_data(conn: taos.TaosConnection):
    sql = get_sql()
    affected_rows = conn.execute(sql)
    print("affected_rows", affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    try:
        create_stable(connection)
        insert_data(connection)
    finally:
        connection.close()

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/native_insert_example.py"},`view source code`)));};_py_sql_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_py_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _py_stmt_frontMatter={};const _py_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _py_stmt_toc=[];const _py_stmt_layoutProps={toc: _py_stmt_toc};const _py_stmt_MDXLayout="wrapper";function _py_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_py_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_py_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py","metastring":"title=Single Row Binding","title":"Single","Row":true,"Binding":true},`import taos
from datetime import datetime

# note: lines have already been sorted by table name
lines = [('d1001', '2018-10-03 14:38:05.000', 10.30000, 219, 0.31000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:15.000', 12.60000, 218, 0.33000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:16.800', 12.30000, 221, 0.31000, 'California.SanFrancisco', 2),
         ('d1002', '2018-10-03 14:38:16.650', 10.30000, 218, 0.25000, 'California.SanFrancisco', 3),
         ('d1003', '2018-10-03 14:38:05.500', 11.80000, 221, 0.28000, 'California.LosAngeles', 2),
         ('d1003', '2018-10-03 14:38:16.600', 13.40000, 223, 0.29000, 'California.LosAngeles', 2),
         ('d1004', '2018-10-03 14:38:05.000', 10.80000, 223, 0.29000, 'California.LosAngeles', 3),
         ('d1004', '2018-10-03 14:38:06.500', 11.50000, 221, 0.35000, 'California.LosAngeles', 3)]


def get_ts(ts: str):
    dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
    return int(dt.timestamp() * 1000)


def create_stable():
    conn = taos.connect()
    try:
        conn.execute("CREATE DATABASE power")
        conn.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                     "TAGS (location BINARY(64), groupId INT)")
    finally:
        conn.close()


def bind_row_by_row(stmt: taos.TaosStmt):
    tb_name = None
    for row in lines:
        if tb_name != row[0]:
            tb_name = row[0]
            tags: taos.TaosBind = taos.new_bind_params(2)  # 2 is count of tags
            tags[0].binary(row[5])  # location
            tags[1].int(row[6])  # groupId
            stmt.set_tbname_tags(tb_name, tags)
        values: taos.TaosBind = taos.new_bind_params(4)  # 4 is count of columns
        values[0].timestamp(get_ts(row[1]))
        values[1].float(row[2])
        values[2].int(row[3])
        values[3].float(row[4])
        stmt.bind_param(values)


def insert_data():
    conn = taos.connect(database="power")
    try:
        stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
        bind_row_by_row(stmt)
        stmt.execute()
        stmt.close()
    finally:
        conn.close()


if __name__ == '__main__':
    create_stable()
    insert_data()

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/bind_param_example.py"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py","metastring":"title=Multiple Row Binding","title":"Multiple","Row":true,"Binding":true},`table_tags = {
    "d1001": ('California.SanFrancisco', 2),
    "d1002": ('California.SanFrancisco', 3),
    "d1003": ('California.LosAngeles', 2),
    "d1004": ('California.LosAngeles', 3)
}

table_values = {
    "d1001": [
        ['2018-10-03 14:38:05.000', '2018-10-03 14:38:15.000', '2018-10-03 14:38:16.800'],
        [10.3, 12.6, 12.3],
        [219, 218, 221],
        [0.31, 0.33, 0.32]
    ],
    "d1002": [
        ['2018-10-03 14:38:16.650'], [10.3], [218], [0.25]
    ],
    "d1003": [
        ['2018-10-03 14:38:05.500', '2018-10-03 14:38:16.600'],
        [11.8, 13.4],
        [221, 223],
        [0.28, 0.29]
    ],
    "d1004": [
        ['2018-10-03 14:38:05.500', '2018-10-03 14:38:06.500'],
        [10.8, 11.5],
        [223, 221],
        [0.29, 0.35]
    ]
}


def bind_multi_rows(stmt: taos.TaosStmt):
    """
    batch bind example
    """
    for tb_name in table_values.keys():
        tags = table_tags[tb_name]
        tag_params = taos.new_bind_params(2)
        tag_params[0].binary(tags[0])
        tag_params[1].int(tags[1])
        stmt.set_tbname_tags(tb_name, tag_params)

        values = table_values[tb_name]
        value_params = taos.new_multi_binds(4)
        value_params[0].timestamp([get_ts(t) for t in values[0]])
        value_params[1].float(values[1])
        value_params[2].int(values[2])
        value_params[3].float(values[3])
        stmt.bind_param_batch(value_params)


def insert_data():
    conn = taos.connect(database="power")
    try:
        stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
        bind_multi_rows(stmt)
        stmt.execute()
        stmt.close()
    finally:
        conn.close()


`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/multi_bind_example.py"},`view source code`)),(0,esm/* mdx */.kt)("admonition",{"type":"info"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`Multiple row binding is better in performance than single row binding, but it can only be used with `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),` statement while single row binding can be used for other SQL statements besides `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),`.`)));};_py_stmt_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_go_sql.mdx
var _go_sql = __webpack_require__(2010);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_go_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _go_stmt_frontMatter={};const _go_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _go_stmt_toc=[];const _go_stmt_layoutProps={toc: _go_stmt_toc};const _go_stmt_MDXLayout="wrapper";function _go_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_go_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_go_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`package main

import (
    "fmt"
    "time"

    "github.com/taosdata/driver-go/v3/af"
    "github.com/taosdata/driver-go/v3/common"
    "github.com/taosdata/driver-go/v3/common/param"
)

func checkErr(err error, prompt string) {
    if err != nil {
        fmt.Printf("%s\\n", prompt)
        panic(err)
    }
}

func prepareStable(conn *af.Connector) {
    _, err := conn.Exec("CREATE DATABASE power")
    checkErr(err, "failed to create database")
    _, err = conn.Exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
    checkErr(err, "failed to create stable")
    _, err = conn.Exec("USE power")
    checkErr(err, "failed to change database")
}

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    checkErr(err, "fail to connect")
    defer conn.Close()
    prepareStable(conn)
    // create stmt
    stmt := conn.InsertStmt()
    defer stmt.Close()
    err = stmt.Prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
    checkErr(err, "failed to create prepare statement")

    // bind table name and tags
    tagParams := param.NewParam(2).AddBinary([]byte("California.SanFrancisco")).AddInt(2)
    err = stmt.SetTableNameWithTags("d1001", tagParams)
    checkErr(err, "failed to execute SetTableNameWithTags")

    // specify ColumnType
    var bindType *param.ColumnType = param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()

    // bind values. note: can only bind one row each time.
    valueParams := []*param.Param{
        param.NewParam(1).AddTimestamp(time.Unix(1648432611, 249300000), common.PrecisionMilliSecond),
        param.NewParam(1).AddFloat(10.3),
        param.NewParam(1).AddInt(219),
        param.NewParam(1).AddFloat(0.31),
    }
    err = stmt.BindParam(valueParams, bindType)
    checkErr(err, "BindParam error")
    err = stmt.AddBatch()
    checkErr(err, "AddBatch error")

    // bind one more row
    valueParams = []*param.Param{
        param.NewParam(1).AddTimestamp(time.Unix(1648432611, 749300000), common.PrecisionMilliSecond),
        param.NewParam(1).AddFloat(12.6),
        param.NewParam(1).AddInt(218),
        param.NewParam(1).AddFloat(0.33),
    }
    err = stmt.BindParam(valueParams, bindType)
    checkErr(err, "BindParam error")
    err = stmt.AddBatch()
    checkErr(err, "AddBatch error")
    // execute
    err = stmt.Execute()
    checkErr(err, "Execute batch error")
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/insert/stmt/main.go"},`view source code`)),(0,esm/* mdx */.kt)("admonition",{"type":"tip"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`github.com/taosdata/driver-go/v3/wrapper`),` module in driver-go is the wrapper for C API, it can be used to insert data with parameter binding.`)));};_go_stmt_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_rust_sql.mdx
var _rust_sql = __webpack_require__(8958);
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_rust_stmt.mdx
var _rust_stmt = __webpack_require__(9900);
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_js_sql.mdx
var _js_sql = __webpack_require__(9301);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_js_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _js_stmt_frontMatter={};const _js_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _js_stmt_toc=[];const _js_stmt_layoutProps={toc: _js_stmt_toc};const _js_stmt_MDXLayout="wrapper";function _js_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_js_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_js_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=Single Row Binding","title":"Single","Row":true,"Binding":true},`const taos = require("@tdengine/client");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();

function prepareSTable() {
  cursor.execute("CREATE DATABASE power");
  cursor.execute("USE power");
  cursor.execute(
    "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
  );
}

function insertData() {
  // init
  cursor.stmtInit();
  // prepare
  cursor.stmtPrepare(
    "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)"
  );

  // bind table name and tags
  let tagBind = new taos.TaosMultiBindArr(2);
  tagBind.multiBindBinary(["California.SanFrancisco"]);
  tagBind.multiBindInt([2]);
  cursor.stmtSetTbnameTags("d1001", tagBind.getMultiBindArr());

  // bind values
  let rows = [[1648432611249, 1648432611749], [10.3, 12.6], [219, 218], [0.31, 0.33]];

  let valueBind = new taos.TaosMultiBindArr(4);
  valueBind.multiBindTimestamp(rows[0]);
  valueBind.multiBindFloat(rows[1]);
  valueBind.multiBindInt(rows[2]);
  valueBind.multiBindFloat(rows[3]);
  cursor.stmtBindParamBatch(valueBind.getMultiBindArr());
  cursor.stmtAddBatch();


  // execute
  cursor.stmtExecute();
  cursor.stmtClose();
}

try {
  prepareSTable();
  insertData();
} finally {
  cursor.close();
  conn.close();
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/param_bind_example.js"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=Multiple Row Binding","title":"Multiple","Row":true,"Binding":true},`function insertData() {
  // init
  cursor.stmtInit();
  // prepare
  cursor.stmtPrepare(
    "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)"
  );

  // bind table name and tags
  let tagBind = new taos.TaosMultiBindArr(2);
  tagBind.multiBindBinary(["California.SanFrancisco"]);
  tagBind.multiBindInt([2]);
  cursor.stmtSetTbnameTags("d1001", tagBind.getMultiBindArr());

  // bind values
  let valueBind = new taos.TaosMultiBindArr(4);
  valueBind.multiBindTimestamp([1648432611249, 1648432611749]);
  valueBind.multiBindFloat([10.3, 12.6]);
  valueBind.multiBindInt([219, 218]);
  valueBind.multiBindFloat([0.31, 0.33]);
  cursor.stmtBindParamBatch(valueBind.getMultiBindArr());
  cursor.stmtAddBatch();

  // execute
  cursor.stmtExecute();
  cursor.stmtClose();
}
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/multi_bind_example.js"},`view source code`)),(0,esm/* mdx */.kt)("admonition",{"type":"info"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`Multiple row binding is better in performance than single row binding, but it can only be used with `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),` statement while single row binding can be used for other SQL statements besides `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),`.`)));};_js_stmt_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/03-insert-data/_cs_sql.mdx
var _cs_sql = __webpack_require__(2917);
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_cs_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _cs_stmt_frontMatter={};const _cs_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _cs_stmt_toc=[];const _cs_stmt_layoutProps={toc: _cs_stmt_toc};const _cs_stmt_MDXLayout="wrapper";function _cs_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_cs_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_cs_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/stmtInsert/Program.cs"},`view source code`)));};_cs_stmt_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_c_sql.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _c_sql_frontMatter={};const _c_sql_contentTitle=(/* unused pure expression or super */ null && (undefined));const _c_sql_toc=[];const _c_sql_layoutProps={toc: _c_sql_toc};const _c_sql_MDXLayout="wrapper";function _c_sql_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_c_sql_MDXLayout,(0,esm_extends/* default */.Z)({},_c_sql_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`// compile with
// gcc -o insert_example insert_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"


/**
 * @brief execute sql and print affected rows.
 * 
 * @param taos 
 * @param sql 
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("Error code: %d; Message: %s\\n", code, taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  int affectedRows = taos_affected_rows(res);
  printf("affected rows %d\\n", affectedRows);
  taos_free_result(res);
}



int main() {
   TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "CREATE DATABASE power");
  executeSQL(taos, "USE power");
  executeSQL(taos, "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
  executeSQL(taos, "INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)"
                "d1002 USING meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)"
                "d1003 USING meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)"
                "d1004 USING meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)");
  taos_close(taos);
  taos_cleanup();
}

// output:
// affected rows 0
// affected rows 0
// affected rows 0
// affected rows 8

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/insert_example.c"},`view source code`)));};_c_sql_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_c_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _c_stmt_frontMatter={};const _c_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _c_stmt_toc=[];const _c_stmt_layoutProps={toc: _c_stmt_toc};const _c_stmt_MDXLayout="wrapper";function _c_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_c_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_c_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c","metastring":"title=Single Row Binding","title":"Single","Row":true,"Binding":true},`// compile with
// gcc -o stmt_example stmt_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

/**
 * @brief execute sql only.
 * 
 * @param taos 
 * @param sql 
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("%s\\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 * 
 * @param stmt 
 * @param code 
 * @param msg 
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char* msg) {
  if (code != 0) {
    printf("%s. error: %s\\n", msg, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

typedef struct {
  int64_t ts;
  float current;
  int voltage;
  float phase;
} Row;

/**
 * @brief insert data using stmt API
 * 
 * @param taos 
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");
  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  char* location = "California.SanFrancisco";
  int groupId = 2;
  tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[0].buffer_length = strlen(location);
  tags[0].length = &tags[0].buffer_length;
  tags[0].buffer = location;
  tags[0].is_null = NULL;
  
  tags[1].buffer_type = TSDB_DATA_TYPE_INT;
  tags[1].buffer_length = sizeof(int);
  tags[1].length = &tags[1].buffer_length;
  tags[1].buffer = &groupId;
  tags[1].is_null = NULL;

  code = taos_stmt_set_tbname_tags(stmt, "d1001", tags);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

  // insert two rows
  Row rows[2] = {
    {1648432611249, 10.3, 219, 0.31},
    {1648432611749, 12.6, 218, 0.33},
  };

  TAOS_MULTI_BIND values[4];
  values[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  values[0].buffer_length = sizeof(int64_t);
  values[0].length = &values[0].buffer_length;
  values[0].is_null = NULL;

  values[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  values[1].buffer_length = sizeof(float);
  values[1].length = &values[1].buffer_length;
  values[1].is_null = NULL;

  values[2].buffer_type = TSDB_DATA_TYPE_INT;
  values[2].buffer_length = sizeof(int);
  values[2].length = &values[2].buffer_length;
  values[2].is_null = NULL;

  values[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  values[3].buffer_length = sizeof(float);
  values[3].length = &values[3].buffer_length;
  values[3].is_null = NULL;

  for (int i = 0; i < 2; ++i) {
    values[0].buffer = &rows[i].ts;
    values[1].buffer = &rows[i].current;
    values[2].buffer = &rows[i].voltage;
    values[3].buffer = &rows[i].phase;
    code = taos_stmt_bind_param(stmt, values); // bind param
    checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param");
    code = taos_stmt_add_batch(stmt); // add batch
    checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
  }
  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");
  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\\n", affectedRows);
  // close
  taos_stmt_close(stmt);
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "CREATE DATABASE power");
  executeSQL(taos, "USE power");
  executeSQL(taos, "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}


// output:
// successfully inserted 2 rows

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/stmt_example.c"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c","metastring":"title=Multiple Row Binding 72:117","title":"Multiple","Row":true,"Binding":true,"72:117":true},`// compile with
// gcc -o multi_bind_example multi_bind_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

/**
 * @brief execute sql only and ignore result set
 *
 * @param taos
 * @param sql
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("%s\\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg) {
  if (code != 0) {
    printf("%s. error: %s\\n", msg, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");
  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  char     *location = "California.SanFrancisco";
  int       groupId = 2;
  tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[0].buffer_length = strlen(location);
  tags[0].length = &tags[0].buffer_length;
  tags[0].buffer = location;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_INT;
  tags[1].buffer_length = sizeof(int);
  tags[1].length = &tags[1].buffer_length;
  tags[1].buffer = &groupId;
  tags[1].is_null = NULL;

  code = taos_stmt_set_tbname_tags(stmt, "d1001", tags);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

  // highlight-start
  // insert two rows with multi binds
  TAOS_MULTI_BIND params[4];
  // values to bind
  int64_t ts[] = {1648432611249, 1648432611749};
  float   current[] = {10.3, 12.6};
  int     voltage[] = {219, 218};
  float   phase[] = {0.31, 0.33};
  // is_null array
  char is_null[2] = {0};
  // length array
  int32_t int64Len[2] = {sizeof(int64_t)};
  int32_t floatLen[2] = {sizeof(float)};
  int32_t intLen[2] = {sizeof(int)};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(int64_t);
  params[0].buffer = ts;
  params[0].length = int64Len;
  params[0].is_null = is_null;
  params[0].num = 2;

  params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[1].buffer_length = sizeof(float);
  params[1].buffer = current;
  params[1].length = floatLen;
  params[1].is_null = is_null;
  params[1].num = 2;

  params[2].buffer_type = TSDB_DATA_TYPE_INT;
  params[2].buffer_length = sizeof(int);
  params[2].buffer = voltage;
  params[2].length = intLen;
  params[2].is_null = is_null;
  params[2].num = 2;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(float);
  params[3].buffer = phase;
  params[3].length = floatLen;
  params[3].is_null = is_null;
  params[3].num = 2;

  code = taos_stmt_bind_param_batch(stmt, params); // bind batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param_batch");
  code = taos_stmt_add_batch(stmt);  // add batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
  // highlight-end
  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");
  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\\n", affectedRows);
  // close
  taos_stmt_close(stmt);
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS power");
  executeSQL(taos, "CREATE DATABASE power");
  executeSQL(taos, "USE power");
  executeSQL(taos,
             "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), "
             "groupId INT)");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}

// output:
// successfully inserted 2 rows

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/multi_bind_example.c"},`view source code`)));};_c_stmt_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_php_sql.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _php_sql_frontMatter={};const _php_sql_contentTitle=(/* unused pure expression or super */ null && (undefined));const _php_sql_toc=[];const _php_sql_layoutProps={toc: _php_sql_toc};const _php_sql_MDXLayout="wrapper";function _php_sql_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_php_sql_MDXLayout,(0,esm_extends/* default */.Z)({},_php_sql_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-php"},`<?php

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

    // insert
    $connection->query('CREATE DATABASE if not exists power');
    $connection->query('CREATE STABLE if not exists meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)');
    $resource = $connection->query(<<<'SQL'
    INSERT INTO power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS(California.LosAngeles, 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS(California.LosAngeles, 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)
    SQL);

    // get affected rows
    var_dump($resource->affectedRows());
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/php/insert.php"},`view source code`)));};_php_sql_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_php_stmt.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _php_stmt_frontMatter={};const _php_stmt_contentTitle=(/* unused pure expression or super */ null && (undefined));const _php_stmt_toc=[];const _php_stmt_layoutProps={toc: _php_stmt_toc};const _php_stmt_MDXLayout="wrapper";function _php_stmt_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_php_stmt_MDXLayout,(0,esm_extends/* default */.Z)({},_php_stmt_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-php"},`<?php

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

    // insert
    $connection->query('CREATE DATABASE if not exists power');
    $connection->query('CREATE STABLE if not exists meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)');
    $stmt = $connection->prepare('INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)');

    // set table name and tags
    $stmt->setTableNameTags('d1001', [
        // same format as parameter binding
        [TDengine\\TSDB_DATA_TYPE_BINARY, 'California.SanFrancisco'],
        [TDengine\\TSDB_DATA_TYPE_INT, 2],
    ]);

    $stmt->bindParams([
        [TDengine\\TSDB_DATA_TYPE_TIMESTAMP, 1648432611249],
        [TDengine\\TSDB_DATA_TYPE_FLOAT, 10.3],
        [TDengine\\TSDB_DATA_TYPE_INT, 219],
        [TDengine\\TSDB_DATA_TYPE_FLOAT, 0.31],
    ]);
    $stmt->bindParams([
        [TDengine\\TSDB_DATA_TYPE_TIMESTAMP, 1648432611749],
        [TDengine\\TSDB_DATA_TYPE_FLOAT, 12.6],
        [TDengine\\TSDB_DATA_TYPE_INT, 218],
        [TDengine\\TSDB_DATA_TYPE_FLOAT, 0.33],
    ]);
    $resource = $stmt->execute();

    // get affected rows
    var_dump($resource->affectedRows());
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/php/insert_stmt.php"},`view source code`)));};_php_stmt_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/01-sql-writing.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _01_sql_writing_frontMatter={title:'Insert Using SQL',description:'This document describes how to insert data into TDengine using SQL.'};const _01_sql_writing_contentTitle=undefined;const metadata={"unversionedId":"develop/insert-data/sql-writing","id":"develop/insert-data/sql-writing","title":"Insert Using SQL","description":"This document describes how to insert data into TDengine using SQL.","source":"@site/docs/07-develop/03-insert-data/01-sql-writing.mdx","sourceDirName":"07-develop/03-insert-data","slug":"/develop/insert-data/sql-writing","permalink":"/docs-en/develop/insert-data/sql-writing","draft":false,"tags":[],"version":"current","sidebarPosition":1,"frontMatter":{"title":"Insert Using SQL","description":"This document describes how to insert data into TDengine using SQL."},"sidebar":"defaultSidebar","previous":{"title":"Insert Data","permalink":"/docs-en/develop/insert-data/"},"next":{"title":"Write from Kafka","permalink":"/docs-en/develop/insert-data/kafka-writting"}};const assets={};const _01_sql_writing_toc=[{value:'Introduction',id:'introduction',level:2},{value:'Insert Single Row',id:'insert-single-row',level:3},{value:'Insert Multiple Rows',id:'insert-multiple-rows',level:3},{value:'Insert into Multiple Tables',id:'insert-into-multiple-tables',level:3},{value:'Sample program',id:'sample-program',level:2},{value:'Insert Using SQL',id:'insert-using-sql',level:3},{value:'Insert with Parameter Binding',id:'insert-with-parameter-binding',level:3}];const _01_sql_writing_layoutProps={toc: _01_sql_writing_toc};const _01_sql_writing_MDXLayout="wrapper";function _01_sql_writing_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_01_sql_writing_MDXLayout,(0,esm_extends/* default */.Z)({},_01_sql_writing_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h2",{"id":"introduction"},`Introduction`),(0,esm/* mdx */.kt)("p",null,`Application programs can execute `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),` statement through connectors to insert rows. The TDengine CLI can also be used to manually insert data.`),(0,esm/* mdx */.kt)("h3",{"id":"insert-single-row"},`Insert Single Row`),(0,esm/* mdx */.kt)("p",null,`The below SQL statement is used to insert one row into table "d1001".`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`INSERT INTO d1001 VALUES (ts1, 10.3, 219, 0.31);
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts1`),` is Unix timestamp, the timestamps which is larger than the difference between current time and KEEP in config is only allowed. For further detail, refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/taos-sql/insert"},`TDengine SQL insert timestamp section`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"insert-multiple-rows"},`Insert Multiple Rows`),(0,esm/* mdx */.kt)("p",null,`Multiple rows can be inserted in a single SQL statement. The example below inserts 2 rows into table "d1001".`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`INSERT INTO d1001 VALUES (ts2, 10.2, 220, 0.23) (ts2, 10.3, 218, 0.25);
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts1`),` and `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts2`),` is Unix timestamp, the timestamps which is larger than the difference between current time and KEEP in config is only allowed. For further detail, refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/taos-sql/insert"},`TDengine SQL insert timestamp section`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"insert-into-multiple-tables"},`Insert into Multiple Tables`),(0,esm/* mdx */.kt)("p",null,`Data can be inserted into multiple tables in the same SQL statement. The example below inserts 2 rows into table "d1001" and 1 row into table "d1002".`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`INSERT INTO d1001 VALUES (ts1, 10.3, 219, 0.31) (ts2, 12.6, 218, 0.33) d1002 VALUES (ts3, 12.3, 221, 0.31);
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts1`),`, `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts2`),` and `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`ts3`),` is Unix timestamp, the timestamps which is larger than the difference between current time and KEEP in config is only allowed. For further detail, refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/taos-sql/insert"},`TDengine SQL insert timestamp section`),`.`),(0,esm/* mdx */.kt)("p",null,`For more details about `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`INSERT`),` please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/taos-sql/insert"},`INSERT`),`.`),(0,esm/* mdx */.kt)("admonition",{"type":"info"},(0,esm/* mdx */.kt)("ul",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Inserting in batches can improve performance. The higher the batch size, the better the performance. Please note that a single row can't exceed 48K bytes and each SQL statement can't exceed 1MB.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`Inserting with multiple threads can also improve performance. However, at a certain point, increasing the number of threads no longer offers any benefit and can even decrease performance due to the overhead involved in frequent thread switching. The optimal number of threads for a system depends on the processing capabilities and configuration of the server, the configuration of the database, the data schema, and the batch size for writing data. In general, more powerful clients and servers can support higher numbers of concurrently writing threads. Given a sufficiently powerful server, a higher number of vgroups for a database also increases the number of concurrent writes. Finally, a simpler data schema enables more concurrent writes as well.`))),(0,esm/* mdx */.kt)("admonition",{"type":"warning"},(0,esm/* mdx */.kt)("ul",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ul"},`If the timestamp of a new record already exists in a table, columns with new data for that timestamp replace old data with new data, while columns without new data are not affected.`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`The timestamp to be inserted must be newer than the timestamp of subtracting current time by the parameter `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`KEEP`),`. If `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`KEEP`),` is set to 3650 days, then the data older than 3650 days ago can't be inserted. The timestamp to be inserted cannot be newer than the timestamp of current time plus parameter `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`DURATION`),`. If `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`DURATION`),` is set to 2, the data newer than 2 days later can't be inserted.`))),(0,esm/* mdx */.kt)("h2",{"id":"sample-program"},`Sample program`),(0,esm/* mdx */.kt)("h3",{"id":"insert-using-sql"},`Insert Using SQL`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"JavaSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_py_sql_MDXContent,{mdxType:"PySQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_go_sql/* default */.ZP,{mdxType:"GoSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_rust_sql/* default */.ZP,{mdxType:"RustSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"nodejs",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_js_sql/* default */.ZP,{mdxType:"NodeSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_cs_sql/* default */.ZP,{mdxType:"CsSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_c_sql_MDXContent,{mdxType:"CSQL"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_php_sql_MDXContent,{mdxType:"PhpSQL"}))),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("ol",{parentName:"admonition"},(0,esm/* mdx */.kt)("li",{parentName:"ol"},`With either native connection or REST connection, the above samples can work well.`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Please note that `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`use db`),` can't be used with a REST connection because REST connections are stateless, so in the samples `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`dbName.tbName`),` is used to specify the table name.`))),(0,esm/* mdx */.kt)("h3",{"id":"insert-with-parameter-binding"},`Insert with Parameter Binding`),(0,esm/* mdx */.kt)("p",null,`TDengine also provides API support for parameter binding. Similar to MySQL, only `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`?`),` can be used in these APIs to represent the parameters to bind. This avoids the resource consumption of SQL syntax parsing when writing data through the parameter binding interface, thus significantly improving write performance in most cases.`),(0,esm/* mdx */.kt)("p",null,`Parameter binding is available only with native connection.`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_java_stmt_MDXContent,{mdxType:"JavaStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_py_stmt_MDXContent,{mdxType:"PyStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_go_stmt_MDXContent,{mdxType:"GoStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_rust_stmt/* default */.ZP,{mdxType:"RustStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"nodejs",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_js_stmt_MDXContent,{mdxType:"NodeStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_cs_stmt_MDXContent,{mdxType:"CsStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_c_stmt_MDXContent,{mdxType:"CStmt"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_php_stmt_MDXContent,{mdxType:"PhpStmt"}))));};_01_sql_writing_MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2917:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineDriver;


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

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/sqlInsert/Program.cs"},`view source code`)));};MDXContent.isMDXComponent=true;

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

/***/ 9301:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-js"},`const taos = require("@tdengine/client");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();
try {
  cursor.execute("CREATE DATABASE power");
  cursor.execute("USE power");
  cursor.execute(
    "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
  );
  var sql = \`INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)\`;
  cursor.execute(sql,{'quiet':false});
} finally {
  cursor.close();
  conn.close();
}

// run with: node insert_example.js
// output:
// Successfully connected to TDengine
// Query OK, 0 row(s) affected (0.00509570s)
// Query OK, 0 row(s) affected (0.00130880s)
// Query OK, 0 row(s) affected (0.00467900s)
// Query OK, 8 row(s) affected (0.04043550s)
// Connection is closed

`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/insert_example.js"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 8958:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

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
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-rust"},`use taos::*;

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

/***/ })

}]);