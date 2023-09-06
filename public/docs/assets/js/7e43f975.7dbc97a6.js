"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[7970],{

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

/***/ 9357:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _07_tmq_contentTitle),
  "default": () => (/* binding */ _07_tmq_MDXContent),
  "frontMatter": () => (/* binding */ _07_tmq_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _07_tmq_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/_sub_java.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscribeDemo {
    private static final String TOPIC = "tmq_topic";
    private static final String DB_NAME = "meters";
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                shutdown.set(true);
            }
        }, 3_000);
        try {
            // prepare
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";
            Connection connection = DriverManager.getConnection(jdbcUrl);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop topic if exists " + TOPIC);
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.executeUpdate("create database " + DB_NAME + " wal_retention_period 3600");
                statement.executeUpdate("use " + DB_NAME);
                statement.executeUpdate(
                        "CREATE TABLE \`meters\` (\`ts\` TIMESTAMP, \`current\` FLOAT, \`voltage\` INT) TAGS (\`groupid\` INT, \`location\` BINARY(24))");
                statement.executeUpdate("CREATE TABLE \`d0\` USING \`meters\` TAGS(0, 'California.LosAngles')");
                statement.executeUpdate("INSERT INTO \`d0\` values(now - 10s, 0.32, 116)");
                statement.executeUpdate("INSERT INTO \`d0\` values(now - 8s, NULL, NULL)");
                statement.executeUpdate(
                        "INSERT INTO \`d1\` USING \`meters\` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119)");
                statement.executeUpdate(
                        "INSERT INTO \`d1\` values (now-8s, 10, 120) (now - 6s, 10, 119) (now - 4s, 11.2, 118)");
                // create topic
                statement.executeUpdate("create topic " + TOPIC + " as select * from meters");
            }

            // create consumer
            Properties properties = new Properties();
            properties.getProperty(TMQConstants.CONNECT_TYPE, "jni");
            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6030");
            properties.setProperty(TMQConstants.CONNECT_USER, "root");
            properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
            properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
            properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
            properties.setProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "1000");
            properties.setProperty(TMQConstants.GROUP_ID, "test1");
            properties.setProperty(TMQConstants.CLIENT_ID, "1");
            properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                    "com.taos.example.MetersDeserializer");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER_ENCODING, "UTF-8");
            properties.setProperty(TMQConstants.EXPERIMENTAL_SNAPSHOT_ENABLE, "true");

            // poll data
            try (TaosConsumer<Meters> consumer = new TaosConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                while (!shutdown.get()) {
                    ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Meters> r : meters) {
                        Meters meter = r.value();
                        System.out.println(meter);
                    }
                }
                consumer.unsubscribe();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        timer.cancel();
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/SubscribeDemo.java"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.tmq.ReferenceDeserializer;

public class MetersDeserializer extends ReferenceDeserializer<Meters> {
}
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/MetersDeserializer.java"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import java.sql.Timestamp;

public class Meters {
    private Timestamp ts;
    private float current;
    private int voltage;
    private int groupid;
    private String location;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getCurrent() {
        return current;
    }

    public void setCurrent(float current) {
        this.current = current;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public int getGroupid() {
        return groupid;
    }

    public void setGroupid(int groupid) {
        this.groupid = groupid;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Meters{" +
                "ts=" + ts +
                ", current=" + current +
                ", voltage=" + voltage +
                ", groupid=" + groupid +
                ", location='" + location + '\\'' +
                '}';
    }
}



[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/Meters.java)
`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_java_ws.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_java_ws_frontMatter={};const _sub_java_ws_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_java_ws_toc=[];const _sub_java_ws_layoutProps={toc: _sub_java_ws_toc};const _sub_java_ws_MDXLayout="wrapper";function _sub_java_ws_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_java_ws_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_java_ws_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebsocketSubscribeDemo {
    private static final String TOPIC = "tmq_topic_ws";
    private static final String DB_NAME = "meters_ws";
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                shutdown.set(true);
            }
        }, 3_000);
        try {
            // prepare
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            String jdbcUrl = "jdbc:TAOS-RS://127.0.0.1:6041/?user=root&password=taosdata&batchfetch=true";
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop topic if exists " + TOPIC);
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.executeUpdate("create database " + DB_NAME + " wal_retention_period 3600");
                statement.executeUpdate("use " + DB_NAME);
                statement.executeUpdate(
                        "CREATE TABLE \`meters\` (\`ts\` TIMESTAMP, \`current\` FLOAT, \`voltage\` INT) TAGS (\`groupid\` INT, \`location\` BINARY(24))");
                statement.executeUpdate("CREATE TABLE \`d0\` USING \`meters\` TAGS(0, 'California.LosAngles')");
                statement.executeUpdate("INSERT INTO \`d0\` values(now - 10s, 0.32, 116)");
                statement.executeUpdate("INSERT INTO \`d0\` values(now - 8s, NULL, NULL)");
                statement.executeUpdate(
                        "INSERT INTO \`d1\` USING \`meters\` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119)");
                statement.executeUpdate(
                        "INSERT INTO \`d1\` values (now-8s, 10, 120) (now - 6s, 10, 119) (now - 4s, 11.2, 118)");
                // create topic
                statement.executeUpdate("create topic " + TOPIC + " as select * from meters");
            }

            // create consumer
            Properties properties = new Properties();
            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
            properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
            properties.setProperty(TMQConstants.CONNECT_USER, "root");
            properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
            properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
            properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
            properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
            properties.setProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "1000");
            properties.setProperty(TMQConstants.GROUP_ID, "test2");
            properties.setProperty(TMQConstants.CLIENT_ID, "1");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                    "com.taos.example.MetersDeserializer");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER_ENCODING, "UTF-8");
            properties.setProperty(TMQConstants.EXPERIMENTAL_SNAPSHOT_ENABLE, "true");

            // poll data
            try (TaosConsumer<Meters> consumer = new TaosConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                while (!shutdown.get()) {
                    ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Meters> r : meters) {
                        Meters meter = (Meters) r.value();
                        System.out.println(meter);
                    }
                }
                consumer.unsubscribe();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        timer.cancel();
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WebsocketSubscribeDemo.java"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import com.taosdata.jdbc.tmq.ReferenceDeserializer;

public class MetersDeserializer extends ReferenceDeserializer<Meters> {
}
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/MetersDeserializer.java"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`package com.taos.example;

import java.sql.Timestamp;

public class Meters {
    private Timestamp ts;
    private float current;
    private int voltage;
    private int groupid;
    private String location;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getCurrent() {
        return current;
    }

    public void setCurrent(float current) {
        this.current = current;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public int getGroupid() {
        return groupid;
    }

    public void setGroupid(int groupid) {
        this.groupid = groupid;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Meters{" +
                "ts=" + ts +
                ", current=" + current +
                ", voltage=" + voltage +
                ", groupid=" + groupid +
                ", location='" + location + '\\'' +
                '}';
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/Meters.java"},`查看源码`)));};_sub_java_ws_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_python.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_python_frontMatter={};const _sub_python_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_python_toc=[];const _sub_python_layoutProps={toc: _sub_python_toc};const _sub_python_MDXLayout="wrapper";function _sub_python_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_python_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_python_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`from taos.tmq import Consumer
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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/tmq_example.py"},`查看源码`)));};_sub_python_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_go.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_go_frontMatter={};const _sub_go_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_go_toc=[];const _sub_go_layoutProps={toc: _sub_go_toc};const _sub_go_MDXLayout="wrapper";function _sub_go_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_go_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_go_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`package main

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
    _, err = db.Exec("create database if not exists example_tmq wal_retention_period 3600")
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
        ev := consumer.Poll(0)
        if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
                fmt.Println(e.String())
            case tmqcommon.Error:
                fmt.Fprintf(os.Stderr, "%% Error: %v: %v\\n", e.Code(), e)
                panic(e)
            }
            consumer.Commit()
        }
    }
    err = consumer.Unsubscribe()
    if err != nil {
        panic(err)
    }
    err = consumer.Close()
    if err != nil {
        panic(err)
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/sub/main.go"},`查看源码`)));};_sub_go_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_rust.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_rust_frontMatter={};const _sub_rust_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_rust_toc=[];const _sub_rust_layoutProps={toc: _sub_rust_toc};const _sub_rust_MDXLayout="wrapper";function _sub_rust_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_rust_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_rust_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`use std::time::Duration;

use chrono::{DateTime, Local};
use taos::*;

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
}

async fn prepare(taos: Taos) -> anyhow::Result<()> {
    let inserted = taos.exec_many([
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
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build()?;
    let db = "tmq";

    // prepare database
    taos.exec_many([
        format!("DROP TOPIC IF EXISTS tmq_meters"),
        format!("DROP DATABASE IF EXISTS \`{db}\`"),
        format!("CREATE DATABASE \`{db}\` WAL_RETENTION_PERIOD 3600"),
        format!("USE \`{db}\`"),
        // create super table
        format!("CREATE TABLE \`meters\` (\`ts\` TIMESTAMP, \`current\` FLOAT, \`voltage\` INT, \`phase\` FLOAT) TAGS (\`groupid\` INT, \`location\` BINARY(24))"),
        // create topic for subscription
        format!("CREATE TOPIC tmq_meters AS SELECT * FROM \`meters\`")
    ])
    .await?;

    let task = tokio::spawn(prepare(taos));

    tokio::time::sleep(Duration::from_secs(1)).await;

    // subscribe
    let tmq = TmqBuilder::from_dsn("taos://localhost:6030/?group.id=test")?;

    let mut consumer = tmq.build()?;
    consumer.subscribe(["tmq_meters"]).await?;

    consumer
        .stream()
        .try_for_each(|(offset, message)| async {
            let topic = offset.topic();
            // the vgroup id, like partition id in kafka.
            let vgroup_id = offset.vgroup_id();
            println!("* in vgroup id {vgroup_id} of topic {topic}\\n");

            if let Some(data) = message.into_data() {
                while let Some(block) = data.fetch_raw_block().await? {
                    let records: Vec<Record> = block.deserialize().try_collect()?;
                    println!("** read {} records: {:#?}\\n", records.len(), records);
                }
            }
            consumer.commit(offset).await?;
            Ok(())
        })
        .await?;

    consumer.unsubscribe().await;

    task.await??;

    Ok(())
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/subscribe_demo.rs"},`查看源码`)));};_sub_rust_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_node.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_node_frontMatter={};const _sub_node_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_node_toc=[];const _sub_node_layoutProps={toc: _sub_node_toc};const _sub_node_MDXLayout="wrapper";function _sub_node_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_node_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_node_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`const taos = require("@tdengine/client");

const conn = taos.connect({ host: "localhost", database: "power" });
var cursor = conn.cursor();

function runConsumer() {

    // create topic 
    cursor.execute("create topic topic_name_example as select * from meters");

    let consumer = taos.consumer({
        'group.id': 'tg2',
        'td.connect.user': 'root',
        'td.connect.pass': 'taosdata',
        'msg.with.table.name': 'true',
        'enable.auto.commit': 'true'
    });
    
    // subscribe the topic just created.
    consumer.subscribe("topic_name_example");

    // get subscribe topic list
    let topicList = consumer.subscription();
    console.log(topicList);

    for (let i = 0; i < 5; i++) {
        let msg = consumer.consume(100);
        console.log(msg.topicPartition);
        console.log(msg.block);
        console.log(msg.fields)
        consumer.commit(msg);
        console.log(\`=======consumer \${i} done\`)
    }

    consumer.unsubscribe();
    consumer.close();

    // drop topic
    cursor.execute("drop topic topic_name_example");
}


try {
    runConsumer();
} finally {

    setTimeout(() => {
        cursor.close();
        conn.close();
    }, 2000);
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/subscribe_demo.js"},`查看源码`)));};_sub_node_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_cs.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_cs_frontMatter={};const _sub_cs_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_cs_toc=[];const _sub_cs_layoutProps={toc: _sub_cs_toc};const _sub_cs_MDXLayout="wrapper";function _sub_cs_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_cs_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_cs_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`﻿using System;
using TDengineTMQ;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace TMQExample
{
    internal class SubscribeDemo
    {
        static void Main(string[] args)
        {
            IntPtr conn = GetConnection();
            string topic = "topic_example";
            //create topic 
            IntPtr res = TDengine.Query(conn, $"create topic if not exists {topic} as select * from meters");

            if (TDengine.ErrorNo(res) != 0 )
            {
                throw new Exception($"create topic failed, reason:{TDengine.Error(res)}");
            }

            var cfg = new ConsumerConfig
            {
                GourpId = "group_1",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
            };

            // create consumer 
            var consumer = new ConsumerBuilder(cfg)
                .Build();

            // subscribe
            consumer.Subscribe(topic);

            // consume 
            for (int i = 0; i < 5; i++)
            {
                var consumeRes = consumer.Consume(300);
                // print consumeResult
                foreach (KeyValuePair<TopicPartition, TaosResult> kv in consumeRes.Message)
                {
                    Console.WriteLine("topic partitions:\\n{0}", kv.Key.ToString());

                    kv.Value.Metas.ForEach(meta =>
                    {
                        Console.Write("{0} {1}({2}) \\t|", meta.name, meta.TypeName(), meta.size);
                    });
                    Console.WriteLine("");
                    kv.Value.Datas.ForEach(data =>
                    {
                        Console.WriteLine(data.ToString());
                    });
                }

                consumer.Commit(consumeRes);
                Console.WriteLine("\\n================ {0} done ", i);

            }

            // retrieve topic list
            List<string> topics = consumer.Subscription();
            topics.ForEach(t => Console.WriteLine("topic name:{0}", t));

            // unsubscribe
            consumer.Unsubscribe();

            // close consumer after use.Otherwise will lead memory leak.
            consumer.Close();
            TDengine.Close(conn);

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/subscribe/Program.cs"},`查看源码`)));};_sub_cs_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/_sub_c.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _sub_c_frontMatter={};const _sub_c_contentTitle=(/* unused pure expression or super */ null && (undefined));const _sub_c_toc=[];const _sub_c_layoutProps={toc: _sub_c_toc};const _sub_c_MDXLayout="wrapper";function _sub_c_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_sub_c_MDXLayout,(0,esm_extends/* default */.Z)({},_sub_c_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"

static int  running = 1;
static char dbName[64] = "tmqdb";
static char stbName[64] = "stb";
static char topicName[64] = "topicname";

static int32_t msg_process(TAOS_RES* msg) {
  char    buf[1024];
  int32_t rows = 0;

  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  printf("topic: %s\\n", topicName);
  printf("db: %s\\n", dbName);
  printf("vgroup id: %d\\n", vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    int32_t*    length = taos_fetch_lengths(msg);
    int32_t     precision = taos_result_precision(msg);
    rows++;
    taos_print_row(buf, row, fields, numOfFields);
    printf("row content: %s\\n", buf);
  }

  return rows;
}

static int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes;
  // drop database if exists
  printf("create database\\n");
  pRes = taos_query(pConn, "drop database if exists tmqdb");
  if (taos_errno(pRes) != 0) {
    printf("error in drop tmqdb, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // create database
  pRes = taos_query(pConn, "create database tmqdb wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create tmqdb, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // create super table
  printf("create super table\\n");
  pRes = taos_query(
      pConn, "create table tmqdb.stb (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table stb, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // create sub tables
  printf("create sub tables\\n");
  pRes = taos_query(pConn, "create table tmqdb.ctb0 using tmqdb.stb tags(0, 'subtable0')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb0, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb1 using tmqdb.stb tags(1, 'subtable1')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb1, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb2 using tmqdb.stb tags(2, 'subtable2')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb2, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb3 using tmqdb.stb tags(3, 'subtable3')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb3, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // insert data
  printf("insert data into sub tables\\n");
  pRes = taos_query(pConn, "insert into tmqdb.ctb0 values(now, 0, 0, 'a0')(now+1s, 0, 0, 'a00')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb1 values(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb2 values(now, 2, 2, 'a1')(now+1s, 22, 22, 'a22')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb3 values(now, 3, 3, 'a1')(now+1s, 33, 33, 'a33')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

int32_t create_topic() {
  printf("create topic\\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  pRes = taos_query(pConn, "use tmqdb");
  if (taos_errno(pRes) != 0) {
    printf("error in use tmqdb, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic topicname as select ts, c1, c2, c3, tbname from tmqdb.stb where c1 > 1");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topicname, reason:%s\\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("tmq_commit_cb_print() code: %d, tmq: %p, param: %p\\n", code, tmq, param);
}

tmq_t* build_consumer() {
  tmq_conf_res_t code;
  tmq_conf_t*    conf = tmq_conf_new();

  code = tmq_conf_set(conf, "enable.auto.commit", "true");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "group.id", "cgrpName");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "client.id", "user defined name");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "td.connect.user", "root");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "td.connect.pass", "taosdata");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topicList = tmq_list_new();
  int32_t     code = tmq_list_append(topicList, "topicname");
  if (code) {
    return NULL;
  }
  return topicList;
}

void basic_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeout = 5000;
  while (running) {
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;
      totalRows += msg_process(tmqmsg);
      taos_free_result(tmqmsg);
    } else {
      break;
    }
  }

  fprintf(stderr, "%d msg consumed, include %d rows\\n", msgCnt, totalRows);
}

int main(int argc, char* argv[]) {
  int32_t code;

  if (init_env() < 0) {
    return -1;
  }

  if (create_topic() < 0) {
    return -1;
  }

  tmq_t* tmq = build_consumer();
  if (NULL == tmq) {
    fprintf(stderr, "%% build_consumer() fail!\\n");
    return -1;
  }

  tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    return -1;
  }

  if ((code = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr, "%% Failed to tmq_subscribe(): %s\\n", tmq_err2str(code));
  }
  tmq_list_destroy(topic_list);

  basic_consume_loop(tmq);

  code = tmq_consumer_close(tmq);
  if (code) {
    fprintf(stderr, "%% Failed to close consumer: %s\\n", tmq_err2str(code));
  } else {
    fprintf(stderr, "%% Consumer closed\\n");
  }

  return 0;
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/tmq_example.c"},`查看源码`)));};_sub_c_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/07-tmq.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _07_tmq_frontMatter={sidebar_label:'数据订阅',description:'数据订阅与推送服务。写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。',title:'数据订阅'};const _07_tmq_contentTitle=undefined;const metadata={"unversionedId":"develop/tmq","id":"develop/tmq","title":"数据订阅","description":"数据订阅与推送服务。写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。","source":"@site/docs/07-develop/07-tmq.mdx","sourceDirName":"07-develop","slug":"/develop/tmq","permalink":"/docs/develop/tmq","draft":false,"tags":[],"version":"current","sidebarPosition":7,"frontMatter":{"sidebar_label":"数据订阅","description":"数据订阅与推送服务。写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。","title":"数据订阅"},"sidebar":"defaultSidebar","previous":{"title":"流式计算","permalink":"/docs/develop/stream"},"next":{"title":"缓存","permalink":"/docs/develop/cache"}};const assets={};const _07_tmq_toc=[{value:'主要数据结构和 API',id:'主要数据结构和-api',level:2},{value:'写入数据',id:'写入数据',level:2},{value:'创建 <em>topic</em>',id:'创建-topic',level:2},{value:'列订阅',id:'列订阅',level:3},{value:'超级表订阅',id:'超级表订阅',level:3},{value:'数据库订阅',id:'数据库订阅',level:3},{value:'创建消费者 <em>consumer</em>',id:'创建消费者-consumer',level:2},{value:'订阅 <em>topics</em>',id:'订阅-topics',level:2},{value:'消费',id:'消费',level:2},{value:'结束消费',id:'结束消费',level:2},{value:'删除 <em>topic</em>',id:'删除-topic',level:2},{value:'状态查看',id:'状态查看',level:2},{value:'示例代码',id:'示例代码',level:2}];const _07_tmq_layoutProps={toc: _07_tmq_toc};const _07_tmq_MDXLayout="wrapper";function _07_tmq_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_07_tmq_MDXLayout,(0,esm_extends/* default */.Z)({},_07_tmq_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`为了帮助应用实时获取写入 TDengine 的数据，或者以事件到达顺序处理数据，TDengine 提供了类似消息队列产品的数据订阅、消费接口。这样在很多场景下，采用 TDengine 的时序数据处理系统不再需要集成消息队列产品，比如 kafka, 从而简化系统设计的复杂度，降低运营维护成本。`),(0,esm/* mdx */.kt)("p",null,`与 kafka 一样，你需要定义 `,(0,esm/* mdx */.kt)("em",{parentName:"p"},`topic`),`, 但 TDengine 的 `,(0,esm/* mdx */.kt)("em",{parentName:"p"},`topic`),` 是基于一个已经存在的超级表、子表或普通表的查询条件，即一个 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`SELECT`),` 语句。你可以使用 SQL 对标签、表名、列、表达式等条件进行过滤，以及对数据进行标量函数与 UDF 计算（不包括数据聚合）。与其他消息队列软件相比，这是 TDengine 数据订阅功能的最大的优势，它提供了更大的灵活性，数据的颗粒度可以由应用随时调整，而且数据的过滤与预处理交给 TDengine，而不是应用完成，有效的减少传输的数据量与应用的复杂度。`),(0,esm/* mdx */.kt)("p",null,`消费者订阅 `,(0,esm/* mdx */.kt)("em",{parentName:"p"},`topic`),` 后，可以实时获得最新的数据。多个消费者可以组成一个消费者组 (consumer group), 一个消费者组里的多个消费者共享消费进度，便于多线程、分布式地消费数据，提高消费速度。但不同消费者组中的消费者即使消费同一个 topic, 并不共享消费进度。一个消费者可以订阅多个 topic。如果订阅的是超级表，数据可能会分布在多个不同的 vnode 上，也就是多个 shard 上，这样一个消费组里有多个消费者可以提高消费效率。TDengine 的消息队列提供了消息的 ACK 机制，在宕机、重启等复杂环境下确保 at least once 消费。`),(0,esm/* mdx */.kt)("p",null,`为了实现上述功能，TDengine 会为 WAL (Write-Ahead-Log) 文件自动创建索引以支持快速随机访问，并提供了灵活可配置的文件切换与保留机制：用户可以按需指定 WAL 文件保留的时间以及大小（详见 create database 语句）。通过以上方式将 WAL 改造成了一个保留事件到达顺序的、可持久化的存储引擎（但由于 TSDB 具有远比 WAL 更高的压缩率，我们不推荐保留太长时间，一般来说，不超过几天）。 对于以 topic 形式创建的查询，TDengine 将对接 WAL 而不是 TSDB 作为其存储引擎。在消费时，TDengine 根据当前消费进度从 WAL 直接读取数据，并使用统一的查询引擎实现过滤、变换等操作，将数据推送给消费者。`),(0,esm/* mdx */.kt)("p",null,`本文档不对消息队列本身的基础知识做介绍，如果需要了解，请自行搜索。`),(0,esm/* mdx */.kt)("p",null,`说明（以c接口为例）：`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`一个消费组消费同一个topic下的所有数据，不同消费组之间相互独立；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`一个消费组消费同一个topic所有的vgroup，消费组可由多个消费者组成，但一个vgroup仅被一个消费者消费，如果消费者数量超过了vgroup数量，多余的消费者不消费数据；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`在服务端每个vgroup仅保存一个offset，每个vgroup的offset是单调递增的，但不一定连续。各个vgroup的offset之间没有关联；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`每次poll服务端会返回一个结果block，该block属于一个vgroup，可能包含多个wal版本的数据，可以通过 tmq_get_vgroup_offset 接口获得是该block第一条记录的offset；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`一个消费组如果从未commit过offset，当其成员消费者重启重新拉取数据时，均从参数auto.offset.reset设定值开始消费；在一个消费者生命周期中，客户端本地记录了最近一次拉取数据的offset，不会拉取重复数据；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`消费者如果异常终止（没有调用tmq_close），需等约12秒后触发其所属消费组rebalance，该消费者在服务端状态变为LOST，约1天后该消费者自动被删除；正常退出，退出后就会删除消费者；新增消费者，需等约2秒触发rebalance，该消费者在服务端状态变为ready；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`消费组rebalance会对该组所有ready状态的消费者成员重新进行vgroup分配，消费者仅能对自己负责的vgroup进行assignment/seek/commit/poll操作；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`消费者可利用 tmq_position 获得当前消费的offset，并seek到指定offset，重新消费；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`seek将position指向指定offset，不执行commit操作，一旦seek成功，可poll拉取指定offset及以后的数据；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`seek 操作之前须调用 tmq_get_topic_assignment 接口获取该consumer的vgroup ID和offset范围。seek 操作会检测vgroup ID 和 offset是否合法，如非法将报错；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`tmq_get_vgroup_offset接口获取的是记录所在结果block块里的第一条数据的offset，当seek至该offset时，将消费到这个block里的全部数据。参见第四点；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`由于存在 WAL 过期删除机制，即使seek 操作成功，poll数据时有可能offset已失效。如果poll 的offset 小于 WAL 最小版本号，将会从WAL最小版本号消费；`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`数据订阅是从 WAL 消费数据，如果一些 WAL 文件被基于 WAL 保留策略删除，则已经删除的 WAL 文件中的数据就无法再消费到。需要根据业务需要在创建数据库时合理设置 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`WAL_RETENTION_PERIOD`),` 或 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`WAL_RETENTION_SIZE`),` ，并确保应用及时消费数据，这样才不会产生数据丢失的现象。数据订阅的行为与 Kafka 等广泛使用的消息队列类产品的行为相似；`)),(0,esm/* mdx */.kt)("h2",{"id":"主要数据结构和-api"},`主要数据结构和 API`),(0,esm/* mdx */.kt)("p",null,`不同语言下， TMQ 订阅相关的 API 及数据结构如下：`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"c",label:"C",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`    typedef struct tmq_t      tmq_t;
    typedef struct tmq_conf_t tmq_conf_t;
    typedef struct tmq_list_t tmq_list_t;

    typedef void(tmq_commit_cb(tmq_t *tmq, int32_t code, void *param));

    typedef enum tmq_conf_res_t {
    TMQ_CONF_UNKNOWN = -2,
    TMQ_CONF_INVALID = -1,
    TMQ_CONF_OK = 0,
} tmq_conf_res_t;

    typedef struct tmq_topic_assignment {
    int32_t vgId;
    int64_t currentOffset;
    int64_t begin;
    int64_t end;
} tmq_topic_assignment;

    DLL_EXPORT tmq_conf_t    *tmq_conf_new();
    DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
    DLL_EXPORT void           tmq_conf_destroy(tmq_conf_t *conf);
    DLL_EXPORT void           tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);

    DLL_EXPORT tmq_list_t *tmq_list_new();
    DLL_EXPORT int32_t     tmq_list_append(tmq_list_t *, const char *);
    DLL_EXPORT void        tmq_list_destroy(tmq_list_t *);
    DLL_EXPORT int32_t     tmq_list_get_size(const tmq_list_t *);
    DLL_EXPORT char      **tmq_list_to_c_array(const tmq_list_t *);

    DLL_EXPORT tmq_t    *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
    DLL_EXPORT int32_t   tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);
    DLL_EXPORT int32_t   tmq_unsubscribe(tmq_t *tmq);
    DLL_EXPORT int32_t   tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
    DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);
    DLL_EXPORT int32_t   tmq_consumer_close(tmq_t *tmq);
    DLL_EXPORT int32_t   tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);
    DLL_EXPORT void      tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);
    DLL_EXPORT int32_t   tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
    DLL_EXPORT void      tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param);
    DLL_EXPORT int32_t   tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,int32_t *numOfAssignment);
    DLL_EXPORT void      tmq_free_assignment(tmq_topic_assignment* pAssignment);
    DLL_EXPORT int32_t   tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
    DLL_EXPORT int64_t   tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId);
    DLL_EXPORT int64_t   tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId);

    DLL_EXPORT const char *tmq_get_topic_name(TAOS_RES *res);
    DLL_EXPORT const char *tmq_get_db_name(TAOS_RES *res);
    DLL_EXPORT int32_t     tmq_get_vgroup_id(TAOS_RES *res);
    DLL_EXPORT int64_t     tmq_get_vgroup_offset(TAOS_RES* res);
    DLL_EXPORT const char *tmq_err2str(int32_t code);DLL_EXPORT void           tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);
`)),(0,esm/* mdx */.kt)("p",null,`下面介绍一下它们的具体用法（超级表和子表结构请参考“数据建模”一节），完整的示例代码请见下面 C 语言的示例代码。`)),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"java",label:"Java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`void subscribe(Collection<String> topics) throws SQLException;

void unsubscribe() throws SQLException;

Set<String> subscription() throws SQLException;

ConsumerRecords<V> poll(Duration timeout) throws SQLException;

Set<TopicPartition> assignment() throws SQLException;
long position(TopicPartition partition) throws SQLException;
Map<TopicPartition, Long> position(String topic) throws SQLException;
Map<TopicPartition, Long> beginningOffsets(String topic) throws SQLException;
Map<TopicPartition, Long> endOffsets(String topic) throws SQLException;
Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) throws SQLException;

void seek(TopicPartition partition, long offset) throws SQLException;
void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException;
void seekToEnd(Collection<TopicPartition> partitions) throws SQLException;

void commitSync() throws SQLException;
void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) throws SQLException;

void close() throws SQLException;
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Python",label:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python"},`class Consumer:
    def subscribe(self, topics):
        pass

    def unsubscribe(self):
        pass

    def poll(self, timeout: float = 1.0):
        pass

    def assignment(self):
        pass

    def seek(self, partition):
        pass

    def close(self):
        pass

    def commit(self, message):
        pass
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)

// 出于兼容目的保留 rebalanceCb 参数，当前未使用
func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error

// 出于兼容目的保留 rebalanceCb 参数，当前未使用
func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error

func (c *Consumer) Poll(timeoutMs int) tmq.Event

// 出于兼容目的保留 tmq.TopicPartition 参数，当前未使用
func (c *Consumer) Commit() ([]tmq.TopicPartition, error)

func (c *Consumer) Unsubscribe() error

func (c *Consumer) Close() error
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`impl TBuilder for TmqBuilder
  fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error>
  fn build(&self) -> Result<Self::Target, Self::Error>

impl AsAsyncConsumer for Consumer
  async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<(), Self::Error>;
  fn stream(
        &self,
    ) -> Pin<
        Box<
            dyn '_
                + Send
                + futures::Stream<
                    Item = Result<(Self::Offset, MessageSet<Self::Meta, Self::Data>), Self::Error>,
                >,
        >,
    >;
  async fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error>;

  async fn unsubscribe(self);
`)),(0,esm/* mdx */.kt)("p",null,`可在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.rs/taos"},`https://docs.rs/taos`),` 上查看详细 API 说明。`)),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`function TMQConsumer(config)

function subscribe(topic)

function consume(timeout)

function subscription()

function unsubscribe()

function commit(msg)

function close()
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"C#",label:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)

virtual IConsumer Build()

Consumer(ConsumerBuilder builder)

void Subscribe(IEnumerable<string> topics)

void Subscribe(string topic) 

ConsumeResult Consume(int millisecondsTimeout)

List<string> Subscription()

void Unsubscribe()
 
void Commit(ConsumeResult consumerResult)

void Close()
`)))),(0,esm/* mdx */.kt)("h2",{"id":"写入数据"},`写入数据`),(0,esm/* mdx */.kt)("p",null,`首先完成建库、建一张超级表和多张子表操作，然后就可以写入数据了，比如：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`DROP DATABASE IF EXISTS tmqdb;
CREATE DATABASE tmqdb WAL_RETENTION_PERIOD 3600;
CREATE TABLE tmqdb.stb (ts TIMESTAMP, c1 INT, c2 FLOAT, c3 VARCHAR(16)) TAGS(t1 INT, t3 VARCHAR(16));
CREATE TABLE tmqdb.ctb0 USING tmqdb.stb TAGS(0, "subtable0");
CREATE TABLE tmqdb.ctb1 USING tmqdb.stb TAGS(1, "subtable1");       
INSERT INTO tmqdb.ctb0 VALUES(now, 0, 0, 'a0')(now+1s, 0, 0, 'a00');
INSERT INTO tmqdb.ctb1 VALUES(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11');
`)),(0,esm/* mdx */.kt)("h2",{"id":"创建-topic"},`创建 `,(0,esm/* mdx */.kt)("em",{parentName:"h2"},`topic`)),(0,esm/* mdx */.kt)("p",null,`TDengine 使用 SQL 创建一个 topic：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`CREATE TOPIC topic_name AS SELECT ts, c1, c2, c3 FROM tmqdb.stb WHERE c1 > 1;
`)),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`topic创建个数有上限，通过参数 tmqMaxTopicNum 控制，默认 20 个`)),(0,esm/* mdx */.kt)("p",null,`TMQ 支持多种订阅类型：`),(0,esm/* mdx */.kt)("h3",{"id":"列订阅"},`列订阅`),(0,esm/* mdx */.kt)("p",null,`语法：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`CREATE TOPIC topic_name as subquery
`)),(0,esm/* mdx */.kt)("p",null,`通过 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`SELECT`),` 语句订阅（包括 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`SELECT *`),`，或 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`SELECT ts, c1`),` 等指定列订阅，可以带条件过滤、标量函数计算，但不支持聚合函数、不支持时间窗口聚合）。需要注意的是：`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`该类型 TOPIC 一旦创建则订阅数据的结构确定。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`被订阅或用于计算的列或标签不可被删除（`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`ALTER table DROP`),`）、修改（`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`ALTER table MODIFY`),`）。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`若发生表结构变更，新增的列不出现在结果中。`)),(0,esm/* mdx */.kt)("h3",{"id":"超级表订阅"},`超级表订阅`),(0,esm/* mdx */.kt)("p",null,`语法：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`CREATE TOPIC topic_name [with meta] AS STABLE stb_name [where_condition]
`)),(0,esm/* mdx */.kt)("p",null,`与 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`SELECT * from stbName`),` 订阅的区别是：`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`不会限制用户的表结构变更。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`返回的是非结构化的数据：返回数据的结构会随之超级表的表结构变化而变化。`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`with meta 参数可选，选择时将返回创建超级表，子表等语句，主要用于taosx做超级表迁移`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`where_condition 参数可选，选择时将用来过滤符合条件的子表，订阅这些子表。where 条件里不能有普通列，只能是tag或tbname，where条件里可以用函数，用来过滤tag，但是不能是聚合函数，因为子表tag值无法做聚合。也可以是常量表达式，比如 2 > 1（订阅全部子表），或者 false（订阅0个子表）`),(0,esm/* mdx */.kt)("li",{parentName:"ul"},`返回数据不包含标签。`)),(0,esm/* mdx */.kt)("h3",{"id":"数据库订阅"},`数据库订阅`),(0,esm/* mdx */.kt)("p",null,`语法：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`CREATE TOPIC topic_name [with meta] AS DATABASE db_name;
`)),(0,esm/* mdx */.kt)("p",null,`通过该语句可创建一个包含数据库所有表数据的订阅`),(0,esm/* mdx */.kt)("ul",null,(0,esm/* mdx */.kt)("li",{parentName:"ul"},`with meta 参数可选，选择时将返回创建数据库里所有超级表，子表的语句，主要用于taosx做数据库迁移`)),(0,esm/* mdx */.kt)("h2",{"id":"创建消费者-consumer"},`创建消费者 `,(0,esm/* mdx */.kt)("em",{parentName:"h2"},`consumer`)),(0,esm/* mdx */.kt)("p",null,`消费者需要通过一系列配置选项创建，基础配置项如下表所示：`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`参数名称`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":"center"},`类型`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`参数说明`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`备注`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`td.connect.ip`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`服务端的 IP 地址`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null})),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`td.connect.user`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`用户名`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null})),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`td.connect.pass`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`密码`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null})),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`td.connect.port`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`integer`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`服务端的端口号`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null})),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`group.id`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`消费组 ID，同一消费组共享消费进度`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("br",null),(0,esm/* mdx */.kt)("strong",{parentName:"td"},`必填项`),`。最大长度：192。`,(0,esm/* mdx */.kt)("br",null),`每个topic最多可建立100个 consumer group`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`client.id`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`客户端 ID`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`最大长度：192。`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`auto.offset.reset`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`enum`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`消费组订阅的初始位置`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("br",null),(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`earliest`),`: default;从头开始订阅; `,(0,esm/* mdx */.kt)("br",null),(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`latest`),`: 仅从最新数据开始订阅; `,(0,esm/* mdx */.kt)("br",null),(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`none`),`: 没有提交的 offset 无法订阅`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`enable.auto.commit`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`boolean`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`是否启用消费位点自动提交，true: 自动提交，客户端应用无需commit；false：客户端应用需要自行commit`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`默认值为 true`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`auto.commit.interval.ms`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`integer`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`消费记录自动提交消费位点时间间隔，单位为毫秒`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`默认值为 5000`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`msg.with.table.name`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":"center"},`boolean`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`是否允许从消息中解析表名, 不适用于列订阅（列订阅时可将 tbname 作为列写入 subquery 语句）`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`默认关闭`)))),(0,esm/* mdx */.kt)("p",null,`对于不同编程语言，其设置方式如下：`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"c",label:"C",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`/* 根据需要，设置消费组 (group.id)、自动提交 (enable.auto.commit)、
   自动提交时间间隔 (auto.commit.interval.ms)、用户名 (td.connect.user)、密码 (td.connect.pass) 等参数 */
tmq_conf_t* conf = tmq_conf_new();
tmq_conf_set(conf, "enable.auto.commit", "true");
tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
tmq_conf_set(conf, "group.id", "cgrpName");
tmq_conf_set(conf, "td.connect.user", "root");
tmq_conf_set(conf, "td.connect.pass", "taosdata");
tmq_conf_set(conf, "auto.offset.reset", "earliest");
tmq_conf_set(conf, "msg.with.table.name", "true");
tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
tmq_conf_destroy(conf);
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"java",label:"Java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`对于 Java 程序，还可以使用如下配置项：`),(0,esm/* mdx */.kt)("table",null,(0,esm/* mdx */.kt)("thead",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"thead"},(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`参数名称`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`类型`),(0,esm/* mdx */.kt)("th",{parentName:"tr","align":null},`参数说明`))),(0,esm/* mdx */.kt)("tbody",{parentName:"table"},(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`td.connect.type`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`连接类型，"jni" 指原生连接，"ws" 指 websocket 连接，默认值为 "jni"`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`bootstrap.servers`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`连接地址，如 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`localhost:6030`))),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`value.deserializer`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`值解析方法，使用此方法应实现 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`com.taosdata.jdbc.tmq.Deserializer`),` 接口或继承 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`com.taosdata.jdbc.tmq.ReferenceDeserializer`),` 类`)),(0,esm/* mdx */.kt)("tr",{parentName:"tbody"},(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},(0,esm/* mdx */.kt)("inlineCode",{parentName:"td"},`value.deserializer.encoding`)),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`string`),(0,esm/* mdx */.kt)("td",{parentName:"tr","align":null},`指定字符串解析的字符集`)))),(0,esm/* mdx */.kt)("p",null,`需要注意：此处使用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`bootstrap.servers`),` 替代 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`td.connect.ip`),` 和 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`td.connect.port`),`，以提供与 Kafka 一致的接口。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`Properties properties = new Properties();
properties.setProperty("enable.auto.commit", "true");
properties.setProperty("auto.commit.interval.ms", "1000");
properties.setProperty("group.id", "cgrpName");
properties.setProperty("bootstrap.servers", "127.0.0.1:6030");
properties.setProperty("td.connect.user", "root");
properties.setProperty("td.connect.pass", "taosdata");
properties.setProperty("auto.offset.reset", "earliest");
properties.setProperty("msg.with.table.name", "true");
properties.setProperty("value.deserializer", "com.taos.example.MetersDeserializer");

TaosConsumer<Meters> consumer = new TaosConsumer<>(properties);

/* value deserializer definition. */
import com.taosdata.jdbc.tmq.ReferenceDeserializer;

public class MetersDeserializer extends ReferenceDeserializer<Meters> {
}
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`conf := &tmq.ConfigMap{
 "group.id":                     "test",
 "auto.offset.reset":            "earliest",
 "td.connect.ip":                "127.0.0.1",
 "td.connect.user":              "root",
 "td.connect.pass":              "taosdata",
 "td.connect.port":              "6030",
 "client.id":                    "test_tmq_c",
 "enable.auto.commit":           "false",
 "msg.with.table.name":          "true",
}
consumer, err := NewConsumer(conf)
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`let mut dsn: Dsn = "taos://".parse()?;
dsn.set("group.id", "group1");
dsn.set("client.id", "test");
dsn.set("auto.offset.reset", "earliest");

let tmq = TmqBuilder::from_dsn(dsn)?;

let mut consumer = tmq.build()?;
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Python",label:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Python 语言下引入 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` 库的 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Consumer`),` 类，创建一个 Consumer 示例：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python"},`from taos.tmq import Consumer

# Syntax: \`consumer = Consumer(configs)\`
#
# Example:
consumer = Consumer({"group.id": "local", "td.connect.ip": "127.0.0.1"})
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`// 根据需要，设置消费组 (group.id)、自动提交 (enable.auto.commit)、
// 自动提交时间间隔 (auto.commit.interval.ms)、用户名 (td.connect.user)、密码 (td.connect.pass) 等参数 

let consumer = taos.consumer({
  'enable.auto.commit': 'true',
  'auto.commit.interval.ms','1000',
  'group.id': 'tg2',
  'td.connect.user': 'root',
  'td.connect.pass': 'taosdata',
  'auto.offset.reset','earliest',
  'msg.with.table.name': 'true',
  'td.connect.ip','127.0.0.1',
  'td.connect.port','6030'  
  });
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"C#",label:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`using TDengineTMQ;

// 根据需要，设置消费组 (GourpId)、自动提交 (EnableAutoCommit)、
// 自动提交时间间隔 (AutoCommitIntervalMs)、用户名 (TDConnectUser)、密码 (TDConnectPasswd) 等参数
var cfg = new ConsumerConfig
 {
    EnableAutoCommit = "true"
    AutoCommitIntervalMs = "1000"
    GourpId = "TDengine-TMQ-C#",
    TDConnectUser = "root",
    TDConnectPasswd = "taosdata",
    AutoOffsetReset = "earliest"
    MsgWithTableName = "true",
    TDConnectIp = "127.0.0.1",
    TDConnectPort = "6030"
 };

var consumer = new ConsumerBuilder(cfg).Build();

`)))),(0,esm/* mdx */.kt)("p",null,`上述配置中包括 consumer group ID，如果多个 consumer 指定的 consumer group ID 一样，则自动形成一个 consumer group，共享消费进度。`),(0,esm/* mdx */.kt)("h2",{"id":"订阅-topics"},`订阅 `,(0,esm/* mdx */.kt)("em",{parentName:"h2"},`topics`)),(0,esm/* mdx */.kt)("p",null,`一个 consumer 支持同时订阅多个 topic。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"c",label:"C",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`// 创建订阅 topics 列表
tmq_list_t* topicList = tmq_list_new();
tmq_list_append(topicList, "topicName");
// 启动订阅
tmq_subscribe(tmq, topicList);
tmq_list_destroy(topicList);
  
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"java",label:"Java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`List<String> topics = new ArrayList<>();
topics.add("tmq_topic");
consumer.subscribe(topics);
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Go",label:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`err = consumer.Subscribe("example_tmq_topic", nil)
if err != nil {
 panic(err)
}
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Rust",label:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.subscribe(["tmq_meters"]).await?;
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Python",label:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python"},`consumer.subscribe(['topic1', 'topic2'])
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`// 创建订阅 topics 列表
let topics = ['topic_test']

// 启动订阅
consumer.subscribe(topics);
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"C#",label:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`// 创建订阅 topics 列表
List<String> topics = new List<string>();
topics.add("tmq_topic");
// 启动订阅
consumer.Subscribe(topics);
`)))),(0,esm/* mdx */.kt)("h2",{"id":"消费"},`消费`),(0,esm/* mdx */.kt)("p",null,`以下代码展示了不同语言下如何对 TMQ 消息进行消费。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"c",label:"C",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`// 消费数据
while (running) {
  TAOS_RES* msg = tmq_consumer_poll(tmq, timeOut);
  msg_process(msg);
}  
`)),(0,esm/* mdx */.kt)("p",null,`这里是一个 `,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`while`),` 循环，每调用一次 tmq_consumer_poll()，获取一个消息，该消息与普通查询返回的结果集完全相同，可以使用相同的解析 API 完成消息内容的解析。`)),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"java",label:"Java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`while(running){
  ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
    for (Meters meter : meters) {
      processMsg(meter);
    }    
}
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Go",label:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`for {
 ev := consumer.Poll(0)
 if ev != nil {
  switch e := ev.(type) {
  case *tmqcommon.DataMessage:
   fmt.Println(e.Value())
  case tmqcommon.Error:
   fmt.Fprintf(os.Stderr, "%% Error: %v: %v\\n", e.Code(), e)
   panic(e)
  }
  consumer.Commit()
 }
}
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Rust",label:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`{
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
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Python",label:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python"},`while True:
    res = consumer.poll(100)
    if not res:
        continue
    err = res.error()
    if err is not None:
        raise err
    val = res.value()

    for block in val:
        print(block.fetchall())
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`while(true){
  msg = consumer.consume(200);
  // process message(consumeResult)
  console.log(msg.topicPartition);
  console.log(msg.block);
  console.log(msg.fields)
}
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"C#",label:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`// 消费数据
while (true)
{
    var consumerRes = consumer.Consume(100);
    // process ConsumeResult
    ProcessMsg(consumerRes);
    consumer.Commit(consumerRes);
}
`)))),(0,esm/* mdx */.kt)("h2",{"id":"结束消费"},`结束消费`),(0,esm/* mdx */.kt)("p",null,`消费结束后，应当取消订阅。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"c",label:"C",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c"},`/* 取消订阅 */
tmq_unsubscribe(tmq);

/* 关闭消费者对象 */
tmq_consumer_close(tmq);
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"java",label:"Java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java"},`/* 取消订阅 */
consumer.unsubscribe();

/* 关闭消费 */
consumer.close();
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Go",label:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go"},`/* Unsubscribe */
_ = consumer.Unsubscribe()

/* Close consumer */
_ = consumer.Close()
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Rust",label:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust"},`consumer.unsubscribe().await;
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"Python",label:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`# 取消订阅
consumer.unsubscribe()
# 关闭消费
consumer.close()
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js"},`consumer.unsubscribe();
consumer.close();
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"C#",label:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp"},`// 取消订阅
consumer.Unsubscribe();

// 关闭消费
consumer.Close();
`)))),(0,esm/* mdx */.kt)("h2",{"id":"删除-topic"},`删除 `,(0,esm/* mdx */.kt)("em",{parentName:"h2"},`topic`)),(0,esm/* mdx */.kt)("p",null,`如果不再需要订阅数据，可以删除 topic，需要注意：只有当前未在订阅中的 TOPIC 才能被删除。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`/* 删除 topic */
DROP TOPIC topic_name;
`)),(0,esm/* mdx */.kt)("h2",{"id":"状态查看"},`状态查看`),(0,esm/* mdx */.kt)("p",null,`1、`,(0,esm/* mdx */.kt)("em",{parentName:"p"},`topics`),`：查询已经创建的 topic`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`SHOW TOPICS;
`)),(0,esm/* mdx */.kt)("p",null,`2、consumers：查询 consumer 的状态及其订阅的 topic`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`SHOW CONSUMERS;
`)),(0,esm/* mdx */.kt)("p",null,`3、subscriptions：查询 consumer 与 vgroup 之间的分配关系`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-sql"},`SHOW SUBSCRIPTIONS;
`)),(0,esm/* mdx */.kt)("h2",{"id":"示例代码"},`示例代码`),(0,esm/* mdx */.kt)("p",null,`以下是各语言的完整示例代码。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"java",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_c_MDXContent,{mdxType:"CDemo"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"native",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"native",label:"\u672C\u5730\u8FDE\u63A5",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"Java"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"ws",label:"WebSocket \u8FDE\u63A5",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_java_ws_MDXContent,{mdxType:"JavaWS"})))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"Go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_go_MDXContent,{mdxType:"Go"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"Rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_rust_MDXContent,{mdxType:"Rust"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_python_MDXContent,{mdxType:"Python"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.JS",value:"Node.JS",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_node_MDXContent,{mdxType:"Node"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"C#",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_sub_cs_MDXContent,{mdxType:"CSharp"}))));};_07_tmq_MDXContent.isMDXComponent=true;

/***/ })

}]);