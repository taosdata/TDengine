"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[4551],{

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

/***/ 4697:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  assets: () => (/* binding */ assets),
  contentTitle: () => (/* binding */ _20_kafka_writting_contentTitle),
  "default": () => (/* binding */ _20_kafka_writting_MDXContent),
  frontMatter: () => (/* binding */ _20_kafka_writting_frontMatter),
  metadata: () => (/* binding */ metadata),
  toc: () => (/* binding */ _20_kafka_writting_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/_py_kafka.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[{value:'python Kafka client',id:'python-kafka-client',level:3},{value:'consume from Kafka',id:'consume-from-kafka',level:3},{value:'multi-threading',id:'multi-threading',level:3},{value:'multi-process',id:'multi-process',level:3},{value:'examples',id:'examples',level:3},{value:'execute Python examples',id:'execute-python-examples',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h3",{"id":"python-kafka-client"},`python Kafka client`),(0,esm/* mdx */.kt)("p",null,`For python kafka client, please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://cwiki.apache.org/confluence/display/KAFKA/Clients#Clients-Python"},`kafka client`),`. In this document, we use `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"http://github.com/dpkp/kafka-python"},`kafka-python`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"consume-from-kafka"},`consume from Kafka`),(0,esm/* mdx */.kt)("p",null,`The simple way to consume messages from Kafka is to read messages one by one. The demo is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`from kafka import KafkaConsumer
consumer = KafkaConsumer('my_favorite_topic')
for msg in consumer:
     print (msg)
`)),(0,esm/* mdx */.kt)("p",null,`For higher performance, we can consume message from kafka in batch. The demo is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`from kafka import KafkaConsumer
consumer = KafkaConsumer('my_favorite_topic')
while True:
    msgs = consumer.poll(timeout_ms=500, max_records=1000)
    if msgs:
        print (msgs)
`)),(0,esm/* mdx */.kt)("h3",{"id":"multi-threading"},`multi-threading`),(0,esm/* mdx */.kt)("p",null,`For more higher performance we can process data from kafka in multi-thread. We can use python's ThreadPoolExecutor to achieve multithreading. The demo is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`from concurrent.futures import ThreadPoolExecutor, Future
pool = ThreadPoolExecutor(max_workers=10)
pool.submit(...)
`)),(0,esm/* mdx */.kt)("h3",{"id":"multi-process"},`multi-process`),(0,esm/* mdx */.kt)("p",null,`For more higher performance, sometimes we use multiprocessing. In this case, the number of Kafka Consumers should not be greater than the number of Kafka Topic Partitions. The demo is as follows:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`from multiprocessing import Process

ps = []
for i in range(5):
    p = Process(target=Consumer().consume())
    p.start()
    ps.append(p)

for p in ps:
    p.join()
`)),(0,esm/* mdx */.kt)("p",null,`In addition to python's built-in multithreading and multiprocessing library, we can also use the third-party library gunicorn.`),(0,esm/* mdx */.kt)("h3",{"id":"examples"},`examples`),(0,esm/* mdx */.kt)("details",null,(0,esm/* mdx */.kt)("summary",null,"kafka_example_perform"),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka_example_perform`),` is the entry point of the examples.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`#! encoding=utf-8

import argparse
import logging
import multiprocessing
import time
from multiprocessing import pool

import kafka_example_common as common
import kafka_example_consumer as consumer
import kafka_example_producer as producer

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-kafka-broker', type=str, default='localhost:9092',
                        help='kafka borker host. default is \`localhost:9200\`')
    parser.add_argument('-kafka-topic', type=str, default='tdengine-kafka-practices',
                        help='kafka topic. default is \`tdengine-kafka-practices\`')
    parser.add_argument('-kafka-group', type=str, default='kafka_practices',
                        help='kafka consumer group. default is \`kafka_practices\`')
    parser.add_argument('-taos-host', type=str, default='localhost',
                        help='TDengine host. default is \`localhost\`')
    parser.add_argument('-taos-port', type=int, default=6030, help='TDengine port. default is 6030')
    parser.add_argument('-taos-user', type=str, default='root', help='TDengine username, default is \`root\`')
    parser.add_argument('-taos-password', type=str, default='taosdata', help='TDengine password, default is \`taosdata\`')
    parser.add_argument('-taos-db', type=str, default='tdengine_kafka_practices',
                        help='TDengine db name, default is \`tdengine_kafka_practices\`')
    parser.add_argument('-table-count', type=int, default=100, help='TDengine sub-table count, default is 100')
    parser.add_argument('-table-items', type=int, default=1000, help='items in per sub-tables, default is 1000')
    parser.add_argument('-message-type', type=str, default='line',
                        help='kafka message type. \`line\` or \`json\`. default is \`line\`')
    parser.add_argument('-max-poll', type=int, default=1000, help='max poll for kafka consumer')
    parser.add_argument('-threads', type=int, default=10, help='thread count for deal message')
    parser.add_argument('-processes', type=int, default=1, help='process count')

    args = parser.parse_args()
    total = args.table_count * args.table_items

    logging.warning("## start to prepare testing data...")
    prepare_data_start = time.time()
    producer.produce_total(100, args.kafka_broker, args.kafka_topic, args.message_type, total, args.table_count)
    prepare_data_end = time.time()
    logging.warning("## prepare testing data finished! spend-[%s]", prepare_data_end - prepare_data_start)

    logging.warning("## start to create database and tables ...")
    create_db_start = time.time()
    # create database and table
    common.create_database_and_tables(host=args.taos_host, port=args.taos_port, user=args.taos_user,
                                      password=args.taos_password, db=args.taos_db, table_count=args.table_count)
    create_db_end = time.time()
    logging.warning("## create database and tables finished! spend [%s]", create_db_end - create_db_start)

    processes = args.processes

    logging.warning("## start to consume data and insert into TDengine...")
    consume_start = time.time()
    if processes > 1:  # multiprocess
        multiprocessing.set_start_method("spawn")
        pool = pool.Pool(processes)

        consume_start = time.time()
        for _ in range(processes):
            pool.apply_async(func=consumer.consume, args=(
                args.kafka_broker, args.kafka_topic, args.kafka_group, args.taos_host, args.taos_port, args.taos_user,
                args.taos_password, args.taos_db, args.message_type, args.max_poll, args.threads))
        pool.close()
        pool.join()
    else:
        consume_start = time.time()
        consumer.consume(kafka_brokers=args.kafka_broker, kafka_topic=args.kafka_topic, kafka_group_id=args.kafka_group,
                         taos_host=args.taos_host, taos_port=args.taos_port, taos_user=args.taos_user,
                         taos_password=args.taos_password, taos_database=args.taos_db, message_type=args.message_type,
                         max_poll=args.max_poll, workers=args.threads)
    consume_end = time.time()
    logging.warning("## consume data and insert into TDengine over! spend-[%s]", consume_end - consume_start)

    # print report
    logging.warning(
        "\\n#######################\\n"
        "     Prepare data      \\n"
        "#######################\\n"
        "# data_type # %s  \\n"
        "# total     # %s  \\n"
        "# spend     # %s s\\n"
        "#######################\\n"
        "     Create database   \\n"
        "#######################\\n"
        "# stable    # 1  \\n"
        "# sub-table # 100  \\n"
        "# spend     # %s s \\n"
        "#######################\\n"
        "        Consume        \\n"
        "#######################\\n"
        "# data_type   # %s  \\n"
        "# threads     # %s  \\n"
        "# processes   # %s  \\n"
        "# total_count # %s  \\n"
        "# spend       # %s s\\n"
        "# per_second  # %s  \\n"
        "#######################\\n",
        args.message_type, total, prepare_data_end - prepare_data_start, create_db_end - create_db_start,
        args.message_type, args.threads, processes, total, consume_end - consume_start,
                                  total / (consume_end - consume_start))

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/kafka_example_perform.py"},`view source code`))),(0,esm/* mdx */.kt)("details",null,(0,esm/* mdx */.kt)("summary",null,"kafka_example_common"),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka_example_common`),` is the common code of the examples.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`#! encoding = utf-8
import taos

LOCATIONS = ['California.SanFrancisco', 'California.LosAngles', 'California.SanDiego', 'California.SanJose',
             'California.PaloAlto', 'California.Campbell', 'California.MountainView', 'California.Sunnyvale',
             'California.SantaClara', 'California.Cupertino']

CREATE_DATABASE_SQL = 'create database if not exists {} keep 365 duration 10 buffer 16 wal_level 1 wal_retention_period 3600'
USE_DATABASE_SQL = 'use {}'
DROP_TABLE_SQL = 'drop table if exists meters'
DROP_DATABASE_SQL = 'drop database if exists {}'
CREATE_STABLE_SQL = 'create stable meters (ts timestamp, current float, voltage int, phase float) tags ' \\
                    '(location binary(64), groupId int)'
CREATE_TABLE_SQL = 'create table if not exists {} using meters tags (\\'{}\\', {})'


def create_database_and_tables(host, port, user, password, db, table_count):
    tags_tables = _init_tags_table_names(table_count=table_count)
    conn = taos.connect(host=host, port=port, user=user, password=password)

    conn.execute(DROP_DATABASE_SQL.format(db))
    conn.execute(CREATE_DATABASE_SQL.format(db))
    conn.execute(USE_DATABASE_SQL.format(db))
    conn.execute(DROP_TABLE_SQL)
    conn.execute(CREATE_STABLE_SQL)
    for tags in tags_tables:
        location, group_id = _get_location_and_group(tags)
        tables = tags_tables[tags]
        for table_name in tables:
            conn.execute(CREATE_TABLE_SQL.format(table_name, location, group_id))
    conn.close()


def clean(host, port, user, password, db):
    conn = taos.connect(host=host, port=port, user=user, password=password)
    conn.execute(DROP_DATABASE_SQL.format(db))
    conn.close()


def _init_tags_table_names(table_count):
    tags_table_names = {}
    group_id = 0
    for i in range(table_count):
        table_name = 'd{}'.format(i)
        location_idx = i % len(LOCATIONS)
        location = LOCATIONS[location_idx]
        if location_idx == 0:
            group_id += 1
            if group_id > 10:
                group_id -= 10
        key = _tag_table_mapping_key(location=location, group_id=group_id)
        if key not in tags_table_names:
            tags_table_names[key] = []
        tags_table_names[key].append(table_name)

    return tags_table_names


def _tag_table_mapping_key(location, group_id):
    return '{}_{}'.format(location, group_id)


def _get_location_and_group(key):
    fields = key.split('_')
    return fields[0], fields[1]

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/kafka_example_common.py"},`view source code`))),(0,esm/* mdx */.kt)("details",null,(0,esm/* mdx */.kt)("summary",null,"kafka_example_producer"),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka_example_producer`),` is `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`producer`),`, which is responsible for generating test data and sending it to kafka.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`#! encoding = utf-8
import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime

from kafka import KafkaProducer

locations = ['California.SanFrancisco', 'California.LosAngles', 'California.SanDiego', 'California.SanJose',
             'California.PaloAlto', 'California.Campbell', 'California.MountainView', 'California.Sunnyvale',
             'California.SantaClara', 'California.Cupertino']

producers: list[KafkaProducer] = []

lock = threading.Lock()
start = 1640966400


def produce_total(workers, broker, topic, message_type, total, table_count):
    if len(producers) == 0:
        lock.acquire()
        if len(producers) == 0:
            _init_kafka_producers(broker=broker, count=10)
        lock.release()
    pool = ThreadPoolExecutor(max_workers=workers)
    futures = []
    for _ in range(0, workers):
        futures.append(pool.submit(_produce_total, topic, message_type, int(total / workers), table_count))
    pool.shutdown()
    for f in futures:
        f.result()
    _close_kafka_producers()


def _produce_total(topic, message_type, total, table_count):
    producer = _get_kafka_producer()
    for _ in range(total):
        message = _get_fake_date(message_type=message_type, table_count=table_count)
        producer.send(topic=topic, value=message.encode(encoding='utf-8'))


def _init_kafka_producers(broker, count):
    for _ in range(count):
        p = KafkaProducer(bootstrap_servers=broker, batch_size=64 * 1024, linger_ms=300, acks=0)
        producers.append(p)


def _close_kafka_producers():
    for p in producers:
        p.close()


def _get_kafka_producer():
    return producers[random.randint(0, len(producers) - 1)]


def _get_fake_date(table_count, message_type='json'):
    if message_type == 'json':
        return _get_json_message(table_count=table_count)
    if message_type == 'line':
        return _get_line_message(table_count=table_count)
    return ''


def _get_json_message(table_count):
    return json.dumps({
        'ts': _get_timestamp(),
        'current': random.randint(0, 1000) / 100,
        'voltage': random.randint(105, 115),
        'phase': random.randint(0, 32000) / 100000,
        'location': random.choice(locations),
        'groupId': random.randint(1, 10),
        'table_name': _random_table_name(table_count)
    })


def _get_line_message(table_count):
    return "{} values('{}', {}, {}, {})".format(
        _random_table_name(table_count),  # table
        _get_timestamp(),  # ts
        random.randint(0, 1000) / 100,  # current
        random.randint(105, 115),  # voltage
        random.randint(0, 32000) / 100000,  # phase
    )


def _random_table_name(table_count):
    return 'd{}'.format(random.randint(0, table_count - 1))


def _get_timestamp():
    global start
    lock.acquire(blocking=True)
    start += 0.001
    lock.release()
    return datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/kafka_example_producer.py"},`view source code`))),(0,esm/* mdx */.kt)("details",null,(0,esm/* mdx */.kt)("summary",null,"kafka_example_consumer"),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka_example_consumer`),` is `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`consumer`),`, which is responsible for consuming data from kafka and writing it to TDengine.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-py"},`#! encoding = utf-8
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from json import JSONDecodeError
from typing import Callable

import taos
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

import kafka_example_common as common


class Consumer(object):
    DEFAULT_CONFIGS = {
        'kafka_brokers': 'localhost:9092',  # kafka broker
        'kafka_topic': 'tdengine_kafka_practices',
        'kafka_group_id': 'taos',
        'taos_host': 'localhost',  # TDengine host
        'taos_port': 6030,  # TDengine port
        'taos_user': 'root',  # TDengine user name
        'taos_password': 'taosdata',  # TDengine password
        'taos_database': 'power',  # TDengine database
        'message_type': 'json',  # message format, 'json' or 'line'
        'clean_after_testing': False,  # if drop database after testing
        'max_poll': 1000,  # poll size for batch mode
        'workers': 10,  # thread count for multi-threading
        'testing': False
    }

    INSERT_SQL_HEADER = "insert into "
    INSERT_PART_SQL = '{} values (\\'{}\\', {}, {}, {})'

    def __init__(self, **configs):
        self.config = self.DEFAULT_CONFIGS
        self.config.update(configs)

        self.consumer = None
        if not self.config.get('testing'):
            self.consumer = KafkaConsumer(
                self.config.get('kafka_topic'),
                bootstrap_servers=self.config.get('kafka_brokers'),
                group_id=self.config.get('kafka_group_id'),
            )

        self.conns = taos.connect(
            host=self.config.get('taos_host'),
            port=self.config.get('taos_port'),
            user=self.config.get('taos_user'),
            password=self.config.get('taos_password'),
            db=self.config.get('taos_database'),
        )
        if self.config.get('workers') > 1:
            self.pool = ThreadPoolExecutor(max_workers=self.config.get('workers'))
            self.tasks = []
        # tags and table mapping # key: {location}_{groupId} value:

    def consume(self):
        """

        consume data from kafka and deal. Base on \`message_type\`, \`bath_consume\`, \`insert_by_table\`,
        there are several deal function.
        :return:
        """
        self.conns.execute(common.USE_DATABASE_SQL.format(self.config.get('taos_database')))
        try:
            if self.config.get('message_type') == 'line':  # line
                self._run(self._line_to_taos)
            if self.config.get('message_type') == 'json':  # json
                self._run(self._json_to_taos)
        except KeyboardInterrupt:
            logging.warning("## caught keyboard interrupt, stopping")
        finally:
            self.stop()

    def stop(self):
        """

        stop consuming
        :return:
        """
        # close consumer
        if self.consumer is not None:
            self.consumer.commit()
            self.consumer.close()

        # multi thread
        if self.config.get('workers') > 1:
            if self.pool is not None:
                self.pool.shutdown()
            for task in self.tasks:
                while not task.done():
                    time.sleep(0.01)

        # clean data
        if self.config.get('clean_after_testing'):
            self.conns.execute(common.DROP_TABLE_SQL)
            self.conns.execute(common.DROP_DATABASE_SQL.format(self.config.get('taos_database')))
        # close taos
        if self.conns is not None:
            self.conns.close()

    def _run(self, f):
        """

        run in batch consuming mode
        :param f:
        :return:
        """
        i = 0  # just for test.
        while True:
            messages = self.consumer.poll(timeout_ms=100, max_records=self.config.get('max_poll'))
            if messages:
                if self.config.get('workers') > 1:
                    self.pool.submit(f, messages.values())
                else:
                    f(list(messages.values()))
            if not messages:
                i += 1  # just for test.
                time.sleep(0.1)
            if i > 3:  # just for test.
                logging.warning('## test over.')  # just for test.
                return  # just for test.

    def _json_to_taos(self, messages):
        """

        convert a batch of json data to sql, and insert into TDengine
        :param messages:
        :return:
        """
        sql = self._build_sql_from_json(messages=messages)
        self.conns.execute(sql=sql)

    def _line_to_taos(self, messages):
        """

        convert a batch of lines data to sql, and insert into TDengine
        :param messages:
        :return:
        """
        lines = []
        for partition_messages in messages:
            for message in partition_messages:
                lines.append(message.value.decode())
        sql = self.INSERT_SQL_HEADER + ' '.join(lines)
        self.conns.execute(sql=sql)

    def _build_single_sql_from_json(self, msg_value):
        try:
            data = json.loads(msg_value)
        except JSONDecodeError as e:
            logging.error('## decode message [%s] error ', msg_value, e)
            return ''
        # location = data.get('location')
        # group_id = data.get('groupId')
        ts = data.get('ts')
        current = data.get('current')
        voltage = data.get('voltage')
        phase = data.get('phase')
        table_name = data.get('table_name')

        return self.INSERT_PART_SQL.format(table_name, ts, current, voltage, phase)

    def _build_sql_from_json(self, messages):
        sql_list = []
        for partition_messages in messages:
            for message in partition_messages:
                sql_list.append(self._build_single_sql_from_json(message.value))
        return self.INSERT_SQL_HEADER + ' '.join(sql_list)


def test_json_to_taos(consumer: Consumer):
    records = [
        [
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'table_name': 'd0',
                                             'ts': '2022-12-06 15:13:38.643',
                                             'current': 3.41,
                                             'voltage': 105,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'table_name': 'd1',
                                             'ts': '2022-12-06 15:13:39.643',
                                             'current': 3.41,
                                             'voltage': 102,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
        ]
    ]

    consumer._json_to_taos(messages=records)


def test_line_to_taos(consumer: Consumer):
    records = [
        [
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value="d0 values('2023-01-01 00:00:00.001', 3.49, 109, 0.02737)".encode('utf-8'),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value="d1 values('2023-01-01 00:00:00.002', 6.19, 112, 0.09171)".encode('utf-8'),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
        ]
    ]
    consumer._line_to_taos(messages=records)


def consume(kafka_brokers, kafka_topic, kafka_group_id, taos_host, taos_port, taos_user,
            taos_password, taos_database, message_type, max_poll, workers):
    c = Consumer(kafka_brokers=kafka_brokers, kafka_topic=kafka_topic, kafka_group_id=kafka_group_id,
                 taos_host=taos_host, taos_port=taos_port, taos_user=taos_user, taos_password=taos_password,
                 taos_database=taos_database, message_type=message_type, max_poll=max_poll, workers=workers)
    c.consume()


if __name__ == '__main__':
    consumer = Consumer(testing=True)
    common.create_database_and_tables(host='localhost', port=6030, user='root', password='taosdata', db='py_kafka_test',
                                      table_count=10)
    consumer.conns.execute(common.USE_DATABASE_SQL.format('py_kafka_test'))
    test_json_to_taos(consumer)
    test_line_to_taos(consumer)
    common.clean(host='localhost', port=6030, user='root', password='taosdata', db='py_kafka_test')

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/kafka_example_consumer.py"},`view source code`))),(0,esm/* mdx */.kt)("h3",{"id":"execute-python-examples"},`execute Python examples`),(0,esm/* mdx */.kt)("details",null,(0,esm/* mdx */.kt)("summary",null,"execute Python examples"),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`1. install and start up \`kafka\`
2. install python3 and pip
3. install \`taospy\` by pip
4. install \`kafka-python\` by pip
5. execute this example

The entry point of this example is \`kafka_example_perform.py\`. For more information about usage, please use \`--help\` command.

\`\`\`
python3 kafka_example_perform.py --help
\`\`\`

For example, the following command is creating 100 sub-table and inserting 20000 data for each table and the kafka max poll is 100 and 1 thread and 1 process per thread.

\`\`\`
python3 kafka_example_perform.py -table-count=100 -table-items=20000 -max-poll=100 -threads=1 -processes=1
\`\`\`
`))));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/03-insert-data/20-kafka-writting.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _20_kafka_writting_frontMatter={title:'Write from Kafka',description:'This document describes how to insert data into TDengine using Kafka.'};const _20_kafka_writting_contentTitle=undefined;const metadata={"unversionedId":"develop/insert-data/kafka-writting","id":"develop/insert-data/kafka-writting","title":"Write from Kafka","description":"This document describes how to insert data into TDengine using Kafka.","source":"@site/docs/07-develop/03-insert-data/20-kafka-writting.mdx","sourceDirName":"07-develop/03-insert-data","slug":"/develop/insert-data/kafka-writting","permalink":"/docs-en/develop/insert-data/kafka-writting","draft":false,"tags":[],"version":"current","sidebarPosition":20,"frontMatter":{"title":"Write from Kafka","description":"This document describes how to insert data into TDengine using Kafka."},"sidebar":"defaultSidebar","previous":{"title":"Insert Using SQL","permalink":"/docs-en/develop/insert-data/sql-writing"},"next":{"title":"InfluxDB Line Protocol","permalink":"/docs-en/develop/insert-data/influxdb-line"}};const assets={};const _20_kafka_writting_toc=[{value:'About Kafka',id:'about-kafka',level:2},{value:'kafka topic',id:'kafka-topic',level:3},{value:'Insert into TDengine',id:'insert-into-tdengine',level:2},{value:'Examples',id:'examples',level:2}];const _20_kafka_writting_layoutProps={toc: _20_kafka_writting_toc};const _20_kafka_writting_MDXLayout="wrapper";function _20_kafka_writting_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_20_kafka_writting_MDXLayout,(0,esm_extends/* default */.Z)({},_20_kafka_writting_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h2",{"id":"about-kafka"},`About Kafka`),(0,esm/* mdx */.kt)("p",null,`Apache Kafka is an open-source distributed event streaming platform, used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications. For the key concepts of kafka, please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://kafka.apache.org/documentation/#gettingStarted"},`kafka documentation`),`.`),(0,esm/* mdx */.kt)("h3",{"id":"kafka-topic"},`kafka topic`),(0,esm/* mdx */.kt)("p",null,`Messages in Kafka are organized by topics. A topic may have one or more partitions. We can manage kafka topics through `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka-topics`),`.`),(0,esm/* mdx */.kt)("p",null,`create a topic named `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka-events`),`:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`bin/kafka-topics.sh --create --topic kafka-events --bootstrap-server localhost:9092
`)),(0,esm/* mdx */.kt)("p",null,`Alter `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`kafka-events`),` topic to set partitions to 3:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`bin/kafka-topics.sh --alter --topic kafka-events --partitions 3 --bootstrap-server=localhost:9092
`)),(0,esm/* mdx */.kt)("p",null,`Show all topics and partitions in Kafka:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe
`)),(0,esm/* mdx */.kt)("h2",{"id":"insert-into-tdengine"},`Insert into TDengine`),(0,esm/* mdx */.kt)("p",null,`We can write data into TDengine via SQL or Schemaless. For more information, please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/develop/insert-data/sql-writing/"},`Insert Using SQL`),` or `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/develop/insert-data/high-volume/"},`High Performance Writing`),` or `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/reference/schemaless/"},`Schemaless Writing`),`.`),(0,esm/* mdx */.kt)("h2",{"id":"examples"},`Examples`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"Python",groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"Python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"PyKafka"}))));};_20_kafka_writting_MDXContent.isMDXComponent=true;

/***/ })

}]);