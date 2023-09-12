"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[3211],{

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

/***/ 2876:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'EMQX Broker writing',sidebar_label:'EMQX Broker',description:'This document describes how to integrate TDengine with the EMQX broker.'};const contentTitle=undefined;const metadata={"unversionedId":"third-party/emq-broker","id":"third-party/emq-broker","title":"EMQX Broker writing","description":"This document describes how to integrate TDengine with the EMQX broker.","source":"@site/docs/20-third-party/09-emq-broker.md","sourceDirName":"20-third-party","slug":"/third-party/emq-broker","permalink":"/docs-en/third-party/emq-broker","draft":false,"tags":[],"version":"current","sidebarPosition":9,"frontMatter":{"title":"EMQX Broker writing","sidebar_label":"EMQX Broker","description":"This document describes how to integrate TDengine with the EMQX broker."},"sidebar":"defaultSidebar","previous":{"title":"TCollector","permalink":"/docs-en/third-party/tcollector"},"next":{"title":"HiveMQ Broker","permalink":"/docs-en/third-party/hive-mq-broker"}};const assets={};const toc=[{value:'Prerequisites',id:'prerequisites',level:2},{value:'Install and start EMQX',id:'install-and-start-emqx',level:2},{value:'Create Database and Table',id:'create-database-and-table',level:2},{value:'Configuring EMQX Rules',id:'configuring-emqx-rules',level:2},{value:'Login EMQX Dashboard',id:'login-emqx-dashboard',level:3},{value:'Creating Rule',id:'creating-rule',level:3},{value:'Edit SQL fields',id:'edit-sql-fields',level:3},{value:'Add &quot;action handler&quot;',id:'add-action-handler',level:3},{value:'Add &quot;Resource&quot;',id:'add-resource',level:3},{value:'Edit &quot;Resource&quot;',id:'edit-resource',level:3},{value:'Edit &quot;action&quot;',id:'edit-action',level:3},{value:'Compose program to mock data',id:'compose-program-to-mock-data',level:2},{value:'Execute tests to simulate sending MQTT data',id:'execute-tests-to-simulate-sending-mqtt-data',level:2},{value:'Verify that EMQX is receiving data',id:'verify-that-emqx-is-receiving-data',level:2},{value:'Verify that data writing to TDengine',id:'verify-that-data-writing-to-tdengine',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`MQTT is a popular IoT data transfer protocol. `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/emqx/emqx"},`EMQX`),` is an open-source MQTT Broker software. You can write MQTT data directly to TDengine without any code. You only need to setup "rules" in EMQX Dashboard to create a simple configuration. EMQX supports saving data to TDengine by sending data to a web service and provides a native TDengine driver for direct saving in the Enterprise Edition. Please refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.emqx.io/docs/en/v4.4/rule/rule-engine.html"},`EMQX official documentation`),` for details on how to use it.).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"prerequisites"},`Prerequisites`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following preparations are required for EMQX to add TDengine data sources correctly.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The TDengine cluster is deployed and working properly`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`taosAdapter is installed and running properly. Please refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"../../reference/taosadapter"},`taosAdapter manual`),` for details.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`If you use the emulated writers described later, you need to install the appropriate version of Node.js. V12 is recommended.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"install-and-start-emqx"},`Install and start EMQX`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Depending on the current operating system, users can download the installation package from the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.emqx.io/downloads"},`EMQX official website`),` and execute the installation. After installation, use `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sudo emqx start`),` or `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sudo systemctl start emqx`),` to start the EMQX service.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: this chapter is based on EMQX v4.4.5. Other version of EMQX probably change its user interface, configuration methods or functions.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"create-database-and-table"},`Create Database and Table`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`In this step we create the appropriate database and table schema in TDengine for receiving MQTT data. Open TDengine CLI and execute SQL bellow: `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE DATABASE test;
USE test;
CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, volume FLOAT, pm10 FLOAT, pm25 FLOAT, so2 FLOAT, no2 FLOAT, co FLOAT, sensor_id NCHAR(255), area TINYINT, coll_time TIMESTAMP);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"configuring-emqx-rules"},`Configuring EMQX Rules`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Since the configuration interface of EMQX differs from version to version, here is v4.4.5 as an example. For other versions, please refer to the corresponding official documentation.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"login-emqx-dashboard"},`Login EMQX Dashboard`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use your browser to open the URL `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`http://IP:18083`),` and log in to EMQX Dashboard. The initial installation username is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`admin`),` and the password is: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`public`),`.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX login dashboard",src:(__webpack_require__(3825)/* ["default"] */ .Z),width:"1154",height:"826"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"creating-rule"},`Creating Rule`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Select "Rule" in the "Rule Engine" on the left and click the "Create" button: !`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX rule engine",src:(__webpack_require__(4468)/* ["default"] */ .Z),width:"1029",height:"472"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"edit-sql-fields"},`Edit SQL fields`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Copy SQL bellow and paste it to the SQL edit area:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`SELECT
  payload
FROM
  "sensor/data"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX create rule",src:(__webpack_require__(2055)/* ["default"] */ .Z),width:"1011",height:"838"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"add-action-handler"},`Add "action handler"`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX add action handler",src:(__webpack_require__(2648)/* ["default"] */ .Z),width:"994",height:"641"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"add-resource"},`Add "Resource"`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX create resource",src:(__webpack_require__(5674)/* ["default"] */ .Z),width:"952",height:"724"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Select "Data to Web Service" and click the "New Resource" button.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"edit-resource"},`Edit "Resource"`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Select "WebHook" and fill in the request URL as the address and port of the server running taosAdapter (default is 6041). Leave the other properties at their default values.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX edit resource",src:(__webpack_require__(236)/* ["default"] */ .Z),width:"953",height:"881"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"edit-action"},`Edit "action"`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Edit the resource configuration to add the key/value pairing for Authorization. If you use the default TDengine username and password then the value of key Authorization is:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`Basic cm9vdDp0YW9zZGF0YQ==
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"/reference/rest-api/"},` TDengine REST API documentation `),` for the authorization in details. `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Enter the rule engine replacement template in the message body:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`INSERT INTO test.sensor_data VALUES(
  now,
  \${payload.temperature},
  \${payload.humidity},
  \${payload.volume},
  \${payload.PM10},
  \${payload.pm25},
  \${payload.SO2},
  \${payload.NO2},
  \${payload.CO},
  '\${payload.id}',
  \${payload.area},
  \${payload.ts}
)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX edit action",src:(__webpack_require__(2828)/* ["default"] */ .Z),width:"792",height:"897"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Finally, click the "Create" button at bottom left corner saving the rule.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"compose-program-to-mock-data"},`Compose program to mock data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-javascript"},`// mock.js
const mqtt = require('mqtt')
const Mock = require('mockjs')
const EMQX_SERVER = 'mqtt://localhost:1883'
const CLIENT_NUM = 10
const STEP = 5000 // Data interval in ms
const AWAIT = 5000 // Sleep time after data be written once to avoid data writing too fast
const CLIENT_POOL = []
startMock()
function sleep(timer = 100) {
  return new Promise(resolve => {
    setTimeout(resolve, timer)
  })
}
async function startMock() {
  const now = Date.now()
  for (let i = 0; i < CLIENT_NUM; i++) {
    const client = await createClient(\`mock_client_\${i}\`)
    CLIENT_POOL.push(client)
  }
  // last 24h every 5s
  const last = 24 * 3600 * 1000
  for (let ts = now - last; ts <= now; ts += STEP) {
    for (const client of CLIENT_POOL) {
      const mockData = generateMockData()
      const data = {
        ...mockData,
        id: client.clientId,
        area: 0,
        ts,
      }
      client.publish('sensor/data', JSON.stringify(data))
    }
    const dateStr = new Date(ts).toLocaleTimeString()
    console.log(\`\${dateStr} send success.\`)
    await sleep(AWAIT)
  }
  console.log(\`Done, use \${(Date.now() - now) / 1000}s\`)
}
/**
 * Init a virtual mqtt client
 * @param {string} clientId ClientID
 */
function createClient(clientId) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(EMQX_SERVER, {
      clientId,
    })
    client.on('connect', () => {
      console.log(\`client \${clientId} connected\`)
      resolve(client)
    })
    client.on('reconnect', () => {
      console.log('reconnect')
    })
    client.on('error', (e) => {
      console.error(e)
      reject(e)
    })
  })
}
/**
* Generate mock data
*/
function generateMockData() {
 return {
   "temperature": parseFloat(Mock.Random.float(22, 100).toFixed(2)),
   "humidity": parseFloat(Mock.Random.float(12, 86).toFixed(2)),
   "volume": parseFloat(Mock.Random.float(20, 200).toFixed(2)),
   "PM10": parseFloat(Mock.Random.float(0, 300).toFixed(2)),
   "pm25": parseFloat(Mock.Random.float(0, 300).toFixed(2)),
   "SO2": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "NO2": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "CO": parseFloat(Mock.Random.float(0, 50).toFixed(2)),
   "area": Mock.Random.integer(0, 20),
   "ts": 1596157444170,
 }
}
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/other/mock.js"},`view source code`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Note: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`CLIENT_NUM`),` in the code can be set to a smaller value at the beginning of the test to avoid hardware performance be not capable to handle a more significant number of concurrent clients.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX client num",src:(__webpack_require__(5463)/* ["default"] */ .Z),width:"1040",height:"454"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"execute-tests-to-simulate-sending-mqtt-data"},`Execute tests to simulate sending MQTT data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`npm install mqtt mockjs --save ---registry=https://registry.npm.taobao.org
node mock.js
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX run mock",src:(__webpack_require__(7024)/* ["default"] */ .Z),width:"475",height:"278"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"verify-that-emqx-is-receiving-data"},`Verify that EMQX is receiving data`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Refresh the EMQX Dashboard rules engine interface to see how many records were received correctly:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX rule matched",src:(__webpack_require__(4207)/* ["default"] */ .Z),width:"1171",height:"560"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"verify-that-data-writing-to-tdengine"},`Verify that data writing to TDengine`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Use the TDengine CLI program to log in and query the appropriate databases and tables to verify that the data is being written to TDengine correctly:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX result in taos",src:(__webpack_require__(6267)/* ["default"] */ .Z),width:"966",height:"982"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Please refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.tdengine.com/"},`TDengine official documentation`),` for more details on how to use TDengine.
EMQX Please refer to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.emqx.io/docs/en/v4.4/rule/rule-engine.html"},`EMQX official documentation`),` for details on how to use EMQX.`));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 2648:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add-action-handler-9437833a9163aeaf8b74314d63214cf5.webp");

/***/ }),

/***/ 6267:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/check-result-in-taos-5fa480c54aba3dc85c8b84b1eed6a83d.webp");

/***/ }),

/***/ 4207:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/check-rule-matched-492e6d89f07f1343f9703dcea3ee3e48.webp");

/***/ }),

/***/ 5463:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/client-num-362987a93d8cda61835ba5bfc41df912.webp");

/***/ }),

/***/ 5674:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create-resource-1a1a334ea30ec38e5c9a0037d34cb23c.webp");

/***/ }),

/***/ 2055:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create-rule-c545f5dda6e320f3a4f633c3201d40b2.webp");

/***/ }),

/***/ 2828:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/edit-action-15a55941fd29a45ee1e1dd61e28af321.webp");

/***/ }),

/***/ 236:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/edit-resource-08b39118f50b95f2c53d1cb47d570564.webp");

/***/ }),

/***/ 3825:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/login-dashboard-f870e3a10aa55396d8cefda486804741.webp");

/***/ }),

/***/ 4468:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/rule-engine-41505bf44470b1087f8d6a626dc64120.webp");

/***/ }),

/***/ 7024:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ })

}]);