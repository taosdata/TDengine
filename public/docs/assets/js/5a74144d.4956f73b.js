"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[3211],{

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

/***/ 2876:
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
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={sidebar_label:'EMQX Broker',title:'EMQX Broker 写入',description:'使用 EMQX Broker 写入 TDengine'};const contentTitle=undefined;const metadata={"unversionedId":"third-party/emq-broker","id":"third-party/emq-broker","title":"EMQX Broker 写入","description":"使用 EMQX Broker 写入 TDengine","source":"@site/docs/20-third-party/09-emq-broker.md","sourceDirName":"20-third-party","slug":"/third-party/emq-broker","permalink":"/docs/third-party/emq-broker","draft":false,"tags":[],"version":"current","sidebarPosition":9,"frontMatter":{"sidebar_label":"EMQX Broker","title":"EMQX Broker 写入","description":"使用 EMQX Broker 写入 TDengine"},"sidebar":"defaultSidebar","previous":{"title":"TCollector","permalink":"/docs/third-party/tcollector"},"next":{"title":"HiveMQ Broker","permalink":"/docs/third-party/hive-mq-broker"}};const assets={};const toc=[{value:'前置条件',id:'前置条件',level:2},{value:'安装并启动 EMQX',id:'安装并启动-emqx',level:2},{value:'创建数据库和表',id:'创建数据库和表',level:2},{value:'配置 EMQX 规则',id:'配置-emqx-规则',level:2},{value:'登录 EMQX Dashboard',id:'登录-emqx-dashboard',level:3},{value:'创建规则（Rule）',id:'创建规则rule',level:3},{value:'编辑 SQL 字段',id:'编辑-sql-字段',level:3},{value:'新增“动作（action handler）”',id:'新增动作action-handler',level:3},{value:'新增“资源（Resource）”',id:'新增资源resource',level:3},{value:'编辑“资源（Resource）”',id:'编辑资源resource',level:3},{value:'编辑“动作（action）”',id:'编辑动作action',level:3},{value:'编写模拟测试程序',id:'编写模拟测试程序',level:2},{value:'执行测试模拟发送 MQTT 数据',id:'执行测试模拟发送-mqtt-数据',level:2},{value:'验证 EMQX 接收到数据',id:'验证-emqx-接收到数据',level:2},{value:'验证数据写入到 TDengine',id:'验证数据写入到-tdengine',level:2}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`MQTT 是流行的物联网数据传输协议，`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/emqx/emqx"},`EMQX`),`是一开源的 MQTT Broker 软件，无需任何代码，只需要在 EMQX Dashboard 里使用“规则”做简单配置，即可将 MQTT 的数据直接写入 TDengine。EMQX 支持通过 发送到 Web 服务的方式保存数据到 TDengine，也在企业版上提供原生的 TDengine 驱动实现直接保存。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"前置条件"},`前置条件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`要让 EMQX 能正常添加 TDengine 数据源，需要以下几方面的准备工作。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`TDengine 集群已经部署并正常运行`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`taosAdapter 已经安装并正常运行。具体细节请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"li","href":"/reference/taosadapter"},`taosAdapter 的使用手册`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`如果使用后文介绍的模拟写入程序，需要安装合适版本的 Node.js，推荐安装 v12`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"安装并启动-emqx"},`安装并启动 EMQX`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`用户可以根据当前的操作系统，到 EMQX 官网下载安装包，并执行安装。下载地址如下：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.emqx.io/zh/downloads"},`https://www.emqx.io/zh/downloads`),`。安装后使用 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sudo emqx start`),` 或 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sudo systemctl start emqx`),` 启动 EMQX 服务。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：本文基于 EMQX v4.4.5 版本，其他版本由于相关配置界面、配置方法以及功能可能随着版本升级有所区别。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"创建数据库和表"},`创建数据库和表`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 TDengine 中为接收 MQTT 数据创建相应数据库和表结构。进入 TDengine CLI 复制并执行以下 SQL 语句：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`CREATE DATABASE test;
USE test;
CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, volume FLOAT, pm10 FLOAT, pm25 FLOAT, so2 FLOAT, no2 FLOAT, co FLOAT, sensor_id NCHAR(255), area TINYINT, coll_time TIMESTAMP);
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注：表结构以博客`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.taosdata.com/blog/2020/08/04/1722.html"},`数据传输、存储、展现，EMQX + TDengine 搭建 MQTT 物联网数据可视化平台`),`为例。后续操作均以此博客场景为例进行，请你根据实际应用场景进行修改。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"配置-emqx-规则"},`配置 EMQX 规则`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`由于 EMQX 不同版本配置界面所有不同，这里仅以 v4.4.5 为例，其他版本请参考相应官网文档。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"登录-emqx-dashboard"},`登录 EMQX Dashboard`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用浏览器打开网址 http://IP:18083 并登录 EMQX Dashboard。初次安装用户名为 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`admin`),` 密码为：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`public`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX login dashboard",src:(__webpack_require__(9280)/* ["default"] */ .Z),width:"1154",height:"826"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建规则rule"},`创建规则（Rule）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`选择左侧“规则引擎（Rule Engine）”中的“规则（Rule）”并点击“创建（Create）”按钮：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX rule engine",src:(__webpack_require__(5694)/* ["default"] */ .Z),width:"1029",height:"472"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"编辑-sql-字段"},`编辑 SQL 字段`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`复制以下内容输入到 SQL 编辑框：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`SELECT
  payload
FROM
  "sensor/data"
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`其中 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`payload`),` 代表整个消息体， `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`sensor/data`),` 为本规则选取的消息主题。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX create rule",src:(__webpack_require__(5084)/* ["default"] */ .Z),width:"1011",height:"838"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"新增动作action-handler"},`新增“动作（action handler）”`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX",src:(__webpack_require__(666)/* ["default"] */ .Z),width:"994",height:"641"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"新增资源resource"},`新增“资源（Resource）”`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX create resource",src:(__webpack_require__(3648)/* ["default"] */ .Z),width:"952",height:"724"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`选择“发送数据到 Web 服务”并点击“新建资源”按钮：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"编辑资源resource"},`编辑“资源（Resource）”`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`选择“WebHook”并填写“请求 URL”为 taosAdapter 提供 REST 服务的地址，如果是本地启动的 taosadapter， 那么默认地址为：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`http://127.0.0.1:6041/rest/sql
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`其他属性请保持默认值。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX edit resource",src:(__webpack_require__(6519)/* ["default"] */ .Z),width:"953",height:"881"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"编辑动作action"},`编辑“动作（action）”`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`编辑资源配置，增加 Authorization 认证的键/值配对项。默认用户名和密码对应的 Authorization 值为：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`Basic cm9vdDp0YW9zZGF0YQ==
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`相关文档请参考`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../connector/rest-api/"},` TDengine REST API 文档`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在消息体中输入规则引擎替换模板:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-sql"},`INSERT INTO test.sensor_data VALUES(
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX edit action",src:(__webpack_require__(2932)/* ["default"] */ .Z),width:"792",height:"897"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`最后点击左下方的 “Create” 按钮，保存规则。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"编写模拟测试程序"},`编写模拟测试程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-javascript"},`// mock.js
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/other/mock.js"},`查看源码`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注意：代码中 CLIENT_NUM 在开始测试中可以先设置一个较小的值，避免硬件性能不能完全处理较大并发客户端数量。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX client num",src:(__webpack_require__(4460)/* ["default"] */ .Z),width:"618",height:"342"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"执行测试模拟发送-mqtt-数据"},`执行测试模拟发送 MQTT 数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre"},`npm install mqtt mockjs --save --registry=https://registry.npm.taobao.org
node mock.js
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX run-mock",src:(__webpack_require__(2648)/* ["default"] */ .Z),width:"475",height:"278"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"验证-emqx-接收到数据"},`验证 EMQX 接收到数据`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 EMQX Dashboard 规则引擎界面进行刷新，可以看到有多少条记录被正确接收到：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX rule matched",src:(__webpack_require__(8567)/* ["default"] */ .Z),width:"1171",height:"560"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"验证数据写入到-tdengine"},`验证数据写入到 TDengine`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`使用 TDengine CLI 程序登录并查询相应数据库和表，验证数据是否被正确写入到 TDengine 中：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("img",{alt:"TDengine Database EMQX result in taos",src:(__webpack_require__(4542)/* ["default"] */ .Z),width:"966",height:"982"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`TDengine 详细使用方法请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://docs.taosdata.com/"},`TDengine 官方文档`),`。
EMQX 详细使用方法请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://www.emqx.io/docs/zh/v4.4/rule/rule-engine.html"},`EMQX 官方文档`),`。`));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 666:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/add-action-handler-9437833a9163aeaf8b74314d63214cf5.webp");

/***/ }),

/***/ 4542:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/check-result-in-taos-5fa480c54aba3dc85c8b84b1eed6a83d.webp");

/***/ }),

/***/ 8567:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/check-rule-matched-492e6d89f07f1343f9703dcea3ee3e48.webp");

/***/ }),

/***/ 4460:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ }),

/***/ 3648:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create-resource-1a1a334ea30ec38e5c9a0037d34cb23c.webp");

/***/ }),

/***/ 5084:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/create-rule-c545f5dda6e320f3a4f633c3201d40b2.webp");

/***/ }),

/***/ 2932:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/edit-action-15a55941fd29a45ee1e1dd61e28af321.webp");

/***/ }),

/***/ 6519:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/edit-resource-08b39118f50b95f2c53d1cb47d570564.webp");

/***/ }),

/***/ 9280:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/login-dashboard-f870e3a10aa55396d8cefda486804741.webp");

/***/ }),

/***/ 5694:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (__webpack_require__.p + "assets/images/rule-engine-41505bf44470b1087f8d6a626dc64120.webp");

/***/ }),

/***/ 2648:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,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");

/***/ })

}]);