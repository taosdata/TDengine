"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[6062],{

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

/***/ 3186:
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
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={toc_max_heading_level:4,title:'可视化管理'};const contentTitle=undefined;const metadata={"unversionedId":"enterprise/explorer","id":"enterprise/explorer","title":"可视化管理","description":"简介","source":"@site/docs/30-enterprise/12-explorer.md","sourceDirName":"30-enterprise","slug":"/enterprise/explorer","permalink":"/docs/enterprise/explorer","draft":false,"tags":[],"version":"current","sidebarPosition":12,"frontMatter":{"toc_max_heading_level":4,"title":"可视化管理"},"sidebar":"defaultSidebar","previous":{"title":"边云协同","permalink":"/docs/enterprise/edge"},"next":{"title":"审计日志","permalink":"/docs/enterprise/audit"}};const assets={};const toc=[{value:'简介',id:'简介',level:2},{value:'部署服务',id:'部署服务',level:2},{value:'登录',id:'登录',level:2},{value:'面板',id:'面板',level:2},{value:'数据浏览器',id:'数据浏览器',level:2},{value:'系统管理',id:'系统管理',level:2},{value:'用户管理',id:'用户管理',level:3},{value:'系统信息',id:'系统信息',level:3},{value:'许可证管理',id:'许可证管理',level:3},{value:'数据写入',id:'数据写入',level:2},{value:'TDengine 订阅',id:'tdengine-订阅',level:3},{value:'Pi',id:'pi',level:3},{value:'OPC-UA',id:'opc-ua',level:3},{value:'OPC-DA',id:'opc-da',level:3},{value:'InfluxDB',id:'influxdb',level:3},{value:'MQTT',id:'mqtt',level:3},{value:'Kafka',id:'kafka',level:3},{value:'CSV',id:'csv',level:3},{value:'备份和恢复',id:'备份和恢复',level:2},{value:'备份数据到本地文件',id:'备份数据到本地文件',level:3},{value:'从本地文件恢复',id:'从本地文件恢复',level:3},{value:'数据订阅',id:'数据订阅',level:2},{value:'创建主题',id:'创建主题',level:3},{value:'分享主题',id:'分享主题',level:3},{value:'查看消费者信息',id:'查看消费者信息',level:3},{value:'示例代码',id:'示例代码',level:3},{value:'流计算',id:'流计算',level:2},{value:'流计算向导',id:'流计算向导',level:3},{value:'使用 SQL 语句建流',id:'使用-sql-语句建流',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"简介"},`简介`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`为了易于企业版用户更容易使用和管理数据库，TDengine 3.0 企业版提供了一个全新的可视化组件 taosExplorer。用户能够在其中方便地管理数据库管理系统中中各元素（数据库、超级表、子表）的生命周期，执行查询，监控系统状态，管理用户和授权，完成数据备份和恢复，与其它集群之间进行数据同步，导出数据，管理主题和流计算。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"部署服务"},`部署服务`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`详情请参考 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"../../get-started"},`部署服务`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"登录"},`登录`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 TDengine 管理系统的登录页面，输入正确的用户名和密码后，点击登录按钮，即可登录。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`说明：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`这里的用户，需要在所连接的 TDengine 中创建，TDengine 默认的用户名和密码为`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`root/taosdata`),`;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`在 TDengine 中创建用户时，默认会设置用户的 SYSINFO 属性值为1, 表示该用户可以查看系统信息，只有 SYSINFO 属性为 1 的用户才能正常登录 TDengine 管理系统。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"面板"},`面板`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`taosExplorer 内置了一个简单的仪表盘展示以下集群信息，点击左侧功能列表中的 "面板" 可以启用此功能。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`默认的仪表盘会返回对应 Grafana 的安装配置向导`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`配置过 Grafana 的仪表盘在点击' 面板' 时会跳转到对应的配置地址（该地址来源于 /profile 接口的返回值）`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"数据浏览器"},`数据浏览器`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击功能列表的“数据浏览器”入口，在“数据浏览器”中可以创建和删除数据库、创建和删除超级表和子表，执行SQL语句，查看SQL语句的执行结果。此外，超级管理员还有对数据库的管理权限，其他用户不提供该功能。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`具体权限有：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`1.查看（提供数据库/超级表/普通表的基本信息）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`2.编辑 (编辑数据库/超级表/普通表的信息)`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`3.数据库管理权限 （仅限超级管理员，该操作可以给指定用户配置数据库管理权限）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`4.删除 （删除数据库/超级表/普通表）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`5.追加 （选择对应的数据库/超级表/普通表名称直接追加到右侧sql输入区域，避免了手工输入）`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"系统管理"},`系统管理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击功能列表中的“系统管理”入口，可以创建用户、对用户进行访问授权、以及删除用户。还能够对当前所管理的集群中的数据进行备份和恢复。也可以配置一个远程 TDengine 的地址进行数据同步。同时也提供了集群信息和许可证的信息以及代理信息以供查看。系统管理 菜单只有 root 用户才有权限看到`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"用户管理"},`用户管理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击“系统管理”后，默认会进入“用户”标签页。
在用户列表，可以查看系统中已存在的用户及其创建时间，并可以对用户进行启用、禁用，编辑（包括修改密码，数据库的读写权限等），删除等操作。
点击用户列表右上方的“+新增”按钮，即可打开“新增用户”对话框：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`输入新增用户的用户名称，必填`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`输入新增用户的登录密码，必填，密码长度要求为8-16个字符，且至少要满足以下4个条件中的3个：大写字母，小写字母，数字，特殊字符`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`选择新增用户对系统中已存在的数据库的读写权限，非必填，默认情况下，新增用户对所有已存在的数据库无读写权限`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`提写完成后，点击确定按钮，即可新增用户。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"系统信息"},`系统信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击“集群”标签后，可以查看DNodes, MNodes和QNodes的状态、创建时间等信息，并可以对以上节点进行新增和删除操作。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"许可证管理"},`许可证管理`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击“许可证”标签后，可以查看系统和系统和各连接器的许可证信息。
点击位于“许可证”标签页右上角的“激活许可证”按钮，输入“激活码”和“连接器激活码”后，点击“确定”按钮，即可激活，激活码请联系 TDengine 客户成功团队获取。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"数据写入"},`数据写入`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`点击功能列表中的 "数据写入"，可以配置不同类型的数据源，包括 TDengine Subscription, PI, OPC-UA, OPC-DA, InfluxDB, MQTT，Kafka, CSV 等，将它们的数据写入到当前正在被管理的 TDengine 集群中。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"tdengine-订阅"},`TDengine 订阅`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`进入TDengine订阅任务配置页面：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在连接协议栏中，配置连接协议，默认为原生连接，可配置为WS、WSS；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在服务器栏中配置服务器的 IP 或域名；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在端口栏中配置连接的端口号，默认值为6030；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在主题栏中，配置可以配置订阅一个或多个数据库，或超级表或普通表，也可以是一个已创建的 Topic；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在认证栏，可以配置访问 TDengine 的用户名密码，用户名默认值为 root，密码默认值为 taosdata；如果数据源为云服务实例，则可以选择令牌认证方式并配置实例 token；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在订阅初始位置栏，可配置从最早数据（earliest）或最晚（latest）数据开始订阅，默认为 earliest；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在超时栏配置超时时间，可配置为 never: 表示无超时时间，持续进行订阅，也可指定超时时间：5s, 1m 等，支持单位 ms（毫秒），s（秒），m（分钟），h（小时），d（天），M（月），y（年）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库栏中，选择本地 TDengine 的库作为目标库，点击 submit，即可启动一个 TDengine 订阅任务。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"pi"},`Pi`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 PI 数据接入页面，设置 PI 服务器的名称、AF 数据库名称。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在监测点集栏，可以配置选择 Point 模式监测点集合、Point 模式监测的 AF 模板、AF 模式监测的 AF 模板。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 PI 系统设置栏，可以配置 PI 系统名，默认为 PI 服务器名。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 Data Queue 栏，可以配置 PI 连接器运行参数：MaxWaitLen（数据最大缓冲条数），默认值为 1000 ,有效取值范围为 `,`[1,10000]`,`；UpdateInterval（PI System 取数据频率），默认值为 10000(毫秒：ms),有效取值范围为 `,`[10,600000]`,`；重启补偿时间（Max Backfill Range，单位：天），每次重启服务时向前补偿该天数的数据，默认为1天。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库栏，选择需要写入的 TDengine 数据库，点击 submit ，即可启动一个 PI 数据接入任务。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"opc-ua"},`OPC-UA`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 OPC-UA页面，配置 OPC-server 的地址，输入格式为 127.0.0.1:6666/OPCUA/ServerPath。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在认证栏，选择访问方式。可以选择匿名访问、用户名密码访问、证书访问。使用证书访问时，需配置证书文件信息、私钥文件信息、OPC-UA 安全协议和 OPC-UA 安全策略`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 Data Sets 栏，配置点位信息。(可通过“选择”按钮选择正则表达式过滤点位，每次最多能过滤出10条点位)；点位配置有两种方式：1.手动输入点位信息 2.上传csv文件配置点位信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在连接配置栏，配置连接超时间隔和采集超时间隔（单位：秒），默认值为10秒。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在采集配置栏，配置采集间隔（单位：秒）、点位数量、采集模式。采集模式可选择observe（轮询模式）和subscribe（订阅模式），默认值为observe。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在库表配置栏，配置目标 TDengine 中存储数据的超级表、子表结构信息。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在其他配置栏，配置并行度、单次采集上报批次（默认值100）、上报超时时间（单位：秒，默认值10）、是否开启debug级别日志。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库栏，选择需要写入的 TDengine 数据库，点击 submit，即可启动一个 OPC-UA 数据接入任务。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"opc-da"},`OPC-DA`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 OPC-DA页面，配置 OPC-server 的地址，输入格式为 127.0.0.1<,localhost>/Matrikon.OPC.Simulation.1。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在数据点栏，配置 OPC-DA 采集点信息。(可通过“选择”按钮选择正则表达式过滤点位，每次最多能过滤出10条点位)。点位配置有两种方式：1.手动输入点位信息 2.上传csv文件配置点位信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在连接栏，配置连接超时时间（单位：秒，默认值为10秒）、采集超时时间（单位：秒，默认值为10秒）。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在库表配置栏，配置目标 TDengine 中存储数据的超级表、子表结构信息。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在其他配置栏，配置并行度、单次采集上报批次（默认值100）、上报超时时间（单位：秒，默认值10）、是否开启debug级别日志。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库栏，选择需要写入的 TDengine 数据库，点击 submit，即可启动一个 OPC-DA 数据接入任务。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"influxdb"},`InfluxDB`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`进入 InfluxDB 数据源同步任务的编辑页面后：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在服务器地址输入框, 输入 InfluxDB 服务器的地址，可以输入 IP 地址或域名，此项为必填字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在端口输入框, 输入 InfluxDB 服务器端口，默认情况下，InfluxDB 监听8086端口的 HTTP 请求和8088端口的 HTTPS 请求，此项为必填字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在组织 ID 输入框，输入将要同步的组织 ID，此项为必填字段;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在令牌 Token 输入框，输入一个至少拥有读取这个组织 ID 下的指定 Bucket 权限的 Token, 此项为必填字段;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在同步设置的起始时间项下，通过点选选择一个同步数据的起始时间，起始时间使用 UTC 时间， 此项为必填字段;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在同步设置的结束时间项下，当不指定结束时间时，将持续进行最新数据的同步；当指定结束时间时，将只同步到这个结束时间为止; 结束时间使用 UTC 时间，此项为可选字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在桶 Bucket 输入框，输入一个需要同步的 Bucket，目前只支持同步一个 Bucket 至 TDengine 数据库，此项为必填字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库下拉列表，选择一个将要写入的 TDengine 目标数据库 （注意：目前只支持同步到精度为纳秒的 TDengine 目标数据库），此项为必填字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`填写完成以上信息后，点击提交按钮，即可直接启动从 InfluxDB 到 TDengine 的数据同步。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"mqtt"},`MQTT`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`进入 MQTT 数据源同步任务的编辑页面后：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 MQTT 地址卡片，输入 MQTT 地址，必填字段，包括 IP 和 端口号，例如：192.168.1.10:1883;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在认证卡片，输入 MQTT 连接器访问 MQTT 服务器时的用户名和密码，这两个字段为选填字段，如果未输入，即采用匿名认证的方式；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在 SSL 证书卡片，可以选择是否打开 SSL/TLS 开关，如果打开此开关，MQTT 连接器和 MQTT 服务器之间的通信将采用 SSL/TLS 的方式进行加密；打开这个开关后，会出现 CA, 客户端证书和客户端私钥三个必填配置项，可以在这里输入证书和私钥文件的内容；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在连接卡片，可以配置以下信息：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`MQTT 协议：支持3.1/3.1.1/5.0三个版本；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Client ID: MQTT 连接器连接 MQTT 服务器时所使用的客户端 ID, 用于标识客户端的身份；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Keep Alive: 用于配置 MQTT 连接器与 MQTT 服务器之间的Keep Alive时间，默认值为60秒；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Clean Session: 用于配置 MQTT 连接器是否以Clean Session的方式连接至 MQTT 服务器，默认值为True;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`订阅主题及 QoS 配置：这里用来配置监听的 MQTT 主题，以及该主题支持的最大QoS, 主题和 QoS 的配置之间用::分隔，多个主题之间用,分隔，主题的配置可以支持 MQTT 协议的通配符#和+;`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在其他卡片，可以配置 MQTT 连接器的日志级别，支持 error, warn, info, debug, trace 5个级别，默认值为 info;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`MQTT Payload 解析卡片，用于配置如何解析 MQTT 消息：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`配置表的第一行为 ts 字段，该字段为 TIMESTAMP 类型，它的值为 MQTT 连接器收到 MQTT 消息的时间；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`配置表的第二行为 topic 字段，为该消息的主题名称，可以选择将该字段作为列或者标签同步至 TDengine;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`配置表的第三行为 qos 字段，为该消息的 QoS 属性，可以选择将该字段作为列或者标签同步至 TDengine;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`剩余的配置项皆为自定义字段，每个字段都需要配置：字段（来源），列（目标），列类型（目标）。字段（来源）是指该 MQTT 消息中的字段名称，当前仅支持 JSON 类型的 MQTT 消息同步，可以使用 JSON Path 语法从 MQTT 消息中提取字段，例如：$.data.id; 列（目标）是指同步至 TDengine 后的字段名称；列类型（目标）是指同步至 TDengine 后的字段类型，可以从下拉列表中选择；当且仅当以上3个配置都填写后，才能新增下一个字段；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`如果 MQTT 消息中包含时间戳，可以选择新增一个自定义字段，将其作为同步至 TDengine 时的主键；需要注意的是，MQTT 消息中时间戳的仅支持 Unix Timestamp格式，且该字段的列类型（目标）的选择，需要与创建 TDengine 数据库时的配置一致；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`子表命名规则：用于配置子表名称，采用“前缀+{列类型(目标)}”的格式，例如：d{id};`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`超级表名：用于配置同步至 TDengine 时，采用的超级表名；`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库卡片，可以选择同步至 TDengine 的数据库名称，支持直接从下拉列表中选择。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`填写完成以上信息后，点击提交按钮，即可直接启动从 MQTT 到 TDengine 的数据同步。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"kafka"},`Kafka`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在Kafka页面，配置Kafka选项，必填字段，包括：bootstrap_server，例如192.168.1.92:9092；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如果使用SSL认证，在SSL认证卡中，选择cert和cert_key的文件路径；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`配置其他参数，topics、topic_partitions这2个参数至少填写一个，其他参数有默认值；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如果消费的Kafka数据是JSON格式，可以配置parser卡片，对数据进行解析转换；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库卡片中，选择同步到TDengine的数据库名称，支持从下拉列表中选择；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`填写完以上信息后，点击提交按钮，即可启动从Kafka到TDengine的数据同步。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"csv"},`CSV`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在CSV页面，配置CSV选项，可设置忽略前N行，可输入具体的数字`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`CSV的写入配置，设置批次写入量，默认是1000`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`CSV文件解析，用于获取CSV对应的列信息：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`上传CSV文件或者输入CSV文件的地址`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`选择是否包包含Header`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`包含Header情况下直接执行下一步，查询出对应CSV的列信息，获取CSV的配置信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`不包含Header情况，需要输入自定列信息，并以逗号分隔，然后下一步，获取CSV的配置信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`CSV的配置项，每个字段都需要配置：CSV列，DB列，列类型（目标），主键(整个配置只能有一个主键，且主键必须是TIMESTAMP类型)，作为列，作为Tag。CSV列是指该 CSV文件中的列或者自定义的列；DB列是对应的数据表的列`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`子表命名规则：用于配置子表名称，采用“前缀+{列类型(目标)}”的格式，例如：d{id};`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`超级表名：用于配置同步至 TDengine 时，采用的超级表名；`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在目标数据库卡片，可以选择同步至 TDengine 的数据库名称，支持直接从下拉列表中选择。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`填写完成以上信息后，点击提交按钮，即可直接启动从 CSV到 TDengine 的数据同步。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"备份和恢复"},`备份和恢复`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`您可以将当前连接的 TDengine 集群中的数据备份至一个或多个本地文件中，稍后可以通过这些文件进行数据恢复。本章节将介绍数据备份和恢复的具体步骤。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"备份数据到本地文件"},`备份数据到本地文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`进入系统管理页面，点击【备份】进入数据备份页面，点击右上角【新增备份】。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在数据备份配置页面中可以配置三个参数：`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`备份周期：必填项，配置每次执行数据备份的时间间隔，可通过下拉框选择每天、每 7 天、每 30 天执行一次数据备份，配置后，会在对应的备份周期的0:00时启动一次数据备份任务；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`数据库：必填项，配置需要备份的数据库名（数据库的 wal_retention_period 参数需大于0）；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`目录：必填项，配置将数据备份到 taosX 所在运行环境中指定的路径下，如 /root/data_backup；`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":3},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`点击【确定】，可创建数据备份任务。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"从本地文件恢复"},`从本地文件恢复`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`完成数据备份任务创建后，在页面中对应的数据备份任务右侧点击【数据恢复】，可将已经备份到指定路径下的数据恢复到当前 TDengine 中。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"数据订阅"},`数据订阅`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`本章节，将介绍如何在 TDengine 集群中，创建主题，并将其分享给其他用户，以及如何查看一个主题的消费者信息。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`通过 Explorer, 您可以轻松地完成对数据订阅的管理，从而更好地利用 TDengine 提供的数据订阅能力。
点击左侧导航栏中的“数据订阅”，即可跳转至数据订阅配置管理页面。
您可以通过以下两种方式创建主题：使用向导和自定义 SQL 语句。通过自定义 SQL 创建主题时，您需要了解 TDengine 提供的数据订阅 SQL 语句的语法，并保证其正确性。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注： 对于数据订阅的详细说明，可参考官方文档中关于“数据订阅”章节，创建数据订阅之前需要先准备源数据库（或源数据库包含相应的超级表或者表），其中源数据库需配置wal_retention_period > 0 。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`包括主题，消费者，共享主题和示例代码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"创建主题"},`创建主题`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“主题”标签页，点击“新增新主题”按钮以后，选择向导窗格，然后输入“主题名称”；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“数据库”下拉列表中，选择相应的数据库；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“类型”标签下，选择“数据库” 或 “超级表” 或 “子查询”，这里以默认值“数据库”为例；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`然后点击“创建” 按钮，即可创建对应的主题。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"分享主题"},`分享主题`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“共享主题”标签页，在“主题“下拉列表中，选择将要分享的主题；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`点击“添加可消费该主题的用户”按钮，然后在“用户名”下拉列表中选择相应的用户，然后点击“新增”，即可将该主题分享给此用户。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"查看消费者信息"},`查看消费者信息`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`通过执行下一节“示例代码”所述的“完整实例”，即可消费共享主题`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“消费者”标签页，可查看到消费者的有关信息`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"示例代码"},`示例代码`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“示例代码”标签页，在“主题“下拉列表中，选择相应的主题；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`选择您熟悉的语言，然后您可以阅读以及使用这部分示例代码用来”创建消费“，”订阅主题“，通过执行 “完整实例”中的程序即可消费共享主题`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"流计算"},`流计算`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`通过 Explorer, 您可以轻松地完成对流的管理，从而更好地利用 TDengine 提供的流计算能力。
点击左侧导航栏中的“流计算”，即可跳转至流计算配置管理页面。
您可以通过以下两种方式创建流：流计算向导和自定义 SQL 语句。当前，通过流计算向导创建流时，暂不支持分组功能。通过自定义 SQL 创建流时，您需要了解 TDengine 提供的流计算 SQL 语句的语法，并保证其正确性。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`注： 对于流计算的详细说明，可参考官方文档中关于“流式计算”章节，创建流计算之前需要先准备源数据库以及相应的超级表或表、输出的数据库。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"流计算向导"},`流计算向导`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`点击“创建流计算”按钮以后，选择流计算向导窗格，然后输入“流名称”；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“输出”部分，输入相应的“数据库”，“超级表”以及“子表前缀”；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`在“源”部分，选择相应的“数据库”，然后根据具体情况，选择使用“超级表”或“表”：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如果使用“超级表“，请从“超级表”下拉列表中选择相应的超级表, 并在“字段设置”区域，选择相应的字段`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如果使用“表“，请从“表”下拉列表中选择相应的表, 并在“字段设置”区域，选择相应的字段`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`对于窗口设置，根据需要选择”SESSION“, "STATE"或"INTERVAL", 并配置相应的值；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`对于”执行“部分，选择相应的”触发器“类型，并设置“Watermark”, "Ignore Expired", "DELETE_MARK", "FILL_HISTORY", "IGNORE UPDATE"；`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`然后点击“创建” 按钮，即可创建对应的流计算。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"使用-sql-语句建流"},`使用 SQL 语句建流`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`点击“创建流计算”按钮以后，选择流计算SQL窗格，然后输入类似如下的SQL语句(反引号内为源数据库以及相应的超级表或表、输出的数据库，请按您的环境更新反引号内的内容)`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-shell"},`CREATE STREAM \`test_stream\` TRIGGER WINDOW_CLOSE IGNORE EXPIRED 1 INTO \`db_name\`.\`stable1\` SUBTABLE(CONCAT('table1',tbname)) AS SELECT count(*) FROM \`test_db\`.\`stable_name\` PARTITION BY tbname INTERVAL(1m)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`点击“创建”按钮，即可创建对应的流计算。`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);