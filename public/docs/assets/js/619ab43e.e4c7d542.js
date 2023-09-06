"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9941],{

/***/ 2236:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {


// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "Z": () => (/* binding */ components_PkgListV3)
});

// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/react-cookies/build/cookie.js
var cookie = __webpack_require__(1768);
;// CONCATENATED MODULE: ./components/components/popupv3.js
//邮箱正则验证
function validateEmail(email){var reg=/^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?/g;return reg.test(email);}/* harmony default export */ const popupv3 = (class extends react.Component{constructor(props){super(props);this.state={isShow:true,sucessMsg:''};}closeBtn(val){this.setState({isShow:true});this.props.pfn(val);//这个地方把值传递给了props的事件当中
}isShowSuccess(sucessMsg){this.setState({isShow:false});this.setState({sucessMsg:sucessMsg});}render(){return/*#__PURE__*/react.createElement("div",{className:this.props.hidden?"popup popup-hidden":"popup"},/*#__PURE__*/react.createElement("div",{className:"popup-container"},/*#__PURE__*/react.createElement("div",{className:this.state.isShow?"display-is-block":"display-is-none"},/*#__PURE__*/react.createElement("div",{className:"popup-title"},/*#__PURE__*/react.createElement("div",{className:"popup-title-text"},"\u4E0B\u8F7D TDengine"),/*#__PURE__*/react.createElement("div",{className:"close-popup",onClick:this.closeBtn.bind(this,this.props.hidden)},/*#__PURE__*/react.createElement("img",{src:(__webpack_require__(8354)/* ["default"] */ .Z),alt:"TDengine Database"}))),/*#__PURE__*/react.createElement("div",{className:"popup-content"},/*#__PURE__*/react.createElement(SubScription,{pkg:this.props.pkg,pkgName:this.props.pkgName,isShowSuccess:this.isShowSuccess.bind(this)}))),/*#__PURE__*/react.createElement("div",{className:this.state.isShow?"display-is-none":"display-is-block"},/*#__PURE__*/react.createElement("div",{style:{diaplay:'block',width:'90%',margin:'0 auto',padding:'1rem'}},/*#__PURE__*/react.createElement("div",{className:"success-msg"},this.state.sucessMsg),/*#__PURE__*/react.createElement("button",{className:"btn btn-primary",onClick:this.closeBtn.bind(this,this.props.hidden)},"\u5173\u95ED")))));}});// 订阅弹窗
class SubScription extends react.Component{constructor(props){super(props);this.state={email:'',email_value:cookie/* default.load */.ZP.load('email')?cookie/* default.load */.ZP.load('email'):'',message:'',showMessage:false,sucessMsg:"成功把链接发到您的邮箱",lang:'cn',can_contact:true,show_notice:false};}download(){if(!this.state.can_contact){this.setState({show_notice:true});return false;}this.setState({show_notice:false});console.log(this.state.lang);let email=this.state.email.value;this.state.email_value=email;if(email==""){this.state.email.focus();this.setState({message:"请输入邮件地址"});this.setState({showMessage:true});return false;}else if(!validateEmail(email)){this.state.email.focus();this.setState({message:"电子邮件不正确"});this.setState({showMessage:true});return false;}cookie/* default.save */.ZP.save('email',email,{domain:'taosdata.com',path:"/",expires:new Date(Date.now()+1000*60*60*24*30)});let postData={"email":email,"pkg":this.props.pkg,"lang":this.state.lang,"can_contact":this.state.can_contact,"pkgName":this.props.pkgName};console.log(postData);console.log(this.state.can_contact);let formData=new FormData();for(let key in postData){formData.append(key,postData[key]);}this.props.isShowSuccess("Please wait...");fetch('https://docs.taosdata.com/assets/globalscripts/generatelink_v3.php',{method:'post',body:formData}).then(response=>{return response.json();}).then(data=>{if(data[0].status=='Success'){this.props.isShowSuccess(this.state.sucessMsg);}}).catch(function(error){console.log(error);});}handleChange(event){this.setState({lang:event.target.value});}handleCheckBox(event){this.setState({can_contact:!this.state.can_contact});}render(){return/*#__PURE__*/react.createElement("div",null,/*#__PURE__*/react.createElement("div",{style:{diaplay:'block',width:'90%',margin:'0 auto',position:'relative'}},/*#__PURE__*/react.createElement("div",{className:this.state.showMessage?"popalert":"popalert popalert-hidden"},this.state.message),/*#__PURE__*/react.createElement("div",{style:{height:'2.1rem'}},"\u8F93\u5165\u60A8\u7684\u7535\u5B50\u90AE\u7BB1\u4EE5\u63A5\u6536\u4E0B\u8F7D\u94FE\u63A5"),/*#__PURE__*/react.createElement("input",{ref:el=>this.state.email=el,value:this.state.email_value,onChange:event=>{this.setState({email_value:this.state.email.value});},className:"sub-scription-input",placeholder:"\u8BF7\u8F93\u5165\u60A8\u7684\u90AE\u7BB1",required:true,type:"eamil"}),/*#__PURE__*/react.createElement("input",{type:"checkbox",onChange:this.handleCheckBox.bind(this),defaultChecked:this.state.can_contact})," \u540C\u610F\u6D9B\u601D\u6570\u636E\u901A\u8FC7\u6B64\u90AE\u4EF6\u5730\u5740\u8054\u7CFB\u6211",/*#__PURE__*/react.createElement("p",{style:{display:this.state.show_notice?"block":"none","font-size":"14px"}},"\u8BF7\u52FE\u9009\u540C\u610F\uFF0C\u4FBF\u4E8E\u6211\u4EEC\u901A\u8FC7\u90AE\u4EF6\u53D1\u9001\u5B89\u88C5\u5305\u7ED9\u60A8\u3002"),/*#__PURE__*/react.createElement("button",{className:"btn btn-primary",onClick:this.download.bind(this)},"\u4E0B\u8F7D")));}}
;// CONCATENATED MODULE: ./components/PkgListV3.js
/**
 * type: 0 - server; 1 - client; 2 - tools;
 */function PkgListV3(props){// const [pkgs, setPkgs] = useState([{"name":"TDengine-server-2.4.0.18-Linux-x64.rpm","size":"14.4 M","id":"tdengine_rpm"},{"name":"TDengine-server-2.4.0.18-Linux-x64.deb","size":"12.7 M","id":"tdengine_deb"},{"name":"TDengine-server-2.4.0.18-Linux-x64.tar.gz","size":"44.5 M","id":"tdengine_tar"},{"name":"TDengine-server-2.4.0.18-Linux-x64-Lite.tar.gz","size":"3.4 M","id":"tdengine_Lite_tar"},{"name":"TDengine-server-2.3.5.0-beta-Linux-x64-Lite.tar.gz","size":"3 M","id":"tdengine_Lite_beta_tar"},{"name":"TDengine-server-2.3.5.0-beta-Linux-x64.rpm","size":"18.4 M","id":"tdengine_beta_rpm"},{"name":"TDengine-server-2.3.5.0-beta-Linux-x64.deb","size":"16.8 M","id":"tdengine_beta_deb"},{"name":"TDengine-server-2.3.5.0-beta-Linux-x64.tar.gz","size":"18.8 M","id":"tdengine_beta_tar"}]);
const[pkgs,setPkgs]=(0,react.useState)([]);const[popState,setPopState]=(0,react.useState)({hidden:true});const[pkgValue,setPkgValue]=(0,react.useState)({pkgId:'',pkgName:''});(0,react.useEffect)(async()=>{let res=await fetch("https://docs.taosdata.com/assets/globalscripts/packages_v3.php?type="+props.type);let data=await res.json();if(props.sys){data=data.filter(pkg=>pkg.name.indexOf(props.sys)>-1);}setPkgs(data);},[]);function setEmail(pkgId,pkgName){setPopState({hidden:false});setPkgValue({pkgId:pkgId,pkgName:pkgName});// setPkgValue({pkgName:pkgName})
}function test(val){setPopState({hidden:true});}return/*#__PURE__*/react.createElement("div",{id:"server-packageList",className:"package-list"},/*#__PURE__*/react.createElement(popupv3,{hidden:popState.hidden,pkg:pkgValue.pkgId,pkgName:pkgValue.pkgName,pfn:test}),/*#__PURE__*/react.createElement("ul",null,pkgs.map(pkg=>/*#__PURE__*/react.createElement("li",{key:pkg.id},/*#__PURE__*/react.createElement("a",{href:"#!",onClick:()=>{setEmail(pkg.id);}},pkg.name," ","("+pkg.size+")")))));}/* harmony default export */ const components_PkgListV3 = (PkgListV3);

/***/ }),

/***/ 6852:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-r","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`library("DBI")
library("rJava")
library("RJDBC")

args<- commandArgs(trailingOnly = TRUE)
driver_path = args[1] # path to jdbc-driver for example: "/root/taos-jdbcdriver-3.2.4-dist.jar"
driver = JDBC("com.taosdata.jdbc.TSDBDriver", driver_path)
conn = dbConnect(driver, "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata")
dbGetQuery(conn, "SELECT server_version()")
dbSendUpdate(conn, "create database if not exists rtest")
dbSendUpdate(conn, "create table if not exists rtest.test (ts timestamp, current float, voltage int, devname varchar(20))")
dbSendUpdate(conn, "insert into rtest.test values (now, 1.2, 220, 'test')")
dbGetQuery(conn, "select * from rtest.test")
dbDisconnect(conn)
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/R/connect_native.r"},`查看源码`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3928:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "assets": () => (/* binding */ assets),
  "contentTitle": () => (/* binding */ _01_connect_contentTitle),
  "default": () => (/* binding */ _01_connect_MDXContent),
  "frontMatter": () => (/* binding */ _01_connect_frontMatter),
  "metadata": () => (/* binding */ metadata),
  "toc": () => (/* binding */ _01_connect_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_java.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.taosdata.jdbc.TSDBDriver;

public class JNIConnectExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        System.out.println("Connected");
        conn.close();
    }
}

// use
// String jdbcUrl = "jdbc:TAOS://localhost:6030/dbName?user=root&password=taosdata";
// if you want to connect a specified database named "dbName".
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/JNIConnectExample.java"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"REST 连接\"","title":"\"REST","连接\"":true},`    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/RESTConnectExample.java"},`查看源码`)),(0,esm/* mdx */.kt)("p",null,`使用 REST 连接时，如果查询数据量比较大，还可开启批量拉取功能。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"开启批量拉取功能\" {4}","title":"\"开启批量拉取功能\"","{4}":true},`    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        System.out.println("Connected");
        conn.close();
    }
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WSConnectExample.java"},`查看源码`)),(0,esm/* mdx */.kt)("p",null,`更多连接参数配置，参考`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../connector/java"},`Java 连接器`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_go.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_go_frontMatter={};const _connect_go_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_go_toc=[{value:'使用数据库访问统一接口',id:'使用数据库访问统一接口',level:4},{value:'使用高级封装',id:'使用高级封装',level:4}];const _connect_go_layoutProps={toc: _connect_go_toc};const _connect_go_MDXLayout="wrapper";function _connect_go_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_go_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_go_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h4",{"id":"使用数据库访问统一接口"},`使用数据库访问统一接口`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
    var taosDSN = "root:taosdata@tcp(localhost:6030)/"
    taos, err := sql.Open("taosSql", taosDSN)
    if err != nil {
        log.Fatalln("failed to connect TDengine, err:", err)
        return
    }
    fmt.Println("Connected")
    defer taos.Close()
}

// use
// var taosDSN = "root:taosdata@tcp(localhost:6030)/dbName"
// if you want to connect a specified database named "dbName".

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/cgoexample/main.go"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"REST 连接\"","title":"\"REST","连接\"":true},`package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        log.Fatalln("failed to connect TDengine, err:", err)
        return
    }
    fmt.Println("Connected")
    defer taos.Close()
}

// use
// var taosDSN = "root:taosdata@http(localhost:6041)/dbName"
// if you want to connect a specified database named "dbName".

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/restexample/main.go"},`查看源码`)),(0,esm/* mdx */.kt)("h4",{"id":"使用高级封装"},`使用高级封装`),(0,esm/* mdx */.kt)("p",null,`也可以使用 driver-go 的 af 包建立连接。这个模块封装了 TDengine 的高级功能, 如：参数绑定、订阅等。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"使用 af 包建立原生连接\"","title":"\"使用","af":true,"包建立原生连接\"":true},`package main

import (
    "fmt"
    "log"

    "github.com/taosdata/driver-go/v3/af"
)

func main() {
    conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
    defer conn.Close()
    if err != nil {
        log.Fatalln("failed to connect, err:", err)
    } else {
        fmt.Println("connected")
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/afconn/main.go"},`查看源码`)));};_connect_go_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_rust.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_rust_frontMatter={};const _connect_rust_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_rust_toc=[];const _connect_rust_layoutProps={toc: _connect_rust_toc};const _connect_rust_MDXLayout="wrapper";function _connect_rust_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_rust_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_rust_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust","metastring":"title=\"原生连接/REST 连接\"","title":"\"原生连接/REST","连接\"":true},`use taos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[allow(unused_variables)]
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    println!("Connected");
    Ok(())
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/connect.rs"},`查看源码`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`对于 Rust 连接器， 连接方式的不同只体现在使用的特性不同。如果启用了 "rest" 特性，那么只有 RESTful 的实现会被编译进来。`)));};_connect_rust_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_node.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_node_frontMatter={};const _connect_node_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_node_toc=[];const _connect_node_layoutProps={toc: _connect_node_toc};const _connect_node_MDXLayout="wrapper";function _connect_node_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_node_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_node_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`//A cursor also needs to be initialized in order to interact with TDengine from Node.js.
const taos = require("@tdengine/client");
var conn = taos.connect({
  host: "127.0.0.1",
  user: "root",
  password: "taosdata",
  config: "/etc/taos",
  port: 0,
});
var cursor = conn.cursor(); // Initializing a new cursor

//Close a connection
conn.close();
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/connect.js"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=\"REST 连接\"","title":"\"REST","连接\"":true},`const { options, connect } = require("@tdengine/rest");

async function test() {
  options.path = "/rest/sql";
  options.host = "localhost";
  options.port = 6041;
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query("SELECT server_version()");
    console.log("res.getResult()",res.getResult());
  } catch (err) {
    console.log(err);
  }
}
test();

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/restexample/connect.js"},`查看源码`)));};_connect_node_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_python.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_python_frontMatter={};const _connect_python_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_python_toc=[];const _connect_python_layoutProps={toc: _connect_python_toc};const _connect_python_MDXLayout="wrapper";function _connect_python_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_python_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_python_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`import taos


def test_connection():
    # all parameters are optional.
    # if database is specified,
    # then it must exist.
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata",
                        database="log")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    conn.close()


if __name__ == "__main__":
    test_connection()

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_example.py"},`查看源码`)));};_connect_python_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_cs.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_cs_frontMatter={};const _connect_cs_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_cs_toc=[];const _connect_cs_layoutProps={toc: _connect_cs_toc};const _connect_cs_MDXLayout="wrapper";function _connect_cs_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_cs_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_cs_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`using TDengineDriver;

namespace TDengineExample
{

    internal class ConnectExample
    {
        static void Main(String[] args)
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
            TDengine.Close(conn);
            TDengine.Cleanup();
        }
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/connect/Program.cs"},`查看源码`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp","metastring":"title=\"WebSocket 连接\"","title":"\"WebSocket","连接\"":true},`using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSConnExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
  
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
                // close connection.
                LibTaosWS.WSClose(wsConn);
            }

            return 0;
        }
    }
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsConnect/Program.cs"},`查看源码`)));};_connect_cs_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_c.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_c_frontMatter={};const _connect_c_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_c_toc=[];const _connect_c_layoutProps={toc: _connect_c_toc};const _connect_c_MDXLayout="wrapper";function _connect_c_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_c_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_c_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`// compile with
// gcc connect_example.c -o connect_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  // if don't want to connect to a default db, set it to NULL or ""
  const char *db = NULL;
  uint16_t    port = 0;  // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    int   errno = taos_errno(NULL);
    char *msg = taos_errstr(NULL);
    printf("%d, %s\\n", errno, msg);
  } else {
    printf("connected\\n");
    taos_close(taos);
  }
  taos_cleanup();
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/connect_example.c"},`查看源码`)));};_connect_c_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/01-connect/_connect_r.mdx
var _connect_r = __webpack_require__(6852);
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_php.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_php_frontMatter={};const _connect_php_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_php_toc=[];const _connect_php_layoutProps={toc: _connect_php_toc};const _connect_php_MDXLayout="wrapper";function _connect_php_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_php_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_php_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-php","metastring":"title=\"原生连接\"","title":"\"原生连接\""},`<?php

use TDengine\\Connection;
use TDengine\\Exception\\TDengineException;

try {
    // instantiate
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = null;
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // connect
    $connection->connect();
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/php/connect.php"},`查看源码`)));};_connect_php_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/08-connector/_linux_install.mdx
var _linux_install = __webpack_require__(7234);
// EXTERNAL MODULE: ./docs/08-connector/_windows_install.mdx
var _windows_install = __webpack_require__(5273);
// EXTERNAL MODULE: ./docs/08-connector/_macos_install.mdx
var _macos_install = __webpack_require__(7995);
// EXTERNAL MODULE: ./docs/08-connector/_verify_linux.mdx
var _verify_linux = __webpack_require__(8537);
// EXTERNAL MODULE: ./docs/08-connector/_verify_macos.mdx
var _verify_macos = __webpack_require__(891);
// EXTERNAL MODULE: ./docs/08-connector/_verify_windows.mdx
var _verify_windows = __webpack_require__(4346);
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/index.md
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _01_connect_frontMatter={title:'建立连接',description:'使用连接器建立与 TDengine 的连接，以及连接器的安装和连接'};const _01_connect_contentTitle=undefined;const metadata={"unversionedId":"develop/connect/index","id":"develop/connect/index","title":"建立连接","description":"使用连接器建立与 TDengine 的连接，以及连接器的安装和连接","source":"@site/docs/07-develop/01-connect/index.md","sourceDirName":"07-develop/01-connect","slug":"/develop/connect/","permalink":"/docs/develop/connect/","draft":false,"tags":[],"version":"current","frontMatter":{"title":"建立连接","description":"使用连接器建立与 TDengine 的连接，以及连接器的安装和连接"},"sidebar":"defaultSidebar","previous":{"title":"开发指南","permalink":"/docs/develop/"},"next":{"title":"数据建模","permalink":"/docs/develop/model/"}};const assets={};const _01_connect_toc=[{value:'连接器建立连接的方式',id:'连接器建立连接的方式',level:2},{value:'安装客户端驱动 taosc',id:'安装客户端驱动-taosc',level:2},{value:'安装步骤',id:'安装步骤',level:3},{value:'安装验证',id:'安装验证',level:3},{value:'安装连接器',id:'安装连接器',level:2},{value:'建立连接',id:'建立连接',level:2}];const _01_connect_layoutProps={toc: _01_connect_toc};const _01_connect_MDXLayout="wrapper";function _01_connect_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_01_connect_MDXLayout,(0,esm_extends/* default */.Z)({},_01_connect_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`TDengine 提供了丰富的应用程序开发接口，为了便于用户快速开发自己的应用，TDengine 支持了多种编程语言的连接器，其中官方连接器包括支持 C/C++、Java、Python、Go、Node.js、C#、Rust、Lua（社区贡献）和 PHP （社区贡献）的连接器。这些连接器支持使用原生接口（taosc）和 REST 接口（部分语言暂不支持）连接 TDengine 集群。社区开发者也贡献了多个非官方连接器，例如 ADO.NET 连接器、Lua 连接器和 PHP 连接器。`),(0,esm/* mdx */.kt)("h2",{"id":"连接器建立连接的方式"},`连接器建立连接的方式`),(0,esm/* mdx */.kt)("p",null,`连接器建立连接的方式，TDengine 提供两种:`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接，这种连接方式下文中简称“REST 连接”`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接，这种连接方式下文中简称“原生连接”。`)),(0,esm/* mdx */.kt)("p",null,`无论使用何种方式建立连接，连接器都提供了相同或相似的 API 操作数据库，都可以执行 SQL 语句，只是初始化连接的方式稍有不同，用户在使用上不会感到什么差别。`),(0,esm/* mdx */.kt)("p",null,`关键不同点在于：`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`使用 REST 连接，用户无需安装客户端驱动程序 taosc，具有跨平台易用的优势，但性能要下降 30% 左右。`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`使用原生连接可以体验 TDengine 的全部功能，如`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"../../connector/cpp/#%E5%8F%82%E6%95%B0%E7%BB%91%E5%AE%9A-api"},`参数绑定接口`),`、`,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"../../connector/cpp/#%E8%AE%A2%E9%98%85%E5%92%8C%E6%B6%88%E8%B4%B9-api"},`订阅`),`等等。`)),(0,esm/* mdx */.kt)("h2",{"id":"安装客户端驱动-taosc"},`安装客户端驱动 taosc`),(0,esm/* mdx */.kt)("p",null,`如果选择原生连接，而且应用程序不在 TDengine 同一台服务器上运行，你需要先安装客户端驱动，否则可以跳过此一步。为避免客户端驱动和服务端不兼容，请使用一致的版本。`),(0,esm/* mdx */.kt)("h3",{"id":"安装步骤"},`安装步骤`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"linux",groupId:"os",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"linux",label:"Linux",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_linux_install/* default */.ZP,{mdxType:"InstallOnLinux"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_windows_install/* default */.ZP,{mdxType:"InstallOnWindows"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"macos",label:"macOS",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_macos_install/* default */.ZP,{mdxType:"InstallOnMacOS"}))),(0,esm/* mdx */.kt)("h3",{"id":"安装验证"},`安装验证`),(0,esm/* mdx */.kt)("p",null,`以上安装和配置完成后，并确认 TDengine 服务已经正常启动运行，此时可以执行安装包里带有的 TDengine 命令行程序 taos 进行登录。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"linux",groupId:"os",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"linux",label:"Linux",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_linux/* default */.ZP,{mdxType:"VerifyLinux"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_windows/* default */.ZP,{mdxType:"VerifyWindows"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"macos",label:"macOS",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_macos/* default */.ZP,{mdxType:"VerifyMacOS"}))),(0,esm/* mdx */.kt)("h2",{"id":"安装连接器"},`安装连接器`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`如果使用 Maven 管理项目，只需在 pom.xml 中加入以下依赖。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-xml"},`<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.2.4</version>
</dependency>
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`使用 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`pip`),` 从 PyPI 安装:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`pip install taospy
`)),(0,esm/* mdx */.kt)("p",null,`从 Git URL 安装：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`pip install git+https://github.com/taosdata/taos-connector-python.git
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`编辑 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`go.mod`),` 添加 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`driver-go`),` 依赖即可。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go-mod","metastring":"title=go.mod","title":"go.mod"},`module goexample

go 1.17

require github.com/taosdata/driver-go/v3 latest
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`driver-go 使用 cgo 封装了 taosc 的 API。cgo 需要使用 GCC 编译 C 的源码。因此需要确保你的系统上有 GCC。`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`编辑 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Cargo.toml`),` 添加 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`libtaos`),` 依赖即可。`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml","metastring":"title=Cargo.toml","title":"Cargo.toml"},`[dependencies]
libtaos = { version = "0.4.2"}
`)),(0,esm/* mdx */.kt)("admonition",{"type":"info"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`Rust 连接器通过不同的特性区分不同的连接方式。如果要建立 REST 连接，需要开启 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`rest`),` 特性：`),(0,esm/* mdx */.kt)("pre",{parentName:"admonition"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`libtaos = { version = "*", features = ["rest"] }
`)))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"node",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Node.js 连接器通过不同的包提供不同的连接方式。`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`安装 Node.js 原生连接器`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`npm install @tdengine/client
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`推荐 Node 版本大于等于 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`node-v12.8.0`),` 小于 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`node-v13.0.0`))),(0,esm/* mdx */.kt)("ol",{"start":2},(0,esm/* mdx */.kt)("li",{parentName:"ol"},`安装 Node.js REST 连接器`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`npm install @tdengine/rest
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`编辑项目配置文件中添加 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://www.nuget.org/packages/TDengine.Connector/"},`TDengine.Connector`),` 的引用即可：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-xml","metastring":"title=csharp.csproj {12}","title":"csharp.csproj","{12}":true},`<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <StartupObject>TDengineExample.AsyncQueryExample</StartupObject>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.0" />
  </ItemGroup>

</Project>
`)),(0,esm/* mdx */.kt)("p",null,`也可通过 dotnet 命令添加：`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`dotnet add package TDengine.Connector
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`以下示例代码，均基于 dotnet6.0，如果使用其它版本，可能需要做适当调整。`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"R",value:"r",mdxType:"TabItem"},(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`下载 `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.0.0/"},`taos-jdbcdriver-version-dist.jar`),`。`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`安装 R 的依赖包`,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`RJDBC`),`：`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-R"},`install.packages("RJDBC")
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`如果已经安装了 TDengine 服务端软件或 TDengine 客户端驱动 taosc， 那么已经安装了 C 连接器，无需额外操作。`),(0,esm/* mdx */.kt)("br",null)),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`下载代码并解压：`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`curl -L -o php-tdengine.tar.gz https://github.com/Yurunsoft/php-tdengine/archive/refs/tags/v1.0.2.tar.gz \\
&& mkdir php-tdengine \\
&& tar -xzf php-tdengine.tar.gz -C php-tdengine --strip-components=1
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},`版本 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`v1.0.2`),` 只是示例，可替换为任意更新的版本，可在 `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/Yurunsoft/php-tdengine/releases"},`TDengine PHP Connector 发布历史`),` 中查看可用版本。`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`非 Swoole 环境：`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure && make -j && make install
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`手动指定 TDengine 目录：`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure --with-tdengine-dir=/usr/local/Cellar/tdengine/3.0.0.0 && make -j && make install
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`--with-tdengine-dir=`),` 后跟上 TDengine 目录。
适用于默认找不到的情况，或者 macOS 系统用户。`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Swoole 环境：`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure --enable-swoole && make -j && make install
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`启用扩展：`)),(0,esm/* mdx */.kt)("p",null,`方法一：在 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`php.ini`),` 中加入 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`extension=tdengine`)),(0,esm/* mdx */.kt)("p",null,`方法二：运行带参数 `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`php -d extension=tdengine test.php`)))),(0,esm/* mdx */.kt)("h2",{"id":"建立连接"},`建立连接`),(0,esm/* mdx */.kt)("p",null,`在执行这一步之前，请确保有一个正在运行的，且可以访问到的 TDengine，而且服务端的 FQDN 配置正确。以下示例代码，都假设 TDengine 安装在本机，且 FQDN（默认 localhost） 和 serverPort（默认 6030） 都使用默认配置。`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{groupId:"lang",defaultValue:"java",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"ConnJava"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_python_MDXContent,{mdxType:"ConnPythonNative"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_go_MDXContent,{mdxType:"ConnGo"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_rust_MDXContent,{mdxType:"ConnRust"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"node",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_node_MDXContent,{mdxType:"ConnNode"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_cs_MDXContent,{mdxType:"ConnCSNative"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"R",value:"r",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_r/* default */.ZP,{mdxType:"ConnR"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_c_MDXContent,{mdxType:"ConnC"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_php_MDXContent,{mdxType:"ConnPHP"}))),(0,esm/* mdx */.kt)("admonition",{"type":"tip"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`如果建立连接失败，大部分情况下是 FQDN 或防火墙的配置不正确，详细的排查方法请看`,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://docs.taosdata.com/train-faq/faq"},`《常见问题及反馈》`),`中的“遇到错误 Unable to establish connection, 我怎么办？”`)));};_01_connect_MDXContent.isMDXComponent=true;

/***/ }),

/***/ 7234:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`下载客户端安装包`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:1,sys:"Linux",mdxType:"PkgListV3"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`解压缩软件包`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`将软件包放置在当前用户可读写的任意目录下，然后执行下面的命令：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`tar -xzvf TDengine-client-VERSION.tar.gz`),`
其中 VERSION 需要替换为实际版本的字符串。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`执行安装脚本`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`解压软件包之后，会在解压目录下看到以下文件(目录)：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` install_client.sh`),`：安装脚本，用于应用驱动程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` package.tar.gz`),`：应用驱动安装包`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` driver`),`：TDengine 应用驱动 driver`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`examples`),`: 各种编程语言的示例程序(c/C#/go/JDBC/MATLAB/python/R)
运行 install_client.sh 进行安装。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`配置 taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`编辑 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` 文件（默认路径/etc/taos/taos.cfg），将 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`firstEP`),` 修改为 TDengine 服务器的 End Point，例如：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如本机没有部署 TDengine 服务，仅安装了应用驱动，则 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` 中仅需配置 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`firstEP`),`，无需在本机配置 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FQDN`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`为防止与服务器端连接时出现“Unable to resolve FQDN”错误，建议确认本机的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/etc/hosts`),` 文件已经配置了服务器正确的 FQDN 值，或配置好了 DNS 服务。`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 7995:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`下载客户端安装包`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:8,sys:"macOS",mdxType:"PkgListV3"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`执行安装程序，按提示选择默认值，完成安装。如果安装被阻止，可以右键或者按 Ctrl 点击安装包，选择 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`打开`),`。`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`配置 taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`编辑 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` 文件（默认路径/etc/taos/taos.cfg），将 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`firstEP`),` 修改为 TDengine 服务器的 End Point，例如：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如本机没有部署 TDengine 服务，仅安装了应用驱动，则 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` 中仅需配置 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`firstEP`),`，无需在本机配置 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FQDN`),`。`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`为防止与服务器端连接时出现“Unable to resolve FQDN”错误，建议确认本机的 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/etc/hosts`),` 文件已经配置了服务器正确的 FQDN 值，或配置好了 DNS 服务。`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 8537:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 Linux shell 下直接执行 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` 连接到 TDengine 服务，进入到 TDengine CLI 界面，示例如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`$ taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 db                             |
Query OK, 3 rows in database (0.019154s)

taos>
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 891:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 macOS shell 下直接执行 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` 连接到 TDengine 服务，进入到 TDengine CLI 界面，示例如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`$ taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 db                             |
Query OK, 3 rows in database (0.019154s)

taos>
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 4346:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`在 cmd 下进入到 C:\\TDengine 目录下直接执行 `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.exe`),`，连接到 TDengine 服务，进入到 TDengine CLI 界面，示例如下：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`taos> show databases;
              name              |       create_time       | vgroups |        ntables        | replica | strict |  duration  |              keep              |   buffer    |  pagesize   |    pages    |   minrows   |   maxrows   | comp | precision |   status   |           retention            | single_stable | cachemodel  |  cachesize  | wal_level | wal_fsync_period | wal_retention_period |  wal_retention_size   |
===============================================================================================================================================================================================================================================================================================================================================================================================================================
 information_schema             | NULL                    |    NULL |                    14 |    NULL | NULL   | NULL       | NULL                           |        NULL |        NULL |        NULL |        NULL |        NULL | NULL | NULL      | ready      | NULL                           | NULL          | NULL        |        NULL |      NULL |             NULL |                 NULL |                  NULL |
 performance_schema             | NULL                    |    NULL |                     3 |    NULL | NULL   | NULL       | NULL                           |        NULL |        NULL |        NULL |        NULL |        NULL | NULL | NULL      | ready      | NULL                           | NULL          | NULL        |        NULL |      NULL |             NULL |                 NULL |                  NULL |
 test                           | 2022-08-04 16:46:40.506 |       2 |                     0 |       1 | off    | 14400m     | 5256000m,5256000m,5256000m     |          96 |           4 |         256 |
100 |        4096 |    2 | ms        | ready      | NULL                           |         false | none        |           1 |         1 |             3000 |                    0 |                     0 |               0 |                     0 |
Query OK, 3 rows in database (0.123000s)

taos>
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 5273:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "ZP": () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(3117);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,C_Users_dingb_enterprise_docs_enterprise_docs_zh_node_modules_docusaurus_core_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`下载客户端安装包`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:4,sys:"Windows",mdxType:"PkgListV3"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`执行安装程序，按提示选择默认值，完成安装`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`安装路径`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`默认安装路径为：C:\\TDengine，其中包括以下文件(目录)：`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taos.exe`),`：TDengine CLI 命令行程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taosadapter.exe`),`：提供 RESTful 服务和接受其他多种软件写入请求的服务端可执行文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taosBenchmark.exe`),`：TDengine 测试程序`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`cfg`),` : 配置文件目录`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`driver`),`: 应用驱动动态链接库`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`examples`),`: 示例程序 bash/C/C#/go/JDBC/Python/Node.js`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`include`),`: 头文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`log`),` : 日志文件`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`unins000.exe`),`: 卸载程序`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`配置 taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`编辑 taos.cfg 文件（默认路径 C:\\TDengine\\cfg\\taos.cfg），将 firstEP 修改为 TDengine 服务器的 End Point，例如：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`),`。`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`如利用 FQDN 连接服务器，必须确认本机网络环境 DNS 已配置好，或在 hosts 文件中添加 FQDN 寻址记录, 如编辑 C:\\Windows\\system32\\drivers\\etc\\hosts，添加类似如下的记录：`,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`192.168.1.99 h1.taos.com`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`卸载：运行 unins000.exe 可卸载 TDengine 应用驱动。`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 8354:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "Z": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,UklGRooCAABXRUJQVlA4WAoAAAAQAAAAIQAAIQAAQUxQSDkCAAAB58SgkSQpdA+Pf8ezC3iIiDxYc1JjWR8sImuxVCLfEImMhBWZk8lZlxuRDa/UxcuTkUpjqYyhKWupjPwh0bZtY2/2F9u2Uzf86za2bdtGbTv406/7Yc/9cPM/QkT/E7qUpqWXP3n+dXewPAS+BNx7Tt3pVCouzX1Gy3y6MNg3vfOX1mQktEabSbVWEQpHQMHwKeXbDWj8J6i206GNGPxPsxYuY5zibTRwWeEnSi2cTZSTa/Ah9gXlJuysC/67Dp9i3vNHFJTfIaUZPmaecxaqjHLgB5W+0+sPd9nGHVjtlHTILiUX1jBlxh9Oj8lPsII+cw7xlF3YxV7KjD9sj0npgd3K87B6yiM4SryUGX8oj0kZNGDHUDyLlFg4S7yUGX94TMqgAeczjrzhJ7hLvJSZByZl0IBrlAen3IGmxEt70IC7id8oy9Bd8VJ1G9A84TllFboqWjP+0FTz4jf3oamiuqDM+MPdxu9P+QPuKqqu4hPKXCBcUzyeoiTCWUXVBSk+oawEwvmWUw8pTXBUUXXAKj6hrATCTqI8jjD52oDloWqBo/iEMga7h/+jsUIphTVAaYGr6BdfwQr/y3VIPuVdEFTswF1oMobzYY1QrkKtUIbg423KJqy435Q6+JT2i2fJsMuoGuBD9jdKDZxNVOPBuMSoOqX0w91I9fF+AHQ521SDBjS3flB96MwLgJ1Ye0D1rx76qHnaF2/2tp7/oX2UgUvTZ86pO6z0gy+hFQO7X068P58u1CdABwBWUDggKgAAADADAJ0BKiIAIgA+jTaWR6UioiExyACgEYlpAACAl+ecMsAA/vikAAAAAA==");

/***/ })

}]);