"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[9941],{

/***/ 2236:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {


// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  Z: () => (/* binding */ components_PkgListV3)
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
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-r","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`library("DBI")
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
`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/R/connect_native.r"},`view source code`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3928:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  assets: () => (/* binding */ assets),
  contentTitle: () => (/* binding */ _01_connect_contentTitle),
  "default": () => (/* binding */ _01_connect_MDXContent),
  frontMatter: () => (/* binding */ _01_connect_frontMatter),
  metadata: () => (/* binding */ metadata),
  toc: () => (/* binding */ _01_connect_toc)
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
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_java.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(MDXLayout,(0,esm_extends/* default */.Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`package com.taos.example;

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
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/JNIConnectExample.java"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"REST Connection\"","title":"\"REST","Connection\"":true},`    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/RESTConnectExample.java"},`view source code`)),(0,esm/* mdx */.kt)("p",null,`When using REST connection, the feature of bulk pulling can be enabled if the size of resulting data set is huge.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-java","metastring":"title=\"Enable Bulk Pulling\" {4}","title":"\"Enable","Bulk":true,"Pulling\"":true,"{4}":true},`    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        System.out.println("Connected");
        conn.close();
    }
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WSConnectExample.java"},`view source code`)),(0,esm/* mdx */.kt)("p",null,`More configuration about connection, please refer to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/reference/connector/java"},`Java Connector`)));};MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_go.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_go_frontMatter={};const _connect_go_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_go_toc=[{value:'Unified Database Access Interface',id:'unified-database-access-interface',level:4},{value:'Advanced Features',id:'advanced-features',level:4}];const _connect_go_layoutProps={toc: _connect_go_toc};const _connect_go_MDXLayout="wrapper";function _connect_go_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_go_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_go_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("h4",{"id":"unified-database-access-interface"},`Unified Database Access Interface`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`package main

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/cgoexample/main.go"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"REST Connection\"","title":"\"REST","Connection\"":true},`package main

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/restexample/main.go"},`view source code`)),(0,esm/* mdx */.kt)("h4",{"id":"advanced-features"},`Advanced Features`),(0,esm/* mdx */.kt)("p",null,`The af package of driver-go can also be used to establish connection, with this way some advanced features of TDengine, like parameter binding and subscription, can be used.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go","metastring":"title=\"Establish native connection using af package\"","title":"\"Establish","native":true,"connection":true,"using":true,"af":true,"package\"":true},`package main

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/go/connect/afconn/main.go"},`view source code`)));};_connect_go_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_rust.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_rust_frontMatter={};const _connect_rust_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_rust_toc=[];const _connect_rust_layoutProps={toc: _connect_rust_toc};const _connect_rust_MDXLayout="wrapper";function _connect_rust_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_rust_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_rust_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-rust","metastring":"title=\"Native Connection/REST Connection\"","title":"\"Native","Connection/REST":true,"Connection\"":true},`use taos::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[allow(unused_variables)]
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    println!("Connected");
    Ok(())
}

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/rust/nativeexample/examples/connect.rs"},`view source code`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`For Rust connector, the connection depends on the feature being used. If "rest" feature is enabled, then only the implementation for "rest" is compiled and packaged.`)));};_connect_rust_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_node.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_node_frontMatter={};const _connect_node_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_node_toc=[];const _connect_node_layoutProps={toc: _connect_node_toc};const _connect_node_MDXLayout="wrapper";function _connect_node_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_node_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_node_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`//A cursor also needs to be initialized in order to interact with TDengine from Node.js.
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
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/nativeexample/connect.js"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-js","metastring":"title=\"REST Connection\"","title":"\"REST","Connection\"":true},`const { options, connect } = require("@tdengine/rest");

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/node/restexample/connect.js"},`view source code`)));};_connect_node_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_python.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_python_frontMatter={};const _connect_python_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_python_toc=[];const _connect_python_layoutProps={toc: _connect_python_toc};const _connect_python_MDXLayout="wrapper";function _connect_python_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_python_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_python_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-python","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`import taos


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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/python/connect_example.py"},`view source code`)));};_connect_python_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_cs.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_cs_frontMatter={};const _connect_cs_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_cs_toc=[];const _connect_cs_layoutProps={toc: _connect_cs_toc};const _connect_cs_MDXLayout="wrapper";function _connect_cs_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_cs_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_cs_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`using TDengineDriver;

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/connect/Program.cs"},`view source code`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-csharp","metastring":"title=\"WebSocket Connection\"","title":"\"WebSocket","Connection\"":true},`using System;
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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/csharp/wsConnect/Program.cs"},`view source code`)));};_connect_cs_MDXContent.isMDXComponent=true;
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_c.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_c_frontMatter={};const _connect_c_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_c_toc=[];const _connect_c_layoutProps={toc: _connect_c_toc};const _connect_c_MDXLayout="wrapper";function _connect_c_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_c_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_c_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-c","metastring":"title=\"Native Connection\"","title":"\"Native","Connection\"":true},`// compile with
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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/c/connect_example.c"},`view source code`)));};_connect_c_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/07-develop/01-connect/_connect_r.mdx
var _connect_r = __webpack_require__(6852);
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/_connect_php.mdx
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _connect_php_frontMatter={};const _connect_php_contentTitle=(/* unused pure expression or super */ null && (undefined));const _connect_php_toc=[];const _connect_php_layoutProps={toc: _connect_php_toc};const _connect_php_MDXLayout="wrapper";function _connect_php_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_connect_php_MDXLayout,(0,esm_extends/* default */.Z)({},_connect_php_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-php","metastring":"title=\"\"native\"","title":"\"\"native\""},`<?php

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

`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://github.com/taosdata/TDengine/blob/main/docs/examples/php/connect.php"},`view source code`)));};_connect_php_MDXContent.isMDXComponent=true;
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_linux_install.mdx
var _linux_install = __webpack_require__(159);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_windows_install.mdx
var _windows_install = __webpack_require__(3656);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_macos_install.mdx
var _macos_install = __webpack_require__(6878);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_verify_linux.mdx
var _verify_linux = __webpack_require__(3018);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_verify_windows.mdx
var _verify_windows = __webpack_require__(9321);
// EXTERNAL MODULE: ./docs/14-reference/03-connector/_verify_macos.mdx
var _verify_macos = __webpack_require__(6948);
;// CONCATENATED MODULE: ./docs/07-develop/01-connect/index.md
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const _01_connect_frontMatter={title:'Connect to TDengine',sidebar_label:'Connect',description:'This document describes how to establish connections to TDengine and how to install and use TDengine connectors.'};const _01_connect_contentTitle=undefined;const metadata={"unversionedId":"develop/connect/index","id":"develop/connect/index","title":"Connect to TDengine","description":"This document describes how to establish connections to TDengine and how to install and use TDengine connectors.","source":"@site/docs/07-develop/01-connect/index.md","sourceDirName":"07-develop/01-connect","slug":"/develop/connect/","permalink":"/docs-en/develop/connect/","draft":false,"tags":[],"version":"current","frontMatter":{"title":"Connect to TDengine","sidebar_label":"Connect","description":"This document describes how to establish connections to TDengine and how to install and use TDengine connectors."},"sidebar":"defaultSidebar","previous":{"title":"Developer Guide","permalink":"/docs-en/develop/"},"next":{"title":"Data Model","permalink":"/docs-en/develop/model/"}};const assets={};const _01_connect_toc=[{value:'Establish Connection',id:'establish-connection',level:2},{value:'Install Client Driver taosc',id:'install-client-driver-taosc',level:2},{value:'Install',id:'install',level:3},{value:'Verify',id:'verify',level:3},{value:'Install Connectors',id:'install-connectors',level:2},{value:'Establish a connection',id:'establish-a-connection',level:2}];const _01_connect_layoutProps={toc: _01_connect_toc};const _01_connect_MDXLayout="wrapper";function _01_connect_MDXContent(_ref){let{components,...props}=_ref;return (0,esm/* mdx */.kt)(_01_connect_MDXLayout,(0,esm_extends/* default */.Z)({},_01_connect_layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,esm/* mdx */.kt)("p",null,`Any application running on any platform can access TDengine through the REST API provided by TDengine. For information, see `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"/reference/rest-api/"},`REST API`),`. Applications can also use the connectors for various programming languages, including C/C++, Java, Python, Go, Node.js, C#, and Rust, to access TDengine. These connectors support connecting to TDengine clusters using both native interfaces (taosc). Some connectors also support connecting over a REST interface. Community developers have also contributed several unofficial connectors, such as the ADO.NET connector, the Lua connector, and the PHP connector.`),(0,esm/* mdx */.kt)("h2",{"id":"establish-connection"},`Establish Connection`),(0,esm/* mdx */.kt)("p",null,`There are two ways for a connector to establish connections to TDengine:`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`REST connection through the REST API provided by the taosAdapter component.`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Native connection through the TDengine client driver (taosc).`)),(0,esm/* mdx */.kt)("p",null,`For REST and native connections, connectors provide similar APIs for performing operations and running SQL statements on your databases. The main difference is the method of establishing the connection, which is not visible to users.`),(0,esm/* mdx */.kt)("p",null,`Key differences:`),(0,esm/* mdx */.kt)("ol",{"start":3},(0,esm/* mdx */.kt)("li",{parentName:"ol"},`The REST connection is more accessible with cross-platform support, however it results in a 30% performance downgrade.`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`The TDengine client driver (taosc) has the highest performance with all the features of TDengine like `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/connector/cpp#parameter-binding-api"},`Parameter Binding`),`, `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"/reference/connector/cpp#subscription-and-consumption-api"},`Subscription`),`, etc.`)),(0,esm/* mdx */.kt)("h2",{"id":"install-client-driver-taosc"},`Install Client Driver taosc`),(0,esm/* mdx */.kt)("p",null,`If you are choosing to use the native connection and the the application is not on the same host as TDengine server, the TDengine client driver taosc needs to be installed on the application host. If choosing to use the REST connection or the application is on the same host as TDengine server, this step can be skipped. It's better to use same version of taosc as the TDengine server.`),(0,esm/* mdx */.kt)("h3",{"id":"install"},`Install`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"linux",groupId:"os",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"linux",label:"Linux",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_linux_install/* default */.ZP,{mdxType:"InstallOnLinux"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_windows_install/* default */.ZP,{mdxType:"InstallOnWindows"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"macos",label:"MacOS",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_macos_install/* default */.ZP,{mdxType:"InstallOnMacOS"}))),(0,esm/* mdx */.kt)("h3",{"id":"verify"},`Verify`),(0,esm/* mdx */.kt)("p",null,`After the above installation and configuration are done and making sure TDengine service is already started and in service, the TDengine command-line interface `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`taos`),` can be launched to access TDengine.`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{defaultValue:"linux",groupId:"os",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"linux",label:"Linux",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_linux/* default */.ZP,{mdxType:"VerifyLinux"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_windows/* default */.ZP,{mdxType:"VerifyWindows"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{value:"macos",label:"MacOS",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_verify_macos/* default */.ZP,{mdxType:"VerifyMacOS"}))),(0,esm/* mdx */.kt)("h2",{"id":"install-connectors"},`Install Connectors`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{groupId:"lang",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`If `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`maven`),` is used to manage the projects, what needs to be done is only adding below dependency in `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`pom.xml`),`.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-xml"},`<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.2.4</version>
</dependency>
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Install from PyPI using `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`pip`),`:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`pip install taospy
`)),(0,esm/* mdx */.kt)("p",null,`Install from Git URL:`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`pip install git+https://github.com/taosdata/taos-connector-python.git
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Just need to add `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`driver-go`),` dependency in `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`go.mod`),` .`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-go-mod","metastring":"title=go.mod","title":"go.mod"},`module goexample

go 1.17

require github.com/taosdata/driver-go/v3 latest
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`driver-go`),` uses `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`cgo`),` to wrap the APIs provided by taosc, while `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`cgo`),` needs `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`gcc`),` to compile source code in C language, so please make sure you have proper `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`gcc`),` on your system.`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Just need to add `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`libtaos`),` dependency in `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`Cargo.toml`),`.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml","metastring":"title=Cargo.toml","title":"Cargo.toml"},`[dependencies]
libtaos = { version = "0.4.2"}
`)),(0,esm/* mdx */.kt)("admonition",{"type":"info"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`Rust connector uses different features to distinguish the way to establish connection. To establish REST connection, please enable `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`rest`),` feature.`),(0,esm/* mdx */.kt)("pre",{parentName:"admonition"},(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-toml"},`libtaos = { version = "*", features = ["rest"] }
`)))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"node",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Node.js connector provides different ways of establishing connections by providing different packages.`),(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Install Node.js Native Connector`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`npm install @tdengine/client
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`It's recommend to use Node whose version is between `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`node-v12.8.0`),` and `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`node-v13.0.0`),`.`)),(0,esm/* mdx */.kt)("ol",{"start":2},(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Install Node.js REST Connector`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`npm install @tdengine/rest
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`Just need to add the reference to `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"https://www.nuget.org/packages/TDengine.Connector/"},`TDengine.Connector`),` in the project configuration file.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-xml","metastring":"title=csharp.csproj {12}","title":"csharp.csproj","{12}":true},`<Project Sdk="Microsoft.NET.Sdk">

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
`)),(0,esm/* mdx */.kt)("p",null,`Or add by `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`dotnet`),` command.`),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre"},`dotnet add package TDengine.Connector
`)),(0,esm/* mdx */.kt)("admonition",{"type":"note"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`The sample code below are based on dotnet6.0, they may need to be adjusted if your dotnet version is not exactly same.`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"R",value:"r",mdxType:"TabItem"},(0,esm/* mdx */.kt)("ol",null,(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Download `,(0,esm/* mdx */.kt)("a",{parentName:"li","href":"https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.0.0/"},`taos-jdbcdriver-version-dist.jar`),`.`),(0,esm/* mdx */.kt)("li",{parentName:"ol"},`Install the dependency package `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"li"},`RJDBC`),`:`)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-R"},`install.packages("RJDBC")
`))),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,`If the client driver (taosc) is already installed, then the C connector is already available.`),(0,esm/* mdx */.kt)("br",null)),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Download Source Code Package and Unzip: `)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`curl -L -o php-tdengine.tar.gz https://github.com/Yurunsoft/php-tdengine/archive/refs/tags/v1.0.2.tar.gz \\
&& mkdir php-tdengine \\
&& tar -xzf php-tdengine.tar.gz -C php-tdengine --strip-components=1
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},`Version number `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`v1.0.2`),` is only for example, it can be replaced to any newer version.`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Non-Swoole Environment: `)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure && make -j && make install
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Specify TDengine Location: `)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure --with-tdengine-dir=/usr/local/Cellar/tdengine/3.0.0.0 && make -j && make install
`)),(0,esm/* mdx */.kt)("blockquote",null,(0,esm/* mdx */.kt)("p",{parentName:"blockquote"},(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`--with-tdengine-dir=`),` is followed by the TDengine installation location.
This way is useful in case TDengine location can't be found automatically or macOS.`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Swoole Environment: `)),(0,esm/* mdx */.kt)("pre",null,(0,esm/* mdx */.kt)("code",{parentName:"pre","className":"language-shell"},`phpize && ./configure --enable-swoole && make -j && make install
`)),(0,esm/* mdx */.kt)("p",null,(0,esm/* mdx */.kt)("strong",{parentName:"p"},`Enable The Extension:`)),(0,esm/* mdx */.kt)("p",null,`Option One: Add `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`extension=tdengine`),` in `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`php.ini`)),(0,esm/* mdx */.kt)("p",null,`Option Two: Specify the extension on CLI `,(0,esm/* mdx */.kt)("inlineCode",{parentName:"p"},`php -d extension=tdengine test.php`)))),(0,esm/* mdx */.kt)("h2",{"id":"establish-a-connection"},`Establish a connection`),(0,esm/* mdx */.kt)("p",null,`Prior to establishing connection, please make sure TDengine is already running and accessible. The following sample code assumes TDengine is running on the same host as the client program, with FQDN configured to "localhost" and serverPort configured to "6030".`),(0,esm/* mdx */.kt)(Tabs/* default */.Z,{groupId:"lang",defaultValue:"java",mdxType:"Tabs"},(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Java",value:"java",mdxType:"TabItem"},(0,esm/* mdx */.kt)(MDXContent,{mdxType:"ConnJava"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Python",value:"python",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_python_MDXContent,{mdxType:"ConnPythonNative"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Go",value:"go",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_go_MDXContent,{mdxType:"ConnGo"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Rust",value:"rust",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_rust_MDXContent,{mdxType:"ConnRust"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"Node.js",value:"node",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_node_MDXContent,{mdxType:"ConnNode"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C#",value:"csharp",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_cs_MDXContent,{mdxType:"ConnCSNative"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"R",value:"r",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_r/* default */.ZP,{mdxType:"ConnR"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"C",value:"c",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_c_MDXContent,{mdxType:"ConnC"})),(0,esm/* mdx */.kt)(TabItem/* default */.Z,{label:"PHP",value:"php",mdxType:"TabItem"},(0,esm/* mdx */.kt)(_connect_php_MDXContent,{mdxType:"ConnPHP"}))),(0,esm/* mdx */.kt)("admonition",{"type":"tip"},(0,esm/* mdx */.kt)("p",{parentName:"admonition"},`If the connection fails, in most cases it's caused by improper configuration for FQDN or firewall. Please refer to the section "Unable to establish connection" in `,(0,esm/* mdx */.kt)("a",{parentName:"p","href":"../../train-faq/faq"},`FAQ`),`.`)));};_01_connect_MDXContent.isMDXComponent=true;

/***/ }),

/***/ 159:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Download the client installation package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:1,sys:"Linux",mdxType:"PkgListV3"}))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{"start":2},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Unzip`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Download the package to any directory the current user has read/write permission. Then execute `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`tar -xzvf TDengine-client-VERSION.tar.gz`),` command.
The VERSION should be the version of the package you just downloaded.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Execute the install script`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Once the package is unzipped, you will see the following files in the directory:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` install_client.sh`),`: install script`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` package.tar.gz`),`: client driver package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},` driver`),`: TDengine client driver`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`examples`),`: some example programs of different programming languages (C/C#/go/JDBC/MATLAB/python/R)
You can run `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`install_client.sh`),`  to install it.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`configure taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Edit `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` file (full path is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/etc/taos/taos.cfg`),` by default), modify `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`firstEP`),` with actual TDengine server's End Point, for example `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If the computer does not run the TDengine service but installs the TDengine client driver, then you need to config `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`firstEP`),` in  `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` only, and there is no need to configure `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FQDN`),`;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If you encounter the "Unable to resolve FQDN" error, please make sure the FQDN in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/etc/hosts`),` file of the current computer is correctly configured, or the DNS service is correctly configured.`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 6878:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Download the client installation package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:8,sys:"macOS",mdxType:"PkgListV3"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Execute the installer, select the default value as prompted, and complete the installation. If the installation is blocked, you can right-click or ctrl-click on the installation package and select `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`Open`),`.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`configure taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Edit `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.cfg`),` file (full path is `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`/etc/taos/taos.cfg`),` by default), modify `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`firstEP`),` with actual TDengine server's End Point, for example `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`)))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If the computer does not run the TDengine service but installs the TDengine client driver, then you need to config `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`firstEP`),` in  `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`taos.cfg`),` only, and there is no need to configure `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`FQDN`),`;`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If you encounter the "Unable to resolve FQDN" error, please make sure the FQDN in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`/etc/hosts`),` file of the current computer is correctly configured, or the DNS service is correctly configured.`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3018:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Execute TDengine CLI program `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` directly from the Linux shell to connect to the TDengine service and enter the TDengine CLI interface, as shown in the following example.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`$ taos

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

/***/ 6948:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Execute TDengine CLI program `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos`),` directly from the macOS shell to connect to the TDengine service and enter the TDengine CLI interface, as shown in the following example.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`$ taos

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

/***/ 9321:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`Go to the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`C:\\TDengine`),` directory from `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`cmd`),` and execute TDengine CLI program `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`taos.exe`),` directly to connect to the TDengine service and enter the TDengine CLI interface, for example, as follows:`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("pre",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("code",{parentName:"pre","className":"language-text"},`taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 test                           |
Query OK, 3 rows in database (0.123000s)

taos>
`)));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 3656:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   ZP: () => (/* binding */ MDXContent)
/* harmony export */ });
/* unused harmony exports frontMatter, contentTitle, toc */
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* harmony import */ var _components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2236);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={};const contentTitle=(/* unused pure expression or super */ null && (undefined));const toc=[];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_3__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Download the client installation package`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(_components_PkgListV3__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z,{type:4,sys:"Windows",mdxType:"PkgListV3"})),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Execute the installer, select the default value as prompted, and complete the installation`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Installation path`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`The default installation path is C:\\TDengine, including the following files (directories).`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",{parentName:"li"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taos.exe`),`: TDengine CLI command-line program`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taosadapter.exe`),`: server-side executable that provides RESTful services and accepts writing requests from a variety of other software`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`taosBenchmark.exe`),`: TDengine testing tool`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`cfg`),`: configuration file directory`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`driver`),`: client driver dynamic link library`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`examples`),`: sample programs bash/C/C#/go/JDBC/Python/Node.js`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`include`),`: header files`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`log`),`: log file`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("em",{parentName:"li"},`unins000.exe`),`: uninstaller`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`configure taos.cfg`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Edit the taos.cfg file (default path C:\\TDengine\\cfg\\taos.cfg) and change the firstEP to the End Point of the TDengine server, for example: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`h1.tdengine.com:6030`),`.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"tip"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",{parentName:"admonition"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`If you use FQDN to connect to the server, you must ensure the local network environment DNS is configured, or add FQDN addressing records in the `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`hosts`),` file, e.g., edit C:\\Windows\\system32\\drivers\\etc\\hosts and add a record like the following: `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`192.168.1.99 h1.taosd.com`),`..`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},`Uninstall: Run unins000.exe to uninstall the TDengine client driver.`))));};MDXContent.isMDXComponent=true;

/***/ }),

/***/ 8354:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ("data:image/webp;base64,UklGRooCAABXRUJQVlA4WAoAAAAQAAAAIQAAIQAAQUxQSDkCAAAB58SgkSQpdA+Pf8ezC3iIiDxYc1JjWR8sImuxVCLfEImMhBWZk8lZlxuRDa/UxcuTkUpjqYyhKWupjPwh0bZtY2/2F9u2Uzf86za2bdtGbTv406/7Yc/9cPM/QkT/E7qUpqWXP3n+dXewPAS+BNx7Tt3pVCouzX1Gy3y6MNg3vfOX1mQktEabSbVWEQpHQMHwKeXbDWj8J6i206GNGPxPsxYuY5zibTRwWeEnSi2cTZSTa/Ah9gXlJuysC/67Dp9i3vNHFJTfIaUZPmaecxaqjHLgB5W+0+sPd9nGHVjtlHTILiUX1jBlxh9Oj8lPsII+cw7xlF3YxV7KjD9sj0npgd3K87B6yiM4SryUGX8oj0kZNGDHUDyLlFg4S7yUGX94TMqgAeczjrzhJ7hLvJSZByZl0IBrlAen3IGmxEt70IC7id8oy9Bd8VJ1G9A84TllFboqWjP+0FTz4jf3oamiuqDM+MPdxu9P+QPuKqqu4hPKXCBcUzyeoiTCWUXVBSk+oawEwvmWUw8pTXBUUXXAKj6hrATCTqI8jjD52oDloWqBo/iEMga7h/+jsUIphTVAaYGr6BdfwQr/y3VIPuVdEFTswF1oMobzYY1QrkKtUIbg423KJqy435Q6+JT2i2fJsMuoGuBD9jdKDZxNVOPBuMSoOqX0w91I9fF+AHQ521SDBjS3flB96MwLgJ1Ye0D1rx76qHnaF2/2tp7/oX2UgUvTZ86pO6z0gy+hFQO7X068P58u1CdABwBWUDggKgAAADADAJ0BKiIAIgA+jTaWR6UioiExyACgEYlpAACAl+ecMsAA/vikAAAAAA==");

/***/ })

}]);