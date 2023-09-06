"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[2422],{

/***/ 803:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "default": () => (/* binding */ search)
});

// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(7294);
// EXTERNAL MODULE: ./node_modules/@docusaurus/theme-classic/lib/theme/Layout/index.js + 66 modules
var Layout = __webpack_require__(7961);
;// CONCATENATED MODULE: ./version_map.json
const version_map_namespaceObject = JSON.parse('[{"branch":"3.0","version":"next","hide":true},{"branch":"3.0","version":"3.0","hide":false},{"branch":"2.6","version":"2.6","hide":false},{"branch":"2.4","version":"2.4","hide":false},{"branch":"docs-cloud","version":"cloud","hide":false}]');
;// CONCATENATED MODULE: ./resource.json
const resource_namespaceObject = JSON.parse('{"pK":"https://docs.taosdata.com","fz":"下一页","IT":"上一页","Kk":"暂无更多内容","eS":"搜索","nJ":"搜索结果"}');
;// CONCATENATED MODULE: ./src/pages/search.js
const publicVersions=[];for(let i of version_map_namespaceObject){if(i.hide===false){publicVersions.push(i.version);}}const latestVersion=publicVersions[0];function unique(arr){return Array.from(new Set(arr));}class Search extends react.Component{constructor(props){super(props);this.state={search:'',searchResult:'',page:1,nextBtn:true,searchLength:0};}getSearchData(){let currentVersion=latestVersion;let linkUrl="";let pathname=window.location.pathname;for(let v of publicVersions){if(pathname.indexOf(v)>=0){currentVersion=v;linkUrl="/"+currentVersion;break;}}fetch(`${resource_namespaceObject.pK}/docs_search?search=${decodeURI(this.props.location.search).slice(1)}&p=${this.state.page}&l=10&version=v${currentVersion}&locale=cn`,{method:'get'}).then(response=>{return response.json();}).then(data=>{this.setState({searchLength:data.length});let str='';for(var i=0;i<data.length;i++){var dataItem=data[i];var title=dataItem[5]+(dataItem[4]?'-'+dataItem[4]:'');var linkStr=dataItem[0];var link='';var linkArr=linkStr.split("/");var uniqueArr=unique(linkArr);var newLinkArr=[];uniqueArr.forEach(element=>{var item=element.replace(/^\d+-/,'');newLinkArr.push(item);});if(newLinkArr[newLinkArr.length-1]=='index'){newLinkArr.pop();}newLinkArr.forEach(ele=>{link+=ele+"/";});link=linkUrl+"/"+link;let anchorlink=dataItem[4].toLowerCase().split(" ").join("-");let ve=new RegExp("("+decodeURI(this.props.location.search).slice(1)+")","gim");title=title.replace(ve,'<span style="color: red">$1</span>');var item='<li class="search-item">'+'<div class="item-title"><a href="'+link+'#'+anchorlink+'" target="_blank"> '+title+' </a></div>'+'<div class="item-content">'+dataItem[2].replace(/\[([^\]]+)\]/g,'<span style="color: red">$1</span>')+'</div>'+'</li>';str+=item;}this.setState({searchResult:str});}).catch(function(error){console.log(error);});}componentDidMount(){this.getSearchData();}previous(){this.setState({page:this.state.page-1},()=>this.getSearchData());}next(){this.setState({page:this.state.page+1},()=>this.getSearchData());}render(){return/*#__PURE__*/react.createElement(Layout/* default */.Z,{title:resource_namespaceObject.eS,description:resource_namespaceObject.nJ},/*#__PURE__*/react.createElement("main",null,/*#__PURE__*/react.createElement("div",{className:"search"},/*#__PURE__*/react.createElement("div",{className:"search-title"},/*#__PURE__*/react.createElement("h1",null,"Search Results")),/*#__PURE__*/react.createElement("div",{className:"search-content"},/*#__PURE__*/react.createElement("div",{dangerouslySetInnerHTML:{__html:this.state.searchResult==''?resource_namespaceObject.Kk:this.state.searchResult}})),/*#__PURE__*/react.createElement("div",{className:"pagination-search"},/*#__PURE__*/react.createElement("div",{className:this.state.page==1||this.state.page!=1&&this.state.searchResult==''?'pagination-page-previous-hidden':'pagination-page-previous',onClick:this.previous.bind(this)},this.state.page==1||this.state.page!=1&&this.state.searchResult==''?"":"« "+resource_namespaceObject.IT),/*#__PURE__*/react.createElement("div",{className:this.state.searchLength<10?'pagination-page-next-hidden':'pagination-page-next',onClick:this.next.bind(this)},this.state.searchLength<10?"":resource_namespaceObject.fz+" »")))));}}/* harmony default export */ const search = (Search);

/***/ })

}]);