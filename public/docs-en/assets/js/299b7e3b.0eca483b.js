"use strict";
(self["webpackChunkdocs"] = self["webpackChunkdocs"] || []).push([[3755],{

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

/***/ 5031:
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
/* harmony import */ var _root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7462);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(7294);
/* harmony import */ var _mdx_js_react__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3905);
/* @jsxRuntime classic */ /* @jsx mdx */ /* @jsxFrag React.Fragment */const frontMatter={title:'Name and Size Limits',sidebar_label:'Name and Size Limits',description:'This document describes the name and size limits in TDengine.'};const contentTitle=undefined;const metadata={"unversionedId":"taos-sql/limit","id":"taos-sql/limit","title":"Name and Size Limits","description":"This document describes the name and size limits in TDengine.","source":"@site/docs/12-taos-sql/19-limit.md","sourceDirName":"12-taos-sql","slug":"/taos-sql/limit","permalink":"/docs-en/taos-sql/limit","draft":false,"tags":[],"version":"current","sidebarPosition":19,"frontMatter":{"title":"Name and Size Limits","sidebar_label":"Name and Size Limits","description":"This document describes the name and size limits in TDengine."},"sidebar":"defaultSidebar","previous":{"title":"Escape Characters","permalink":"/docs-en/taos-sql/escape"},"next":{"title":"Reserved Keywords","permalink":"/docs-en/taos-sql/keywords"}};const assets={};const toc=[{value:'Naming Rules',id:'naming-rules',level:2},{value:'Password Rules',id:'password-rules',level:2},{value:'General Limits',id:'general-limits',level:2},{value:'Restrictions of Table/Column Names',id:'restrictions-of-tablecolumn-names',level:2},{value:'Name Restrictions of Table/Column',id:'name-restrictions-of-tablecolumn',level:3},{value:'Name Restrictions After Escaping',id:'name-restrictions-after-escaping',level:3}];const layoutProps={toc};const MDXLayout="wrapper";function MDXContent(_ref){let{components,...props}=_ref;return (0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)(MDXLayout,(0,_root_enterprise_docs_enterprise_docs_en_node_modules_babel_runtime_helpers_esm_extends_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .Z)({},layoutProps,props,{components:components,mdxType:"MDXLayout"}),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"naming-rules"},`Naming Rules`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ol",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Names can include letters, digits, and underscores (_).`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Names can begin with letters or underscores (_) but not with digits.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Names are not case-sensitive.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ol"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`Rules for names with escape characters are as follows:
You can escape a name by enclosing it in backticks (\`). In this way, you can reuse keyword names for table names. However, the first three naming rules no longer apply.
Table and column names that are enclosed in escape characters are still subject to length limits. When the length of such a name is calculated, the escape characters are not included. Names specified using escape character are case-sensitive.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"li"},`For example, `,`\``,`aBc`,`\``,` and `,`\``,`abc`,`\``,` are different table or column names, but "abc" and "aBc" are same names because internally they are all "abc".
Only ASCII visible characters can be used with escape character.`))),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"password-rules"},`Password Rules`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"p"},`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The following characters cannot occur in a password: single quotation marks ('), double quotation marks ("), backticks (\`), backslashes (`,`\\`,`), and spaces.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"general-limits"},`General Limits`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("ul",null,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum length of database name is 64 bytes`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum length of table name is 192 bytes, excluding the database name prefix and the separator.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum length of each data row is 48K(64K since version 3.0.5.0) bytes. Note that the upper limit includes the extra 2 bytes consumed by each column of BINARY/NCHAR type.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum length of a column name is 64 bytes.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum number of columns is 4096. There must be at least 2 columns, and the first column must be timestamp.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum length of a tag name is 64 bytes`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum number of tags is 128. There must be at least 1 tag. The total length of tag values cannot exceed 16 KB.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum length of single SQL statement is 1 MB (1048576 bytes). `),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`At most 4096 columns can be returned by `,(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("inlineCode",{parentName:"li"},`SELECT`),`. Functions in the query statement constitute columns. An error is returned if the limit is exceeded.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`Maximum numbers of databases, STables, tables are dependent only on the system resources.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The number of replicas can only be 1 or 3.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum length of a username is 23 bytes.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum length of a password is 31 bytes.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum number of rows depends on system resources.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("li",{parentName:"ul"},`The maximum number of vnodes in a database is 1024.`)),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h2",{"id":"restrictions-of-tablecolumn-names"},`Restrictions of Table/Column Names`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"name-restrictions-of-tablecolumn"},`Name Restrictions of Table/Column`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`The name of a table or column can only be composed of ASCII characters, digits and underscore and it cannot start with a digit. The maximum length is 192 bytes. Names are case insensitive. The name mentioned in this rule doesn't include the database name prefix and the separator.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("h3",{"id":"name-restrictions-after-escaping"},`Name Restrictions After Escaping`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`To support more flexible table or column names, new escape character "`,`\``,`" is introduced in TDengine to avoid the conflict between table name and keywords and break the above restrictions for table names. The escape character is not counted in the length of table name.
With escaping, the string inside escape characters are case sensitive, i.e. will not be converted to lower case internally. The table names specified using escape character are case sensitive.`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",null,`For example:
`,`\``,`aBc`,`\``,` and `,`\``,`abc`,`\``,` are different table or column names, but "abc" and "aBc" are same names because internally they are all "abc".`),(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("admonition",{"type":"note"},(0,_mdx_js_react__WEBPACK_IMPORTED_MODULE_1__/* .mdx */ .kt)("p",{parentName:"admonition"},`The characters inside escape characters must be printable characters.`)));};MDXContent.isMDXComponent=true;

/***/ })

}]);