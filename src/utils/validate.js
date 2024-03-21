/**
 * @param {string} path
 * @returns {Boolean}
 */
export function isExternal(path) {
  return /^(https?:|mailto:|tel:)/.test(path);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUsername(str) {
  return /^[-a-zA-Z0-9 _\u4e00-\u9fa5]{1,32}$/.test(str);
}

/**
 * @param {string} url
 * @returns {Boolean}
 */
export function validURL(url) {
  const reg =
    /^(https?|ftp):\/\/([a-zA-Z0-9.-]+(:[a-zA-Z0-9.&%$-]+)*@)*((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3}|([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+\.(com|edu|gov|int|mil|net|org|biz|arpa|info|name|pro|aero|coop|museum|[a-zA-Z]{2}))(:[0-9]+)*(\/($|[a-zA-Z0-9.,?'\\+&%$#=~_-]+))*$/;
  return reg.test(url);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validLowerCase(str) {
  const reg = /^[a-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUpperCase(str) {
  const reg = /^[A-Z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validAlphabets(str) {
  const reg = /^[A-Za-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} email
 * @returns {Boolean}
 */
export function validEmail(email) {
  const reg = /^\w+([-+.]\w+)*@\w+(-\w+)*(\.\w+){0,2}([-.][a-zA-Z]+)$/g;
  return reg.test(email);
}

export function validPassword(password) {
  return /^(?![A-Za-z]+$)(?![A-Z0-9]+$)(?![a-z0-9]+$)(?![a-z\W]+$)(?![A-Z\W]+$)(?![0-9\W]+$)[a-zA-Z0-9_\W]{8,16}$/.test(password);
}
/**
 * @param {string} str
 * @returns {Boolean}
 */
export function isString(str) {
  if (typeof str === "string" || str instanceof String) {
    return true;
  }
  return false;
}

/**
 * @param {Array} arg
 * @returns {Boolean}
 */
export function isArray(arg) {
  if (typeof Array.isArray === "undefined") {
    return Object.prototype.toString.call(arg) === "[object Array]";
  }
  return Array.isArray(arg);
}

/**
 * 只允许数字、字母、下划线、`
 * @param {*} str 
 * @returns {Boolean}
 */
export function validDatabaseName(str) {
  const reg = /^[`a-zA-Z_]`|\w*$/;
  return reg.test(str);
}

/**
 * 只允许数字、字母、下划线
 * @param {*} str 
 * @returns {Boolean}
 */
 export function validName(str) {
  const reg = /^[a-zA-Z_]|\w*$/;
  return reg.test(str);
}

export function isIPUrl(url) {
  return /^((http|https):\/\/)?(\d{1,3}\.){3}\d{1,3}(:\d{1,5})?\/?/.test(url);
}

export function validUnit(arg) {
  return /^(([1-9][0-9]*))+(([hdm0-9]))(,(([1-9][0-9]*))+(([hdm0-9]))){0,2}$/g.test(arg)
}

export function validRetentions(arg) {
  return /^[1-9]\d*[dhms]:[1-9]\d*[dhms](,[1-9]\d*[dhms]:[1-9]\d*[dhms]){0,2}$/g.test(arg)
}

export function validStreamSql(sql) {
  return /^create stream/i.test(sql)
}

export function validTopicSql(sql) {
  return /^create topic/i.test(sql)
}

export function validDir(arg) {
  return /^[(\s\S)\/]*$/g.test(arg)
}

//linux文件路径校验
export function validPath(arg) {
  if((String.raw`${arg}`).includes('\\')){//windows路径
    arg=(String.raw`${arg}`).replace(/\\/g,'\\\\')
    return /^[a-zA-Z]:(\/|\\)(\s\S)*/g.test(arg)
  }else{
    return /^(\/|\\)[(\s\S)\(/|\\)]*$/g.test(arg)
  }
  
}

// 获取数据类型
export function getType(data) {
  return Object.prototype.toString.call(data).slice(8, -1).toLowerCase();
}

// 是否为对象
export function isObject(arg) {
  return getType(arg) === 'object';
}