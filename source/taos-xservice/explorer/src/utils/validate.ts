/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUsername(str: string) {
  return /^[-a-zA-Z0-9 _\u4e00-\u9fa5]{1,32}$/.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validLowerCase(str: string) {
  const reg = /^[a-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUpperCase(str: string) {
  const reg = /^[A-Z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validAlphabets(str: string) {
  const reg = /^[A-Za-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} email
 * @returns {Boolean}
 */
export function validEmail(email: string) {
  const reg = /^\w+([-+.]\w+)*@\w+(-\w+)*(\.\w+){0,2}([-.][a-zA-Z]+)$/g;
  return reg.test(email);
}

// EnableStrongPassword 时，采用此规则，要求必须至少包含大写字母、小写字母、数字、特殊字符中的三类
export function validPassword(password: string) {
  return /^(?![A-Za-z]+$)(?![A-Z0-9]+$)(?![a-z0-9]+$)(?![a-z\W]+$)(?![A-Z\W]+$)(?![0-9\W]+$)[a-zA-Z0-9_\W]{8,255}$/.test(
    password
  );
}

// EnableStrongPassword = false 时，采用此规则
export function validPasswordNotStrict(password: string) {
  return /^[a-zA-Z0-9_\W]{8,255}$/.test(
    password
  );
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function isString(str: any) {
  if (typeof str === 'string' || str instanceof String) {
    return true;
  }
  return false;
}

/**
 * @param {Array} arg
 * @returns {Boolean}
 */
export function isArray(arg: any) {
  if (typeof Array.isArray === 'undefined') {
    return Object.prototype.toString.call(arg) === '[object Array]';
  }
  return Array.isArray(arg);
}

/**
 * 只允许数字、字母、下划线、`
 * @param {*} str
 * @returns {Boolean}
 */
export function validDatabaseName(str: string) {
  const reg = /^[`a-zA-Z_]`|\w*$/;
  return reg.test(str);
}

/**
 * 只允许数字、字母、下划线
 * @param {*} str
 * @returns {Boolean}
 */
export function validName(str: string) {
  const reg = /^[a-zA-Z_]|\w*$/;
  return reg.test(str);
}

export function isIPUrl(url: string) {
  return /^((http|https):\/\/)?(\d{1,3}\.){3}\d{1,3}(:\d{1,5})?\/?/.test(url);
}

export function validUnit(arg: string) {
  return /^(([1-9][0-9]*))+(([hdm0-9]))(,(([1-9][0-9]*))+(([hdm0-9]))){0,2}$/g.test(arg);
}

export function validRetentions(arg: string) {
  return /^[1-9]\d*[dhms]:[1-9]\d*[dhms](,[1-9]\d*[dhms]:[1-9]\d*[dhms]){0,2}$/g.test(arg);
}

export function validStreamSql(sql: string) {
  return /^create stream/i.test(sql);
}

export function validTopicSql(sql: string) {
  return /^create topic/i.test(sql);
}

export function validDir(arg: string) {
  return /^[(\s\S)\\/]*$/g.test(arg);
}

//linux文件路径校验
export function validPath(arg: string) {
  if (String.raw`${arg}`.includes('\\')) {
    //windows路径
    arg = String.raw`${arg}`.replace(/\\/g, '\\\\');
    return /^[a-zA-Z]:(\/|\\)(\s\S)*/g.test(arg);
  } else {
    return /^(\/|\\)[(\s\S)(/|\\)]*$/g.test(arg);
  }
}

// 获取数据类型
export function getType(data: any) {
  return Object.prototype.toString.call(data).slice(8, -1).toLowerCase();
}

// 是否为对象
export function isObject(arg: any) {
  return getType(arg) === 'object';
}
