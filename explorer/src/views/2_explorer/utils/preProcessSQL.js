// 检查该语句是不是use语句，如果是返回true
export function isUseDbSQL(sql) {
  let arrays = sql.split(" ");
  if (arrays[0].toLowerCase() != "use") {
    return false;
  }
  return true;
}

// 去除字符串前后的空格, 换行符和制表符
function trim(str) {
  return str.replace(/(^[\s\n\t]+|[\s\n\t]+$)/g, "");
}

// 将sql语句中的多个空格，换行符或制表符替换成一个空格
function replaceMoreSnt(str) {
  return str.replace(/([\s\n\t]+)/g, " ");
}

// 去除字符串尾部的分号, 和去除分号后的空格
function removeComma(str) {
  if (str && /\s*;\s*/.test(str)) {
    // return str.split(/\s*;\s*/)[0];
    return str.split(/;(?=(?:(?:[^'"]*['"]){2})*[^'"]*$)/)[0]
  } else {
    return str;
  }
}

// 检查该语句是不是select语句 且没有limit，如果是返回true
export function addLimit(sql) {
  if (/^select/i.test(sql) && !/limit/i.test(sql)) {
   sql += ' limit 1000'
  }
  return sql
}

/**
 * 对用户要执行的sql语句进行预处理
 * 预处理包括去除输入sql语句前的空格和zhi
 * @returns
 */
export async function proprocess_sql(sqlStr) {
  // 首先去除语句前后的空格，换行和制表符
  sqlStr = trim(sqlStr);
  // 然后将语句中间的连续多个空格，换行和制表符都替换成一个空格
  sqlStr = replaceMoreSnt(sqlStr);
  // 有多条语句时，通过分号取第一条
  sqlStr = removeComma(sqlStr);
  // 如果是select，加上 limit 1000
  sqlStr = addLimit(sqlStr)
  
  return { isSendSQL: true, updated_sqlStr: sqlStr };
  // if (isUseDbSQL(sqlStr)) {
  //   // 是use db语句, 前台自己处理
  //   process_usedb_sql(sqlStr);
  //   return { isSendSQL: false, updated_sqlStr: "" };
  // } else {
  //   // 不是use db语句，发送sql给tdengine处理
  // }
}
