export function isObject(obj: string | any[]) {
  return Object.prototype.toString.call(obj) === '[object Object]';
}

const hasOwnProperty = Object.prototype.hasOwnProperty;

export function hasOwn(obj: any, key: any) {
  return hasOwnProperty.call(obj, key);
}

// TODO: use native Array.find, Array.findIndex when IE support is dropped
export const arrayFindIndex = function (arr: string | any[], pred: (arg0: any) => any) {
  for (let i = 0; i !== arr.length; ++i) {
    if (pred(arr[i])) {
      return i;
    }
  }
  return -1;
};

export const arrayFind = function (arr: any[], pred: any) {
  const idx = arrayFindIndex(arr, pred);
  return idx !== -1 ? arr[idx] : undefined;
};

export const kebabCase = function (str: string) {
  const hyphenateRE = /([^-])([A-Z])/g;
  return str.replace(hyphenateRE, '$1-$2').replace(hyphenateRE, '$1-$2').toLowerCase();
};

export const looseEqual = function (a: string | any, b: string | any[]) {
  const isObjectA = isObject(a);
  const isObjectB = isObject(b);
  if (isObjectA && isObjectB) {
    return JSON.stringify(a) === JSON.stringify(b);
  } else if (!isObjectA && !isObjectB) {
    return String(a) === String(b);
  } else {
    return false;
  }
};

export const arrayEquals = function (arrayA: string | any[], arrayB: string | any[]) {
  arrayA = arrayA || [];
  arrayB = arrayB || [];

  if (arrayA.length !== arrayB.length) {
    return false;
  }

  for (let i = 0; i < arrayA.length; i++) {
    if (!looseEqual(arrayA[i], arrayB[i])) {
      return false;
    }
  }

  return true;
};

export const isEqual = function (value1: string | any[], value2: string | any[]) {
  if (Array.isArray(value1) && Array.isArray(value2)) {
    return arrayEquals(value1, value2);
  }
  return looseEqual(value1, value2);
};

export const isEmpty = function (val: { message?: any; length?: any; size?: any } | null) {
  // null or undefined
  if (val == null) return true;

  if (typeof val === 'boolean') return false;

  if (typeof val === 'number') return !val;

  if (val instanceof Error) return val.message === '';

  switch (Object.prototype.toString.call(val)) {
    // String or Array
    case '[object String]':
    case '[object Array]':
      return !val.length;

    // Map or Set or File
    case '[object File]':
    case '[object Map]':
    case '[object Set]': {
      return !val.size;
    }
    // Plain Object
    case '[object Object]': {
      return !Object.keys(val).length;
    }
  }

  return false;
};

// 随机生成uuid
export function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0,
      v = c == 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function concatS3Config(backupPlan: any) {
  if (backupPlan.s3_enable === false) {
    return 's3_enable=false';
  }

  let s3Config = `s3_enable=true&s3_endpoint=${backupPlan.s3_endpoint}&s3_access_key_id=${backupPlan.s3_access_key_id}&s3_secret_access_key=${backupPlan.s3_secret_access_key}&s3_region=${backupPlan.s3_region}&s3_bucket=${backupPlan.s3_bucket}&s3_object_prefix=${backupPlan.s3_object_prefix || ''}`;
  if (backupPlan.backup_retention_period_value) {
    s3Config += `&backup_retention_period=${backupPlan.backup_retention_period_value}${backupPlan.backup_retention_period_unit}`;
  }
  if (backupPlan.backup_retention_size) {
    s3Config += `&backup_retention_size=${backupPlan.backup_retention_size}`;
  }

  return s3Config;
}
