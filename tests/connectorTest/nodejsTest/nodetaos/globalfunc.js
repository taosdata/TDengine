/* Wrap a callback, reduce code amount */
function wrapCB(callback, input) {
  if (typeof callback === 'function') {
    callback(input);
  }
  return;
}
global.wrapCB = wrapCB;
function toTaosTSString(date) {
  date = new Date(date);
  let tsArr = date.toISOString().split("T")
  return tsArr[0] + " " + tsArr[1].substring(0, tsArr[1].length-1);
}
global.toTaosTSString = toTaosTSString;
