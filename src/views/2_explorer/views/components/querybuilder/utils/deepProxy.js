function Archiver(callback) {
  var arr = [];
  Object.defineProperty(this, 'data', {
    get: function() {
      return arr;
    },
    set: function(value) {
      arr = value;
      callback && callback(arr);
    }
  });
  // this.__proto__ = arr;
  this.forEach = Array.prototype.forEach
  this.push = function(params) {
    arr.push(params);
    callback && callback(arr);
  };
}
function deepProxy (callback) {
  return new Archiver(callback);
}
export default deepProxy;