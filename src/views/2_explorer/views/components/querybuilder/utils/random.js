function random () {
  const crypto = window.crypto || window.webkitCrypto || window.mozCrypto || window.oCrypto || window.msCrypto;
  return crypto.getRandomValues(new Uint32Array(1))[0];
}
export default random;