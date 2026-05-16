import CryptoJS from 'crypto-js';

//加密
export function encrypt(data: string) {
  const encryptedData = CryptoJS.AES.encrypt(
    data,
    // spellchecker:off
    `-----BEGIN PUBLIC KEY-----
  MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC//nB6rRTnxCU2bMBGatp1N1Q0
  kuSEZl3Ot2EQMlNwINYTm7izxjTyA1pgmBmotAXVZuZNviJNUZUMBn73bIjso1l2
  qhwe/FcewPjP2ubbdf89yWPnen/wRGo+Q0QRmt1q7eDeVTJMC4LVdetuv6QABnUJ
  +siG1ILDsJ2BsYMBMwIDAQAB
  -----END PUBLIC KEY-----`
    // spellchecker:on
  ).toString(); // 使用AES算法加密数据
  return encryptedData;
}
//解密
export function decrypt(encryptedData: string) {
  const decryptedMessage = CryptoJS.AES.decrypt(
    encryptedData,
    // spellchecker:off
    `-----BEGIN PUBLIC KEY-----
  MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC//nB6rRTnxCU2bMBGatp1N1Q0
  kuSEZl3Ot2EQMlNwINYTm7izxjTyA1pgmBmotAXVZuZNviJNUZUMBn73bIjso1l2
  qhwe/FcewPjP2ubbdf89yWPnen/wRGo+Q0QRmt1q7eDeVTJMC4LVdetuv6QABnUJ
  +siG1ILDsJ2BsYMBMwIDAQAB
  -----END PUBLIC KEY-----`
    // spellchecker:on
  ).toString(CryptoJS.enc.Utf8); // 使用AES算法解密数据

  return decryptedMessage;
}
