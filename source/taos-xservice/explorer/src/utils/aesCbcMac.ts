import CryptoJS from 'crypto-js';

// Split a 64-byte key (base64) into AES key (32B) and HMAC key (32B)
function splitKey(keyB64: string) {
  const full = CryptoJS.enc.Base64.parse(keyB64);
  if (full.sigBytes !== 64) throw new Error('Key must be 64 bytes (base64).');
  const aesKey = CryptoJS.lib.WordArray.create(full.words.slice(0, 8), 32); // 8 words * 4 = 32B
  const hmacKey = CryptoJS.lib.WordArray.create(full.words.slice(8, 16), 32); // next 32B
  return { aesKey, hmacKey };
}

// Constant-time compare for Uint8Array
function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}

function wordArrayToUint8(wa: CryptoJS.lib.WordArray): Uint8Array {
  const { words, sigBytes } = wa;
  const u8 = new Uint8Array(sigBytes);
  let idx = 0;
  for (let i = 0; i < words.length; i++) {
    const w = words[i];
    u8[idx++] = (w >>> 24) & 0xff;
    if (idx >= sigBytes) break;
    u8[idx++] = (w >>> 16) & 0xff;
    if (idx >= sigBytes) break;
    u8[idx++] = (w >>> 8) & 0xff;
    if (idx >= sigBytes) break;
    u8[idx++] = w & 0xff;
    if (idx >= sigBytes) break;
  }
  return u8;
}

// Encrypt: returns base64(iv || ciphertext || mac)
export function encryptCbcMac(plaintext: string, keyB64: string): string {
  const { aesKey, hmacKey } = splitKey(keyB64);
  const iv = CryptoJS.lib.WordArray.random(16); // 16 bytes
  const encrypted = CryptoJS.AES.encrypt(plaintext, aesKey, {
    iv,
    mode: CryptoJS.mode.CBC,
    padding: CryptoJS.pad.Pkcs7
  });

  // iv || ciphertext
  const ivCipher = iv.clone().concat(encrypted.ciphertext);

  // MAC over iv||ciphertext
  const mac = CryptoJS.HmacSHA256(ivCipher, hmacKey);

  // iv || ciphertext || mac
  const payload = ivCipher.clone().concat(mac);
  return payload.toString(CryptoJS.enc.Base64);
}

// Decrypt: expects base64(iv || ciphertext || mac)
export function decryptCbcMac(payloadB64: string, keyB64: string): string {
  const { aesKey, hmacKey } = splitKey(keyB64);
  const payload = CryptoJS.enc.Base64.parse(payloadB64);

  const ivBytes = 16;
  const macBytes = 32;
  if (payload.sigBytes < ivBytes + macBytes) throw new Error('Ciphertext too short');

  // Extract iv, ciphertext, mac
  const iv = CryptoJS.lib.WordArray.create(payload.words.slice(0, 4), ivBytes); // 4 words = 16B
  const cipherBytes = payload.sigBytes - ivBytes - macBytes;
  const cipher = CryptoJS.lib.WordArray.create(payload.words.slice(4, 4 + Math.ceil(cipherBytes / 4)), cipherBytes);
  const mac = CryptoJS.lib.WordArray.create(payload.words.slice(4 + Math.ceil(cipherBytes / 4)), macBytes);

  // Verify MAC over iv||cipher
  const calcMac = CryptoJS.HmacSHA256(iv.clone().concat(cipher), hmacKey);
  if (!constantTimeEqual(wordArrayToUint8(calcMac), wordArrayToUint8(mac))) {
    throw new Error('MAC verification failed');
  }

  // Decrypt
  const decrypted = CryptoJS.AES.decrypt({ ciphertext: cipher } as any, aesKey, {
    iv,
    mode: CryptoJS.mode.CBC,
    padding: CryptoJS.pad.Pkcs7
  });
  return decrypted.toString(CryptoJS.enc.Utf8);
}

export default {
  encryptCbcMac,
  decryptCbcMac
};
