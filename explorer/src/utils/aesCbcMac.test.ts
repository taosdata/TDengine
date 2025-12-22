import { describe, it, expect } from 'vitest';
import CryptoJS from 'crypto-js';
import { encryptCbcMac, decryptCbcMac } from './aesCbcMac';

function randomKeyB64(): string {
  return CryptoJS.lib.WordArray.random(64).toString(CryptoJS.enc.Base64); // 64 bytes => 512-bit key
}

describe('AES-CBC + HMAC (encrypt-then-MAC)', () => {
  it('encrypt/decrypt with the same test key in server side', () => {
    const keyB64 = 'WrQKXN+tJkr/PJWbJDswU/SrikmLK04YKc4NW6jX5hT6W3oIEldHUj8AulIHZ01oO4nxG9FSQRD0pzOpyQZxKQ==';
    const plaintext = 'hello taosx – AES-CBC+HMAC';
    const ct = encryptCbcMac(plaintext, keyB64);
    console.log(ct);

    expect(ct).toBeTypeOf('string');
    expect(ct).not.toContain(plaintext);

    const pt = decryptCbcMac(ct, keyB64);
    console.log(pt);
    expect(pt).toBe(plaintext);
  });
  it('roundtrips plaintext with same key', () => {
    const keyB64 = randomKeyB64();
    const plaintext = 'hello taosx – AES-CBC+HMAC';
    const ct = encryptCbcMac(plaintext, keyB64);

    expect(ct).toBeTypeOf('string');
    expect(ct).not.toContain(plaintext);

    const pt = decryptCbcMac(ct, keyB64);
    expect(pt).toBe(plaintext);
  });

  it('fails MAC verification when ciphertext is tampered', () => {
    const keyB64 = randomKeyB64();
    const plaintext = 'integrity check';
    const ct = encryptCbcMac(plaintext, keyB64);

    const buf = Buffer.from(ct, 'base64');
    buf[buf.length - 1] ^= 0xff; // flip one byte in MAC
    const tampered = buf.toString('base64');

    expect(() => decryptCbcMac(tampered, keyB64)).toThrow(/MAC/i);
  });

  it('fails MAC verification when key is wrong', () => {
    const keyB64 = randomKeyB64();
    const wrongKeyB64 = randomKeyB64();
    const plaintext = 'key mismatch should fail';

    const ct = encryptCbcMac(plaintext, keyB64);

    expect(() => decryptCbcMac(ct, wrongKeyB64)).toThrow(/MAC/i);
  });
});
