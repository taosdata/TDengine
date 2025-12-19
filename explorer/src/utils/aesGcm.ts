// import crypto from 'crypto-js';
import * as aes from '@juanelas/aes-gcm';

export interface EncryptedResult {
  encrypted: string;
  algorithm: string;
  timestamp: number;
  iv?: string;
}

export interface EncryptionKey {
  key: CryptoKey;
  type: 'generated' | 'derived';
  createdAt: number;
}

export interface PasswordDerivationParams {
  salt: Uint8Array;
  iterations: number;
  hash: 'SHA-256' | 'SHA-384' | 'SHA-512';
}

export interface CryptoError extends Error {
  code: string;
  details?: unknown;
}

export interface AesGcmEncryptedB64Data {
  data: string;
  algorithm: string;
  version: string;
}
export interface AesGcmEncryptedData {
  data: ArrayBuffer;
  iv: Uint8Array;
  algorithm: string;
  version: string;
}

// 工具方法
function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

const subtle: any = {
  importKey: function (
    format: KeyFormat,
    keyData: BufferSource,
    algorithm: AlgorithmIdentifier,
    extractable: boolean,
    keyUsages: KeyUsage[]
  ): Promise<CryptoKey> {
    try {
      return window.crypto.subtle.importKey(
        format as Exclude<KeyFormat, 'jwk'>,
        keyData,
        algorithm,
        extractable,
        keyUsages
      );
    } catch {
      return aes.importKey(keyData as Buffer);
    }
  },
  encrypt: function (params: AesGcmParams, key: CryptoKey, data: ArrayBufferLike | Buffer): Promise<ArrayBuffer> {
    try {
      return window.crypto.subtle.encrypt(params, key, data);
    } catch {
      return aes.encrypt(data, key, params.iv as Buffer).then(ciphertext => ciphertext.encrypted);
    }
  },
  decrypt: function (params: AesGcmParams, key: CryptoKey, data: ArrayBufferLike | Buffer): Promise<ArrayBuffer> {
    try {
      return window.crypto.subtle.decrypt(params, key, data);
    } catch {
      return aes.decrypt({ encrypted: arrayBufferToBase64(data), iv: arrayBufferToBase64(params.iv as Buffer) }, key);
    }
  },
  generateKey: function (params: AesKeyGenParams, extractable: boolean, keyUsages: KeyUsage[]): Promise<CryptoKey> {
    try {
      return window.crypto.subtle.generateKey(params, extractable, keyUsages);
    } catch {
      return aes.generateKey(params.length as aes.aesKeyLength, extractable);
    }
  },
  exportKey: function (format: KeyFormat, key: CryptoKey): Promise<ArrayBuffer | JsonWebKey> {
    return window.crypto.subtle.exportKey(format, key);
  }
};

// utils/crypto/aes-gcm.ts
class AesGcmCrypto {
  private readonly algorithm = 'AES-GCM';
  private readonly keyLength = 256;
  private readonly ivLength = 12; // 12 bytes for GCM
  private readonly tagLength = 16; // 16 bytes for authentication tag
  private readonly version = '1.0.0';

  /**
   * 生成随机 AES-GCM 密钥
   */
  async generateKey(): Promise<CryptoKey> {
    try {
      return await subtle.generateKey(
        {
          name: this.algorithm,
          length: this.keyLength
        },
        true, // extractable
        ['encrypt', 'decrypt']
      );
    } catch (error) {
      throw this.createError('KEY_GENERATION_FAILED', '生成密钥失败', error);
    }
  }

  async mergeIvAndData(iv: Uint8Array, data: ArrayBuffer): Promise<ArrayBuffer> {
    const merged = new Uint8Array(iv.length + data.byteLength);
    merged.set(iv, 0);
    merged.set(new Uint8Array(data), iv.length);
    return merged.buffer;
  }

  async encryptB64(plaintext: string, key: CryptoKey, iv?: Uint8Array): Promise<AesGcmEncryptedB64Data> {
    const encryptedData = await this.encrypt(plaintext, key, iv);
    const mergedData = await this.mergeIvAndData(encryptedData.iv, encryptedData.data);
    return {
      data: arrayBufferToBase64(mergedData),
      algorithm: encryptedData.algorithm,
      version: encryptedData.version
    };
  }

  async decryptB64(encryptedB64Data: AesGcmEncryptedB64Data | string, key: CryptoKey): Promise<string> {
    if (typeof encryptedB64Data === 'string') {
      const mergedData = this.base64ToArrayBuffer(encryptedB64Data);
      const iv = new Uint8Array(mergedData.slice(0, this.ivLength));
      const data = mergedData.slice(this.ivLength);
      const encryptedData: AesGcmEncryptedData = {
        data,
        iv,
        algorithm: this.algorithm,
        version: this.version
      };
      return this.decrypt(encryptedData, key);
    }
    const mergedData = this.base64ToArrayBuffer(encryptedB64Data.data);
    const iv = new Uint8Array(mergedData.slice(0, this.ivLength));
    const data = mergedData.slice(this.ivLength);
    const encryptedData: AesGcmEncryptedData = {
      data,
      iv,
      algorithm: encryptedB64Data.algorithm,
      version: encryptedB64Data.version
    };
    return this.decrypt(encryptedData, key);
  }
  /**
   * 加密文本数据
   */
  async encrypt(input: string, key: CryptoKey, iv?: Uint8Array): Promise<AesGcmEncryptedData> {
    try {
      // 生成随机 IV（如果未提供）
      const initializationVector = iv || this.generateRandomBytes(this.ivLength);

      const encoder = new TextEncoder();
      const data = encoder.encode(input);

      const encrypted = await subtle.encrypt(
        {
          name: this.algorithm,
          iv: initializationVector.slice(0, this.ivLength)
        },
        key,
        data
      );

      return {
        data: encrypted,
        iv: initializationVector,
        algorithm: this.algorithm,
        version: this.version
      };
    } catch (error) {
      throw this.createError('ENCRYPTION_FAILED', '加密失败', error);
    }
  }

  /**
   * 解密文本数据
   */
  async decrypt(encryptedData: AesGcmEncryptedData, key: CryptoKey): Promise<string> {
    // 验证算法和版本兼容性
    if (encryptedData.algorithm !== this.algorithm) {
      throw new Error(`不支持的加密算法: ${encryptedData.algorithm}`);
    }

    try {
      if (encryptedData.version !== this.version) {
        console.warn(`加密数据版本不匹配: ${encryptedData.version}, 期望: ${this.version}`);
      }

      const decrypted = await subtle.decrypt(
        {
          name: this.algorithm,
          iv: new Uint8Array(encryptedData.iv),
          tagLength: this.tagLength * 8
        },
        key,
        encryptedData.data
      );

      const decoder = new TextDecoder();
      return decoder.decode(decrypted);
    } catch (error) {
      if (error instanceof Error && error.message.includes('decryption')) {
        throw this.createError('DECRYPTION_FAILED', '解密失败: 可能密钥不正确或数据被篡改', error);
      }
      throw this.createError('DECRYPTION_FAILED', '解密失败', error);
    }
  }

  /**
   * 加密对象数据
   */
  async encryptObject<T extends object>(obj: T, key: CryptoKey, iv?: Uint8Array): Promise<AesGcmEncryptedData> {
    const jsonString = JSON.stringify(obj);
    return this.encrypt(jsonString, key, iv);
  }

  /**
   * 解密对象数据
   */
  async decryptObject<T extends object>(encryptedData: AesGcmEncryptedData, key: CryptoKey): Promise<T> {
    const decryptedString = await this.decrypt(encryptedData, key);
    return JSON.parse(decryptedString) as T;
  }

  /**
   * 导出密钥为 Base64 字符串
   */
  async exportKey(key: CryptoKey): Promise<string> {
    try {
      const exported = await subtle.exportKey('raw', key);
      return arrayBufferToBase64(exported);
    } catch (error) {
      throw this.createError('KEY_EXPORT_FAILED', '导出密钥失败', error);
    }
  }

  /**
   * 从 Base64 字符串导入密钥
   */
  async importKey(keyBase64: string): Promise<CryptoKey> {
    try {
      const keyData = this.base64ToArrayBuffer(keyBase64);
      return await subtle.importKey(
        'raw',
        keyData,
        {
          name: this.algorithm,
          length: this.keyLength
        },
        true,
        ['encrypt', 'decrypt']
      );
    } catch (error) {
      throw this.createError('KEY_IMPORT_FAILED', '导入密钥失败', error);
    }
  }

  /**
   * 生成随机字节数组
   */
  generateRandomBytes(length: number): Uint8Array {
    return window.crypto.getRandomValues(new Uint8Array(length));
  }

  /**
   * 生成随机盐值
   */
  generateSalt(length: number = 16): Uint8Array {
    return this.generateRandomBytes(length);
  }

  /**
   * 检查浏览器是否支持 Web Crypto API
   */
  isSupported(): boolean {
    return true;
  }

  private base64ToArrayBuffer(base64: string): ArrayBuffer {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  private createError(code: string, message: string, originalError?: unknown): CryptoError {
    return {
      name: 'CryptoError',
      message,
      code,
      details: originalError
    } as CryptoError;
  }
}

export default new AesGcmCrypto();
