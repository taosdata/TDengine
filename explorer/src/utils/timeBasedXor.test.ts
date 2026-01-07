import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  TimeBasedXor,
  XorError,
  InvalidDataError,
  InvalidTimestampError,
  ExpiredError,
  Base64Error,
  Utf8Error,
  decryptXor
} from './timeBasedXor';

describe('TimeBasedXor', () => {
  let originalDateNow: () => number;

  beforeEach(() => {
    // 保存原始的 Date.now
    originalDateNow = Date.now;
  });

  afterEach(() => {
    // 恢复原始的 Date.now
    Date.now = originalDateNow;
  });

  describe('构造函数', () => {
    it('应该正确设置允许的持续时间', () => {
      const xor = new TimeBasedXor(300);
      expect(xor).toBeInstanceOf(TimeBasedXor);

      const xor2 = new TimeBasedXor(0);
      expect(xor2).toBeInstanceOf(TimeBasedXor);
    });
  });

  describe('genKey 方法', () => {
    it('应该生成24字节的密钥', () => {
      const key = (TimeBasedXor as any).genKey(1704556800);
      expect(key).toBeInstanceOf(Uint8Array);
      expect(key.length).toBe(24);
    });

    it('密钥前16字节应该是固定密钥', () => {
      const timestamp = 1704556800;
      const key = (TimeBasedXor as any).genKey(timestamp);

      // 检查前16字节
      const expectedKeyPart = new TextEncoder().encode('taosdataexplorer');
      for (let i = 0; i < 16; i++) {
        expect(key[i]).toBe(expectedKeyPart[i]);
      }
    });

    it('密钥后8字节应该是时间戳的大端序表示', () => {
      const timestamp = 1704556800;
      const key = (TimeBasedXor as any).genKey(timestamp);

      // 创建8字节的大端序缓冲区
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setBigUint64(0, BigInt(timestamp), false);
      const expectedBytes = new Uint8Array(buffer);

      // 检查后8字节
      for (let i = 0; i < 8; i++) {
        expect(key[16 + i]).toBe(expectedBytes[i]);
      }
    });
  });

  describe('encryptXor 和 decryptXor 方法', () => {
    it('应该正确进行XOR加密和解密', () => {
      const xor = new TimeBasedXor(300);
      const key = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
      const data = new Uint8Array([0x10, 0x20, 0x30, 0x40, 0x50]);

      const encrypted = (xor as any).encryptXor(key, data);
      expect(encrypted).toBeInstanceOf(Uint8Array);
      expect(encrypted.length).toBe(data.length);

      const decrypted = (xor as any).decryptXor(key, encrypted);
      expect(decrypted).toEqual(data);
    });

    it('XOR加密解密应该是可逆的', () => {
      const xor = new TimeBasedXor(300);
      const key = new Uint8Array(32);
      crypto.getRandomValues(key);

      const data = new TextEncoder().encode('Hello, World! 这是一个测试');

      const encrypted = (xor as any).encryptXor(key, data);
      const decrypted = (xor as any).decryptXor(key, encrypted);

      expect(decrypted).toEqual(data);
    });

    it('当密钥短于数据时应该循环使用', () => {
      const xor = new TimeBasedXor(300);
      const key = new Uint8Array([0x01, 0x02]);
      const data = new Uint8Array([0x10, 0x20, 0x30, 0x40]);

      const encrypted = (xor as any).encryptXor(key, data);

      // 手动计算期望值
      const expected = new Uint8Array([
        0x10 ^ 0x01, // 0x11
        0x20 ^ 0x02, // 0x22
        0x30 ^ 0x01, // 0x31 (循环使用key[0])
        0x40 ^ 0x02 // 0x42 (循环使用key[1])
      ]);

      expect(encrypted).toEqual(expected);
    });
  });

  describe('encrypt 方法', () => {
    it('应该返回正确的格式: timestamp.base64', () => {
      const fixedTimestamp = 1704556800;
      Date.now = vi.fn(() => fixedTimestamp * 1000);

      const xor = new TimeBasedXor(300);
      const encrypted = xor.encrypt('测试数据');

      expect(encrypted).toMatch(/^\d+\.[A-Za-z0-9+/=]+$/);

      const [timestampStr, base64Data] = encrypted.split('.');
      expect(timestampStr).toBe(fixedTimestamp.toString());
      expect(base64Data).toBeTruthy();
    });

    it('应该支持字符串输入', () => {
      const xor = new TimeBasedXor(300);
      const encrypted = xor.encrypt('Hello, World!');
      expect(encrypted).toBeTruthy();
    });

    it('应该支持Uint8Array输入', () => {
      const xor = new TimeBasedXor(300);
      const data = new TextEncoder().encode('二进制数据');
      const encrypted = xor.encrypt(data);
      expect(encrypted).toBeTruthy();
    });

    it('应该支持ArrayBuffer输入', () => {
      const xor = new TimeBasedXor(300);
      const encoder = new TextEncoder();
      const data = encoder.encode('ArrayBuffer数据').buffer;
      const encrypted = xor.encrypt(data);
      expect(encrypted).toBeTruthy();
    });

    it('相同的输入应该产生不同的输出（由于时间戳）', () => {
      const xor = new TimeBasedXor(300);

      // 模拟时间变化
      let time = 1704556800;
      Date.now = vi.fn(() => {
        time += 1;
        return time * 1000;
      });

      const encrypted1 = xor.encrypt('相同数据');
      const encrypted2 = xor.encrypt('相同数据');

      expect(encrypted1).not.toBe(encrypted2);
    });
  });

  describe('decrypt 方法', () => {
    it('应该能够解密加密的数据', () => {
      const fixedTimestamp = 1704556800;
      Date.now = vi.fn(() => fixedTimestamp * 1000);

      const xor = new TimeBasedXor(300);
      const originalData = '这是一条测试消息！';

      const encrypted = xor.encrypt(originalData);
      const decrypted = xor.decrypt(encrypted);

      expect(decrypted).toBe(originalData);
    });

    it('应该处理Unicode字符', () => {
      const xor = new TimeBasedXor(300);
      const testCases = [
        'Hello, World!',
        '测试中文',
        '🎉 Emoji 😊',
        '🚀 火箭 🚀',
        'Line1\nLine2\tTab',
        '特殊字符: !@#$%^&*()'
      ];

      for (const testData of testCases) {
        const encrypted = xor.encrypt(testData);
        const decrypted = xor.decrypt(encrypted);
        expect(decrypted).toBe(testData);
      }
    });

    it('应该抛出InvalidDataError当数据格式不正确时', () => {
      const xor = new TimeBasedXor(300);

      expect(() => xor.decrypt('invalid-data')).toThrow(InvalidDataError);
      expect(() => xor.decrypt('timestampOnly.')).toThrow(InvalidDataError);
      expect(() => xor.decrypt('.base64Only')).toThrow(InvalidDataError);
      expect(() => xor.decrypt('')).toThrow(InvalidDataError);
    });

    it('应该抛出InvalidTimestampError当时间戳无效时', () => {
      const xor = new TimeBasedXor(300);

      expect(() => xor.decrypt('not-a-number.base64data')).toThrow(InvalidTimestampError);
    });

    it('应该抛出ExpiredError当数据过期时', () => {
      const xor = new TimeBasedXor(60); // 1分钟有效期

      // 设置当前时间比时间戳晚2分钟
      const oldTimestamp = 1704556800;
      const currentTime = oldTimestamp + 120; // 2分钟后
      Date.now = vi.fn(() => currentTime * 1000);

      const expiredData = `${oldTimestamp}.SGVsbG8=`; // "Hello"的base64

      expect(() => xor.decrypt(expiredData)).toThrow(ExpiredError);
    });

    it('应该正确处理未过期的数据', () => {
      const xor = new TimeBasedXor(300); // 5分钟有效期

      const timestamp = 1704556800;
      const currentTime = timestamp + 200; // 3分20秒后
      Date.now = vi.fn(() => currentTime * 1000);

      // 加密一些测试数据
      const testData = 'Test Data';
      const encrypted = xor.encrypt(testData);

      // 应该能够成功解密
      const decrypted = xor.decrypt(encrypted);
      expect(decrypted).toBe(testData);
    });

    it('应该抛出Base64Error当Base64解码失败时', () => {
      const xor = new TimeBasedXor(300);
      const timestamp = Math.floor(Date.now() / 1000);
      const invalidBase64 = `${timestamp}.🎉🎊✨`;

      expect(() => xor.decrypt(invalidBase64)).toThrow(Base64Error);
    });

  describe('边界情况', () => {
    it('应该处理空字符串', () => {
      const xor = new TimeBasedXor(300);
      expect(() => xor.encrypt('')).toThrow(InvalidDataError);
    });

    it('应该处理大量数据', () => {
      const xor = new TimeBasedXor(300);
      const largeData = 'A'.repeat(10000);
      const encrypted = xor.encrypt(largeData);
      const decrypted = xor.decrypt(encrypted);
      expect(decrypted).toBe(largeData);
    });

    it('应该处理零秒有效期（立即过期）', () => {
      const xor = new TimeBasedXor(0);

      const data = 'Test Data';
      const encrypted = xor.encrypt(data);

      // 解密应该立即失败，因为任何非当前时间的数据都会过期
      expect(() => xor.decrypt(encrypted)).toThrow(ExpiredError);
    });

    it('应该正确处理负数时间戳（如果系统支持）', () => {
      const xor = new TimeBasedXor(300);

      // 1970年之前的时间戳
      const negativeTimestamp = -86400; // 1970-01-01 前一天
      const data = `${negativeTimestamp}.SGVsbG8=`; // "Hello"的base64

      // 这应该抛出ExpiredError，因为时间戳太旧
      expect(() => xor.decrypt(data)).toThrow(ExpiredError);
    });
  });
});

describe('decryptXor 函数', () => {
  it('应该正确进行XOR解密', () => {
    const key = new Uint8Array([0x01, 0x02, 0x03]);
    const data = new Uint8Array([0x10, 0x20, 0x30, 0x40, 0x50]);

    const encrypted = new Uint8Array([
      0x10 ^ 0x01, // 0x11
      0x20 ^ 0x02, // 0x22
      0x30 ^ 0x03, // 0x33
      0x40 ^ 0x01, // 0x41 (循环使用key[0])
      0x50 ^ 0x02 // 0x52 (循环使用key[1])
    ]);

    const decrypted = decryptXor(key, encrypted);
    expect(decrypted).toEqual(data);
  });

  it('XOR操作应该是可逆的', () => {
    const key = new Uint8Array(16);
    crypto.getRandomValues(key);

    const data = new TextEncoder().encode('测试XOR的可逆性');

    // 加密
    const encrypted = decryptXor(key, data);
    // 解密
    const decrypted = decryptXor(key, encrypted);

    expect(decrypted).toEqual(data);
  });
});

describe('错误类', () => {
  it('应该正确创建各种错误类型', () => {
    const baseError = new Error('原始错误');

    const invalidDataError = new InvalidDataError();
    expect(invalidDataError).toBeInstanceOf(XorError);
    expect(invalidDataError.message).toBe('Invalid data');

    const invalidTimestampError = new InvalidTimestampError();
    expect(invalidTimestampError).toBeInstanceOf(XorError);
    expect(invalidTimestampError.message).toBe('Invalid timestamp');

    const expiredError = new ExpiredError();
    expect(expiredError).toBeInstanceOf(XorError);
    expect(expiredError.message).toBe('Time-based xor decoding expired');

    const base64Error = new Base64Error(baseError);
    expect(base64Error).toBeInstanceOf(XorError);
    expect(base64Error.cause).toBe(baseError);

    const utf8Error = new Utf8Error(baseError);
    expect(utf8Error).toBeInstanceOf(XorError);
    expect(utf8Error.cause).toBe(baseError);
  });
});
