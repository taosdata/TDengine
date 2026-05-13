export class XorError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'XorError';
  }
}

export class InvalidDataError extends XorError {
  constructor() {
    super('Invalid data');
  }
}

export class InvalidTimestampError extends XorError {
  constructor() {
    super('Invalid timestamp');
  }
}

export class ExpiredError extends XorError {
  constructor() {
    super('Time-based xor decoding expired');
  }
}

export class Base64Error extends XorError {
  constructor(cause: Error) {
    super(`Base64 decoding data error: ${cause.message}`, cause);
  }
}

export class Utf8Error extends XorError {
  constructor(cause: Error) {
    super(`Xor decoding caused UTF-8 error: ${cause.message}`, cause);
  }
}

const KEY_CHARS = new Uint8Array([
  116, // 't'
  97, // 'a'
  111, // 'o'
  115, // 's'
  100, // 'd'
  97, // 'a'
  116, // 't'
  97, // 'a'
  101, // 'e'
  120, // 'x'
  112, // 'p'
  108, // 'l'
  111, // 'o'
  114, // 'r'
  101, // 'e'
  114 // 'r'
]);
const KEY = new Uint8Array(KEY_CHARS);

export class TimeBasedXor {
  constructor(private readonly allowedDurationInSeconds: number) {}

  /**
   * 解密数据
   * @param data 格式为 "timestamp.base64_encoded_data"
   * @returns 解密后的字符串
   */
  public decrypt(data: string): string {
    if (this.allowedDurationInSeconds === 0) {
      throw new ExpiredError();
    }

    const [timestampStr, encryptedData] = data.split('.');

    if (!timestampStr || !encryptedData) {
      throw new InvalidDataError();
    }

    const timestamp = parseInt(timestampStr, 10);
    if (isNaN(timestamp)) {
      throw new InvalidTimestampError();
    }

    const currentTime = Math.floor(Date.now() / 1000);
    if (currentTime - timestamp > this.allowedDurationInSeconds) {
      throw new ExpiredError();
    }

    let bytes: Uint8Array;
    try {
      bytes = Buffer.from(encryptedData, 'base64');
    } catch (error) {
      throw new Base64Error(error as Error);
    }
    if (bytes.length === 0) {
      throw new Base64Error(new Error('Invalid Base64 data'));
    }

    const key = TimeBasedXor.genKey(timestamp);
    const decrypted = this.decryptXor(key, bytes);

    try {
      return Buffer.from(decrypted).toString('utf8');
    } catch (error) {
      throw new Utf8Error(error as Error);
    }
  }

  /**
   * 仅用于测试的加密方法
   * @param data 要加密的数据
   * @returns 格式为 "timestamp.base64_encoded_data"
   */
  public encrypt(data: string | Uint8Array | ArrayBuffer): string {
    if (!data) {
      throw new InvalidDataError();
    }
    const timestamp = Math.floor(Date.now() / 1000);
    const key = TimeBasedXor.genKey(timestamp);

    const dataBytes =
      data instanceof Uint8Array ? data : data instanceof ArrayBuffer ? new Uint8Array(data) : Buffer.from(data);

    const encrypted = this.encryptXor(key, dataBytes);
    const encryptedData = Buffer.from(encrypted).toString('base64');

    return `${timestamp}.${encryptedData}`;
  }

  /**
   * 生成加密密钥
   * 密钥结构: KEY(16字节) + timestamp(8字节，大端序)
   */
  private static genKey(timestamp: number): Uint8Array {
    const key = new Uint8Array(24);

    // 复制16字节的固定密钥
    key.set(KEY, 0);

    // 将timestamp转换为8字节大端序
    const timestampBuffer = new ArrayBuffer(8);
    const timestampView = new DataView(timestampBuffer);
    // 使用Math.trunc确保是整数
    const truncatedTimestamp = Math.trunc(timestamp);
    timestampView.setBigUint64(0, BigInt(truncatedTimestamp), false); // false表示大端序

    // 复制timestamp到密钥的后8字节
    key.set(new Uint8Array(timestampBuffer), 16);

    return key;
  }

  /**
   * XOR解密
   */
  private decryptXor(key: Uint8Array, data: Uint8Array): Uint8Array {
    return this.encryptXor(key, data); // XOR解密和加密是相同的操作
  }

  /**
   * XOR加密
   */
  private encryptXor(key: Uint8Array, data: Uint8Array): Uint8Array {
    const result = new Uint8Array(data.length);
    let keyIndex = 0;

    for (let i = 0; i < data.length; i++) {
      result[i] = data[i] ^ key[keyIndex];
      keyIndex = (keyIndex + 1) % key.length;
    }

    return result;
  }
}

/**
 * 辅助函数：XOR解密
 */
export function decryptXor(key: Uint8Array, data: Uint8Array): Uint8Array {
  const result = new Uint8Array(data.length);
  let keyIndex = 0;

  for (let i = 0; i < data.length; i++) {
    result[i] = data[i] ^ key[keyIndex];
    keyIndex = (keyIndex + 1) % key.length;
  }

  return result;
}

/**
 * 创建TimeBasedXor实例的工厂函数
 */
export function createTimeBasedXor(allowedDurationInSeconds: number): TimeBasedXor {
  return new TimeBasedXor(allowedDurationInSeconds);
}
