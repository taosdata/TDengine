import { describe, it, expect, beforeAll } from 'vitest';
import aesGcm from './aesGcm';
import type { AesGcmEncryptedData, AesGcmEncryptedB64Data } from './aesGcm';

describe('AesGcmCrypto', () => {
	let testKey: CryptoKey;
	const testPlaintext = 'Hello, AES-256-GCM with Base64!';
	const testObject = { user: 'admin', password: 'secret123', roles: ['admin', 'user'] };

	beforeAll(async () => {
		// 检查浏览器支持
		if (!aesGcm.isSupported()) {
			throw new Error('Web Crypto API is not supported');
		}
		// 生成测试密钥
		// testKey = await aesGcm.generateKey();
		testKey = await aesGcm.importKey('Ioc0q7sVElGXOaBFDPEHrjgLZeIFbm55Ol5HOTiNqg8=');
	});

	describe('密钥管理', () => {
		it('应该成功生成密钥', async () => {
			const key = await aesGcm.generateKey();
			expect(key).toBeDefined();
			expect(key.type).toBe('secret');
			expect(key.algorithm.name).toBe('AES-GCM');
		});

		it('应该成功导出和导入密钥', async () => {
			const originalKey = await aesGcm.generateKey();
			const exportedKey = await aesGcm.exportKey(originalKey);

			expect(exportedKey).toBeDefined();
			expect(typeof exportedKey).toBe('string');
			expect(exportedKey.length).toBeGreaterThan(0);

			const importedKey = await aesGcm.importKey(exportedKey);
			expect(importedKey).toBeDefined();
			expect(importedKey.type).toBe('secret');

			// 验证密钥可用性
			const testData = 'test encryption';
			const encrypted = await aesGcm.encrypt(testData, originalKey);
			const decrypted = await aesGcm.decrypt(encrypted, importedKey);
			expect(decrypted).toBe(testData);
		});

		it('导入无效密钥应该失败', async () => {
			await expect(aesGcm.importKey('invalid_base64')).rejects.toThrow();
		});
	});

	describe('文本加密解密', () => {
		it('应该成功加密和解密文本', async () => {
			const encrypted = await aesGcm.encrypt(testPlaintext, testKey);

			expect(encrypted).toBeDefined();
			expect(encrypted.data).toBeInstanceOf(ArrayBuffer);
			expect(encrypted.iv).toBeInstanceOf(Uint8Array);
			expect(encrypted.iv.length).toBe(12);
			expect(encrypted.algorithm).toBe('AES-GCM');
			expect(encrypted.version).toBe('1.0.0');

			const decrypted = await aesGcm.decrypt(encrypted, testKey);
			expect(decrypted).toBe(testPlaintext);
		});

		it('应该支持自定义 IV', async () => {
			const customIv = aesGcm.generateRandomBytes(12);
			const encrypted = await aesGcm.encrypt(testPlaintext, testKey, customIv);

			expect(encrypted.iv).toEqual(customIv);

			const decrypted = await aesGcm.decrypt(encrypted, testKey);
			expect(decrypted).toBe(testPlaintext);
		});

		it('使用错误的密钥解密应该失败', async () => {
			const encrypted = await aesGcm.encrypt(testPlaintext, testKey);
			const wrongKey = await aesGcm.generateKey();

			await expect(aesGcm.decrypt(encrypted, wrongKey)).rejects.toThrow();
		});

		it('篡改加密数据应该导致解密失败', async () => {
			const encrypted = await aesGcm.encrypt(testPlaintext, testKey);

			// 篡改数据
			const tamperedData = new Uint8Array(encrypted.data);
			tamperedData[0] ^= 0xFF; // 翻转第一个字节
			const tamperedEncrypted: AesGcmEncryptedData = {
				...encrypted,
				data: tamperedData.buffer
			};

			await expect(aesGcm.decrypt(tamperedEncrypted, testKey)).rejects.toThrow();
		});

		it('应该处理空字符串', async () => {
			const encrypted = await aesGcm.encrypt('', testKey);
			const decrypted = await aesGcm.decrypt(encrypted, testKey);
			expect(decrypted).toBe('');
		});

		it('应该处理长文本', async () => {
			const longText = 'A'.repeat(10000);
			const encrypted = await aesGcm.encrypt(longText, testKey);
			const decrypted = await aesGcm.decrypt(encrypted, testKey);
			expect(decrypted).toBe(longText);
		});

		it('应该处理特殊字符和表情符号', async () => {
			const specialText = '特殊字符: !@#$%^&*()_+-=[]{}|;:\'",.<>?/~`\n\t\r 😀🎉🔐';
			const encrypted = await aesGcm.encrypt(specialText, testKey);
			const decrypted = await aesGcm.decrypt(encrypted, testKey);
			expect(decrypted).toBe(specialText);
		});
	});

	describe('Base64 加密解密', () => {
		it('应该成功进行 Base64 加密和解密', async () => {
			const encrypted = await aesGcm.encryptB64(testPlaintext, testKey);
			console.log('Encrypted Base64 Data:', encrypted.data);

			expect(encrypted).toBeDefined();
			expect(typeof encrypted.data).toBe('string');
			expect(encrypted.data.length).toBeGreaterThan(0);
			expect(encrypted.algorithm).toBe('AES-GCM');

			const decrypted = await aesGcm.decryptB64(encrypted, testKey);
			expect(decrypted).toBe(testPlaintext);
		});

		it('Base64 数据应该包含 IV 和加密数据', async () => {
			const encrypted = await aesGcm.encryptB64(testPlaintext, testKey);

			// Base64 解码检查长度 (IV 12 bytes + 加密数据 + 认证标签 16 bytes)
			const decoded = atob(encrypted.data);
			expect(decoded.length).toBeGreaterThanOrEqual(12 + 16);
		});

		it('使用自定义 IV 进行 Base64 加密', async () => {
			const customIv = aesGcm.generateRandomBytes(12);
			const encrypted = await aesGcm.encryptB64(testPlaintext, testKey, customIv);
			const decrypted = await aesGcm.decryptB64(encrypted, testKey);
			expect(decrypted).toBe(testPlaintext);
		});

		it('Base64 格式错误应该抛出异常', async () => {
			const invalidData: AesGcmEncryptedB64Data = {
				data: 'invalid!!!base64',
				algorithm: 'AES-GCM',
				version: '1.0.0'
			};

			await expect(aesGcm.decryptB64(invalidData, testKey)).rejects.toThrow();
		});
	});

	describe('对象加密解密', () => {
		it('应该成功加密和解密对象', async () => {
			const encrypted = await aesGcm.encryptObject(testObject, testKey);
			const decrypted = await aesGcm.decryptObject<typeof testObject>(encrypted, testKey);

			expect(decrypted).toEqual(testObject);
		});

		it('应该处理嵌套对象', async () => {
			const nestedObject = {
				user: {
					name: 'admin',
					profile: {
						age: 30,
						roles: ['admin', 'user']
					}
				},
				settings: {
					theme: 'dark',
					notifications: true
				}
			};

			const encrypted = await aesGcm.encryptObject(nestedObject, testKey);
			const decrypted = await aesGcm.decryptObject<typeof nestedObject>(encrypted, testKey);

			expect(decrypted).toEqual(nestedObject);
		});

		it('应该处理包含特殊值的对象', async () => {
			const specialObject = {
				nullValue: null,
				undefinedValue: undefined,
				numberValue: 42,
				boolValue: true,
				arrayValue: [1, 2, 3],
				dateString: new Date().toISOString()
			};

			const encrypted = await aesGcm.encryptObject(specialObject, testKey);
			const decrypted = await aesGcm.decryptObject<typeof specialObject>(encrypted, testKey);

			// undefined 会在 JSON.stringify 时被移除
			expect(decrypted.nullValue).toBe(null);
			expect(decrypted.numberValue).toBe(42);
			expect(decrypted.boolValue).toBe(true);
			expect(decrypted.arrayValue).toEqual([1, 2, 3]);
		});
	});

	describe('随机数生成', () => {
		it('应该生成指定长度的随机字节', () => {
			const bytes = aesGcm.generateRandomBytes(16);
			expect(bytes).toBeInstanceOf(Uint8Array);
			expect(bytes.length).toBe(16);
		});

		it('每次生成的随机字节应该不同', () => {
			const bytes1 = aesGcm.generateRandomBytes(16);
			const bytes2 = aesGcm.generateRandomBytes(16);

			expect(bytes1).not.toEqual(bytes2);
		});

		it('应该生成随机盐值', () => {
			const salt = aesGcm.generateSalt();
			expect(salt).toBeInstanceOf(Uint8Array);
			expect(salt.length).toBe(16); // 默认长度

			const customSalt = aesGcm.generateSalt(32);
			expect(customSalt.length).toBe(32);
		});
	});

	describe('错误处理', () => {
		it('不支持的算法应该抛出错误', async () => {
			const encrypted = await aesGcm.encrypt(testPlaintext, testKey);
			const invalidEncrypted: AesGcmEncryptedData = {
				...encrypted,
				algorithm: 'INVALID-ALGORITHM'
			};

			await expect(aesGcm.decrypt(invalidEncrypted, testKey)).rejects.toThrow('不支持的加密算法');
		});

		it('版本不匹配应该显示警告但继续解密', async () => {
			const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => { });

			const encrypted = await aesGcm.encrypt(testPlaintext, testKey);
			const oldVersionEncrypted: AesGcmEncryptedData = {
				...encrypted,
				version: '0.9.0'
			};

			const decrypted = await aesGcm.decrypt(oldVersionEncrypted, testKey);
			expect(decrypted).toBe(testPlaintext);
			expect(consoleWarnSpy).toHaveBeenCalled();

			consoleWarnSpy.mockRestore();
		});
	});

	describe('浏览器兼容性', () => {
		it('应该检测 Web Crypto API 支持', () => {
			const isSupported = aesGcm.isSupported();
			expect(typeof isSupported).toBe('boolean');
			// 在测试环境中应该支持
			expect(isSupported).toBe(true);
		});
	});

	describe('性能测试', () => {
		it('应该能够快速加密大量数据', async () => {
			const largeData = 'x'.repeat(1024 * 1024); // 1MB
			const startTime = performance.now();

			const encrypted = await aesGcm.encrypt(largeData, testKey);
			const decrypted = await aesGcm.decrypt(encrypted, testKey);

			const endTime = performance.now();
			const duration = endTime - startTime;

			expect(decrypted).toBe(largeData);
			expect(duration).toBeLessThan(1000); // 应该在 1 秒内完成
		});

		it('应该能够处理多次连续加密', async () => {
			const iterations = 100;
			const results: string[] = [];

			for (let i = 0; i < iterations; i++) {
				const data = `test-${i}`;
				const encrypted = await aesGcm.encrypt(data, testKey);
				const decrypted = await aesGcm.decrypt(encrypted, testKey);
				results.push(decrypted);
			}

			expect(results.length).toBe(iterations);
			results.forEach((result, index) => {
				expect(result).toBe(`test-${index}`);
			});
		});
	});
});
