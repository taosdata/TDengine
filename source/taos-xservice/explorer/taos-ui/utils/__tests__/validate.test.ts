/* eslint-disable no-prototype-builtins */
import { describe, it, expect, vi, afterEach } from 'vitest';
import * as validate from '../validate';

describe('validate.ts', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });
  it('should validate isString', () => {
    expect(validate.isString('test')).toBe(true);
    expect(validate.isString(123)).toBe(false);
  });

  it('should validate isArray', () => {
    expect(validate.isArray([])).toBe(true);
    expect(validate.isArray('test')).toBe(false);
  });

  it('should validate isNumber', () => {
    expect(validate.isNumber(123)).toBe(true);
    expect(validate.isNumber('123')).toBe(false);
  });

  it('should validate isObject', () => {
    expect(validate.isObject({})).toBe(true);
    expect(validate.isObject(null)).toBe(false);
  });

  it('should validate isEmpty', () => {
    expect(validate.isEmpty([])).toBe(true);
    expect(validate.isEmpty({})).toBe(true);
    expect(validate.isEmpty('')).toBe(true);
    expect(validate.isEmpty([1])).toBe(false);
    expect(validate.isEmpty({ a: 1 })).toBe(false);
  });

  it('should validate isFunction', () => {
    expect(validate.isFunction(() => {})).toBe(true);
    expect(validate.isFunction(123)).toBe(false);
  });

  it('should validate isBoolean', () => {
    expect(validate.isBoolean(true)).toBe(true);
    expect(validate.isBoolean(false)).toBe(true);
    expect(validate.isBoolean('true')).toBe(false);
  });

  it('should validate isRegExp', () => {
    expect(validate.isRegExp(/test/)).toBe(true);
    expect(validate.isRegExp('test')).toBe(false);
  });

  it('should validate isPromise', () => {
    expect(validate.isPromise(Promise.resolve())).toBe(true);
    expect(validate.isPromise({})).toBe(false);
  });

  it('rejects objects that spoof Promise via Symbol.toStringTag without thenable methods', () => {
    expect(validate.isPromise({ [Symbol.toStringTag]: 'Promise' })).toBe(false);
  });

  it('should validate isIterable', () => {
    expect(validate.isIterable([])).toBe(true);
    expect(validate.isIterable({})).toBe(false);
  });

  it('should validate validUsername', () => {
    expect(validate.validUsername('admin')).toBe(true);
    expect(validate.validUsername('user')).toBe(false);
  });

  it('should validate validPhone', () => {
    expect(validate.validPhone('13800138000')).toBe(true);
    expect(validate.validPhone('123456')).toBe(false);
  });

  it('should validate validURL', () => {
    expect(validate.validURL('https://example.com')).toBe(true);
    expect(validate.validURL('invalid-url')).toBe(false);
  });

  it('should validate validLowerCase', () => {
    expect(validate.validLowerCase('abc')).toBe(true);
    expect(validate.validLowerCase('ABC')).toBe(false);
  });

  it('should validate validUpperCase', () => {
    expect(validate.validUpperCase('ABC')).toBe(true);
    expect(validate.validUpperCase('abc')).toBe(false);
  });

  it('should validate validAlphabets', () => {
    expect(validate.validAlphabets('abcABC')).toBe(true);
    expect(validate.validAlphabets('abc123')).toBe(false);
  });

  it('should validate validEmail', () => {
    expect(validate.validEmail('test@example.com')).toBe(true);
    expect(validate.validEmail('invalid-email')).toBe(false);
  });

  it('should validate validPassword', () => {
    expect(validate.validPassword('Abc123!@#')).toBe(true);
    expect(validate.validPassword('abc123')).toBe(false);
  });

  it('should validate validID (credential/token: letter + digit + special, 8-20 chars)', () => {
    expect(validate.validID('Abc123!@#')).toBe(true);
    expect(validate.validID('Abc123._~')).toBe(true);
    // pure digits — no letters, no special chars
    expect(validate.validID('123456789012345678')).toBe(false);
    // no digits
    expect(validate.validID('invalid-id')).toBe(false);
    // too short
    expect(validate.validID('Ab1!')).toBe(false);
  });

  it('should validate isNull', () => {
    expect(validate.isNull(null)).toBe(true);
    expect(validate.isNull(undefined)).toBe(false);
  });

  it('should validate isUnDef', () => {
    expect(validate.isUnDef(undefined)).toBe(true);
    expect(validate.isUnDef(null)).toBe(false);
  });

  it('should validate isNullOrUnDef (returns true for null OR undefined)', () => {
    expect(validate.isNullOrUnDef(null)).toBe(true);
    expect(validate.isNullOrUnDef(undefined)).toBe(true);
    expect(validate.isNullOrUnDef(0)).toBe(false);
    expect(validate.isNullOrUnDef('')).toBe(false);
  });

  it('isNullAndUnDef is a backward-compatible alias for isNullOrUnDef', () => {
    expect(validate.isNullAndUnDef).toBe(validate.isNullOrUnDef);
  });

  it('should validate validTDengineImageVersion', () => {
    expect(validate.validTDengineImageVersion('2.0.0.0')).toBe(true);
    expect(validate.validTDengineImageVersion('2.0')).toBe(false);
  });

  it('should validate isWindows', () => {
    vi.stubGlobal('navigator', { platform: 'Win32' });
    expect(validate.isWindows()).toBe(true);

    vi.stubGlobal('navigator', { platform: 'MacIntel' });
    expect(validate.isWindows()).toBe(false);
  });

  it('should validate validName', () => {
    expect(validate.validName('testName')).toBe(true);
    expect(validate.validName('123Name')).toBe(false);
  });

  it('should validate validBankAccount', () => {
    expect(validate.validBankAccount('123456789012')).toBe(true);
    expect(validate.validBankAccount('123')).toBe(false);
    // ^ anchor: prefix garbage must not be accepted
    expect(validate.validBankAccount('garbage123456789012')).toBe(false);
  });

  it('should validate isBefore', () => {
    expect(validate.isBefore(new Date('2023-01-01'), new Date('2023-01-02'))).toBe(true);
    expect(validate.isBefore(new Date('2023-01-02'), new Date('2023-01-01'))).toBe(false);
  });

  it('should validate isAfter', () => {
    expect(validate.isAfter(new Date('2023-01-02'), new Date('2023-01-01'))).toBe(true);
    expect(validate.isAfter(new Date('2023-01-01'), new Date('2023-01-02'))).toBe(false);
  });

  it('should validate isEqual', () => {
    expect(validate.isEqual(new Date('2023-01-01'), new Date('2023-01-01'))).toBe(true);
    expect(validate.isEqual(new Date('2023-01-01'), new Date('2023-01-02'))).toBe(false);
  });

  it('should validate isSameOrBefore', () => {
    expect(validate.isSameOrBefore(new Date('2023-01-01'), new Date('2023-01-01'))).toBe(true);
    expect(validate.isSameOrBefore(new Date('2023-01-01'), new Date('2023-01-02'))).toBe(true);
    expect(validate.isSameOrBefore(new Date('2023-01-02'), new Date('2023-01-01'))).toBe(false);
  });

  it('should validate isSameOrAfter', () => {
    expect(validate.isSameOrAfter(new Date('2023-01-01'), new Date('2023-01-01'))).toBe(true);
    expect(validate.isSameOrAfter(new Date('2023-01-02'), new Date('2023-01-01'))).toBe(true);
    expect(validate.isSameOrAfter(new Date('2023-01-01'), new Date('2023-01-02'))).toBe(false);
  });

  it('should validate isBetween', () => {
    expect(validate.isBetween(new Date('2023-01-02'), new Date('2023-01-01'), new Date('2023-01-03'))).toBe(true);
    expect(validate.isBetween(new Date('2023-01-01'), new Date('2023-01-02'), new Date('2023-01-03'))).toBe(false);
  });

  it('should validate validInvoiceNumber', () => {
    expect(validate.validInvoiceNumber('123456789012345678')).toBe(true);
    expect(validate.validInvoiceNumber('invalid-invoice')).toBe(false);
  });

  it('should validate validDsn', () => {
    expect(validate.validDsn('taos+ws://example.com')).toBe(true);
    expect(validate.validDsn('invalid-dsn')).toBe(false);
  });

  it('should validate isIPUrl', () => {
    expect(validate.isIPUrl('http://192.168.0.1')).toBe(true);
    expect(validate.isIPUrl('invalid-url')).toBe(false);
  });

  it('should validate isIP', () => {
    expect(validate.isIP('192.168.0.1')).toBe(true);
    expect(validate.isIP('invalid-ip')).toBe(false);
  });

  it('should validate isIPV4', () => {
    expect(validate.isIPV4('192.168.0.1')).toBe(true);
    expect(validate.isIPV4('invalid-ip')).toBe(false);
    // ^ anchor: prefix garbage must not be accepted
    expect(validate.isIPV4('garbage192.168.0.1')).toBe(false);
    expect(validate.isIPV4('prefix:192.168.0.1')).toBe(false);
  });

  it('should validate isIPV6', () => {
    expect(validate.isIPV6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')).toBe(true);
    expect(validate.isIPV6('invalid-ip')).toBe(false);
  });

  it('should validate hasOwnProperty', () => {
    const obj = { a: 1 };
    expect(validate.hasOwnProperty(obj, 'a')).toBe(true);
    expect(validate.hasOwnProperty(obj, 'b')).toBe(false);
  });

  it('should validate validDbDuration', () => {
    expect(validate.validDbDuration('100h')).toBe(true);
    expect(validate.validDbDuration('invalid-duration')).toBe(false);
  });

  it('should validate validDbKeep', () => {
    expect(validate.validDbKeep('100h,100d,3650d')).toBe(true);
    expect(validate.validDbKeep('invalid-keep')).toBe(false);
  });

  it('should validate validTDKeywords', () => {
    expect(validate.validTDKeywords('SELECT')).toBe(true);
    expect(validate.validTDKeywords('invalid-keyword')).toBe(false);
  });

  it('should validate validTableName', () => {
    expect(validate.validTableName('table_name')).toBe(true);
    expect(validate.validTableName('123table')).toBe(false);
  });

  it('should validate validSqlIsSelect', () => {
    expect(validate.validSqlIsSelect('SELECT * FROM table')).toBe(true);
    expect(validate.validSqlIsSelect('INSERT INTO table')).toBe(false);
  });

  it('should validate validHost', () => {
    expect(validate.validHost('example.com')).toBe(true);
    expect(validate.validHost('host with spaces')).toBe(false);
  });
});
