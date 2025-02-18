import { describe, it, expect } from 'vitest';
import {
  compHeadAndData,
  compareVersion,
  escapeSpecialChar,
  getFieldType,
  getTypeAndLength,
  getDbParamsByTdVersion,
  addStrBackquote,
  rmStrBackquote,
  composeType,
  processStringTagValue
} from '../tdengine';

describe('tdengine utils', () => {
  it('should correctly map head and data', () => {
    const head = [['name'], ['age']];
    const data = [
      ['Alice', '25'],
      ['Bob', '30']
    ];
    const result = compHeadAndData(head, data);
    expect(result).toEqual([
      { name: 'Alice', age: '25' },
      { name: 'Bob', age: '30' }
    ]);
  });

  it('should compare versions correctly', () => {
    expect(compareVersion('1.0.0', '>0.9.0')).toBe(true);
    expect(compareVersion('1.0.0', '<1.1.0')).toBe(true);
    expect(compareVersion('1.0.0', '=1.0.0')).toBe(true);
    expect(compareVersion('1.0.0', '>1.1.0')).toBe(false);
  });

  it('should escape special characters in SQL strings', () => {
    const str = `O'Reilly`;
    const result = escapeSpecialChar(str);
    expect(result).toBe(`O\\'Reilly`);
  });

  it('should get field type correctly', () => {
    expect(getFieldType('VARCHAR(255)')).toBe('STRING');
    expect(getFieldType('INT')).toBe('NUMBER');
    expect(getFieldType('UNKNOWN')).toBe('UNKNOWN');
  });

  it('should get type and length correctly', () => {
    expect(getTypeAndLength('VARCHAR(255)')).toEqual({ type: 'VARCHAR', length: 255 });
    expect(getTypeAndLength('INT')).toEqual({ type: 'INT', length: 0 });
  });

  it('should get DB params by TD version', () => {
    const version = '2.0.0';
    const result = getDbParamsByTdVersion(version, 'array');
    expect(result).toBeInstanceOf(Array);
  });

  it('should add backquote to string', () => {
    const name = 'column';
    const result = addStrBackquote(name);
    expect(result).toBe('`column`');
  });

  it('should remove backquote from string', () => {
    const name = '`column`';
    const result = rmStrBackquote(name);
    expect(result).toBe('column');
  });

  it('should compose type correctly', () => {
    const data = { type: 'VARCHAR', length: 255 };
    const result = composeType(data);
    expect(result).toBe('VARCHAR(255)');
  });

  it('should process string tag value correctly', () => {
    const dataType = 'VARCHAR';
    const value = 'test';
    const result = processStringTagValue(dataType, value);
    expect(result).toBe(`'test'`);
  });
});
