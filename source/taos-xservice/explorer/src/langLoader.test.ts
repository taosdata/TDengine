import { describe, expect, it } from 'vitest';
import source from './lang/index.ts?raw';

describe('lang/index.ts', () => {
  it('excludes test modules at glob time when eager-loading locale files', () => {
    expect(source).toContain("['./**/*.ts', '!./**/*.test.ts', '!./**/*.spec.ts']");
  });
});
