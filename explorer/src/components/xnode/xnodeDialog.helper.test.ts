import { describe, expect, it } from 'vitest';
import {
  buildDropXnodeSql,
  buildCreateXnodeSql,
  hasAnyXnode,
  normalizeXnodeRows,
  validateXnodeForm
} from './xnodeDialog.helper';

describe('xnodeDialog.helper', () => {
  it('builds create xnode sql without credentials', () => {
    expect(buildCreateXnodeSql({ endpoint: 'x1:6050', user: '', pass: '' })).toBe("create xnode 'x1:6050';");
  });

  it('builds create xnode sql with credentials', () => {
    expect(buildCreateXnodeSql({ endpoint: 'x1:6050', user: '__xnode__', pass: 'Ab123456' })).toBe(
      "create xnode 'x1:6050' user __xnode__ pass 'Ab123456';"
    );
  });

  it('builds create xnode sql with token auth', () => {
    expect(buildCreateXnodeSql({ endpoint: 'x1:6050', user: '', pass: '', token: 'token-123' })).toBe(
      "create xnode 'x1:6050' token 'token-123';"
    );
  });

  it('escapes single quotes in endpoint and password', () => {
    expect(buildCreateXnodeSql({ endpoint: "x'1:6050", user: '__xnode__', pass: "Ab'123456" })).toBe(
      "create xnode 'x''1:6050' user __xnode__ pass 'Ab''123456';"
    );
  });

  it('escapes backslashes in endpoint and password', () => {
    expect(buildCreateXnodeSql({ endpoint: 'x\\1:6050', user: '__xnode__', pass: 'Ab\\123456' })).toBe(
      "create xnode 'x\\\\1:6050' user __xnode__ pass 'Ab\\\\123456';"
    );
  });

  it('builds drop xnode sql only for integer ids', () => {
    expect(buildDropXnodeSql(1)).toBe('drop xnode 1;');
    expect(buildDropXnodeSql('1')).toBe('drop xnode 1;');
    expect(() => buildDropXnodeSql('x1')).toThrow('invalid xnode id');
    expect(() => buildDropXnodeSql(0)).toThrow('invalid xnode id');
    expect(() => buildDropXnodeSql(-1)).toThrow('invalid xnode id');
  });

  it('requires user and password together', () => {
    expect(validateXnodeForm({ endpoint: 'x1:6050', user: '__xnode__', pass: '', token: '' })).toBe('credentials');
  });

  it('rejects usernames that are not safe SQL identifiers', () => {
    expect(validateXnodeForm({ endpoint: 'x1:6050', user: "bad'user", pass: 'Ab123456', token: '' })).toBe('user');
  });

  it('rejects mixing token auth with user/password auth', () => {
    expect(validateXnodeForm({ endpoint: 'x1:6050', user: '__xnode__', pass: 'Ab123456', token: 'token-123' })).toBe(
      'authMode'
    );
  });

  it('checks xnode existence from show xnodes rows without reading status', () => {
    expect(
      hasAnyXnode(
        normalizeXnodeRows({
          column_meta: [
            ['id', 'INT'],
            ['endpoint', 'VARCHAR'],
            ['status', 'VARCHAR']
          ],
          data: [[1, 'x1:6050', 'offline']]
        })
      )
    ).toBe(true);
    expect(hasAnyXnode([])).toBe(false);
  });

  it('maps SHOW XNODES url rows to the endpoint field used by the cluster table', () => {
    expect(
      normalizeXnodeRows({
        column_meta: [
          ['id', 'INT'],
          ['url', 'VARCHAR'],
          ['status', 'VARCHAR'],
          ['create_time', 'TIMESTAMP']
        ],
        data: [[1, 'h1:6055', 'online', '2026-04-15T12:20:55.626+08:00']]
      })
    ).toEqual([
      {
        id: 1,
        url: 'h1:6055',
        endpoint: 'h1:6055',
        status: 'online',
        create_time: '2026-04-15T12:20:55.626+08:00'
      }
    ]);
  });
});
