import { describe, expect, it } from 'vitest';
import {
  bundledZipFileEntries,
  bundledZipUploadFileName,
  rewriteBundledReferencesInValue,
  singleUploadedPath
} from './taskImportFiles';

describe('rewriteBundledReferencesInValue', () => {
  it('rewrites only exact bundled file references in nested task data', () => {
    const input = {
      tasks: [
        {
          name: 'keep @files/req-1/config.csv inside the task name',
          from: {
            tls_cert_file: '@files/req-1/config.csv',
            csv_config_file: '@files/req-1/config.csv,@files/req-2/more.csv',
            nested: ['@files/req-2/more.csv', 'plain text']
          }
        }
      ]
    };

    const rewritten = rewriteBundledReferencesInValue(input, {
      '@files/req-1/config.csv': '@/tmp/upload/config.csv',
      '@files/req-2/more.csv': '@/tmp/upload/more.csv'
    });

    expect(rewritten).toEqual({
      tasks: [
        {
          name: 'keep @files/req-1/config.csv inside the task name',
          from: {
            tls_cert_file: '@/tmp/upload/config.csv',
            csv_config_file: '@/tmp/upload/config.csv,@/tmp/upload/more.csv',
            nested: ['@/tmp/upload/more.csv', 'plain text']
          }
        }
      ]
    });
  });
});

describe('bundledZipUploadFileName', () => {
  it('uses the last path segment as the upload filename', () => {
    expect(bundledZipUploadFileName('files/req-1/nested/config.csv')).toBe('config.csv');
  });

  it('rejects traversal segments in bundled zip paths', () => {
    expect(() => bundledZipUploadFileName('files/req-1/../evil.txt')).toThrow(
      'invalid bundled ZIP entry path'
    );
  });
});

describe('bundledZipFileEntries', () => {
  it('skips directory entries under files/', () => {
    const entries = bundledZipFileEntries({
      'tasks.json': new Uint8Array([1]),
      'files/req-1/': new Uint8Array(),
      'files/req-1/config.csv': new Uint8Array([1, 2, 3])
    });

    expect(entries).toEqual([['files/req-1/config.csv', new Uint8Array([1, 2, 3])]]);
  });
});

describe('singleUploadedPath', () => {
  it('returns the only uploaded path', () => {
    expect(singleUploadedPath(['/tmp/upload/config.csv'], 'config.csv')).toBe('/tmp/upload/config.csv');
  });

  it('rejects empty upload responses', () => {
    expect(() => singleUploadedPath([], 'config.csv')).toThrow(
      'expected exactly one uploaded path for config.csv, got 0'
    );
  });

  it('rejects multiple uploaded paths', () => {
    expect(() => singleUploadedPath(['/tmp/a.csv', '/tmp/b.csv'], 'config.csv')).toThrow(
      'expected exactly one uploaded path for config.csv, got 2'
    );
  });
});
