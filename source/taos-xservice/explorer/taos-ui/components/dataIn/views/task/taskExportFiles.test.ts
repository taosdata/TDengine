import { describe, expect, it } from 'vitest';

import { getTaskExportFilename } from './taskExportFiles';

describe('getTaskExportFilename', () => {
  it('returns a zip filename when the export blob is a zip archive', () => {
    const blob = new Blob(['zip-data'], { type: 'application/zip' });

    expect(getTaskExportFilename([13], blob)).toBe('datain-tasks-13.zip');
  });

  it('treats zip content types with parameters as zip exports', () => {
    const blob = new Blob(['zip-data'], { type: 'application/zip; charset=binary' });

    expect(getTaskExportFilename([13], blob)).toBe('datain-tasks-13.zip');
  });

  it('keeps the legacy json filename when the export blob is json', () => {
    const blob = new Blob(['{}'], { type: 'application/json' });

    expect(getTaskExportFilename([1, 2], blob)).toBe('datain-tasks-1,2.json');
  });
});
