import { describe, expect, it } from 'vitest';
import taskImportSource from './task-import.vue?raw';

describe('task-import upload markup', () => {
  it('handles import files locally instead of posting them through el-upload', () => {
    expect(taskImportSource).not.toContain(':action="dataInProps.uploadFileUrl"');
    expect(taskImportSource).not.toContain(':headers="uploadHeaders"');
    expect(taskImportSource).not.toContain(':with-credentials="true"');
    expect(taskImportSource).toContain('await handleJsonImport(file);');
    expect(taskImportSource).toContain('return false;');
  });

  it('uses i18n keys for zip import errors', () => {
    expect(taskImportSource).toContain("t('dataIn.invalidZipFile'");
    expect(taskImportSource).toContain("t('dataIn.failedToUpload'");
    expect(taskImportSource).toContain("t('dataIn.zipImportUploadFailed'");
  });
});
