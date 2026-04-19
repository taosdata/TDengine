import { describe, expect, it } from 'vitest';
import uploadCsvSource from './uploadCsv.vue?raw';
import csvTransformerSource from './csv/csvTransformer.vue?raw';
import commonTransformerSource from './commonTransformer/index.vue?raw';

describe('dataIn upload components', () => {
  it('sends credentials for uploadCsv.vue uploads', () => {
    expect(uploadCsvSource).toContain(':action="dataInProps.uploadFileUrl"');
    expect(uploadCsvSource).toContain(':with-credentials="true"');
  });

  it('sends credentials for csvTransformer.vue uploads', () => {
    expect(csvTransformerSource).toContain(':action="dataInProps.uploadFileUrl"');
    expect(csvTransformerSource).toContain(':with-credentials="true"');
  });

  it('sends credentials for commonTransformer uploads', () => {
    expect(commonTransformerSource.match(/:action="dataInProps\.uploadFileUrl"/g)?.length).toBe(2);
    expect(commonTransformerSource.match(/:with-credentials="true"/g)?.length).toBe(2);
  });
});
