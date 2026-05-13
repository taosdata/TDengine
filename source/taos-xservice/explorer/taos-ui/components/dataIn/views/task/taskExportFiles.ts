export function getTaskExportFilename(ids: number[], exportBlob?: Blob) {
  const extension = exportBlob?.type?.startsWith('application/zip') ? 'zip' : 'json';
  return `datain-tasks-${ids.join()}.${extension}`;
}
