export function rewriteBundledReferencesInValue<T>(value: T, pathMap: Record<string, string>): T {
  if (typeof value === 'string') {
    return value
      .split(',')
      .map(part => {
        const trimmed = part.trim();
        return (pathMap[trimmed] ?? part) as string;
      })
      .join(',') as T;
  }

  if (Array.isArray(value)) {
    return value.map(item => rewriteBundledReferencesInValue(item, pathMap)) as T;
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nestedValue]) => [
        key,
        rewriteBundledReferencesInValue(nestedValue, pathMap)
      ])
    ) as T;
  }

  return value;
}

export function bundledZipFileEntries(zipFiles: Record<string, Uint8Array>): Array<[string, Uint8Array]> {
  return Object.entries(zipFiles).filter(([zipPath]) => zipPath.startsWith('files/') && !zipPath.endsWith('/'));
}

export function bundledZipUploadFileName(zipPath: string): string {
  const parts = zipPath.split('/');
  if (parts.length < 3 || parts[0] !== 'files') {
    throw new Error(`invalid bundled ZIP entry path: ${zipPath}`);
  }

  const nestedParts = parts.slice(2, -1);
  const fileName = parts.at(-1) ?? '';
  const hasInvalidSegment = [...nestedParts, fileName].some(
    part => !part || part === '.' || part === '..' || part.includes('/') || part.includes('\\')
  );

  if (hasInvalidSegment) {
    throw new Error(`invalid bundled ZIP entry path: ${zipPath}`);
  }

  return fileName;
}

export function singleUploadedPath(uploadedPaths: string[], fileName: string): string {
  if (uploadedPaths.length !== 1) {
    throw new Error(`expected exactly one uploaded path for ${fileName}, got ${uploadedPaths.length}`);
  }
  return uploadedPaths[0];
}
