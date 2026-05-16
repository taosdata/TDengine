import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  loadJS,
  loadCss,
  loadImage,
  base64ToBlob,
  urlToBase64,
  blobToFile,
  base64ToFile,
  downloadByData,
  downloadByBase64,
  downloadByUrl,
  blobToText,
  exportCsv,
  UTF8_BOM,
  withCsvUtf8Bom
} from '../files';

const originalImage = globalThis.Image;
class MockImage {
  width = 64;
  height = 32;
  crossOrigin = '';
  onload: null | (() => void) = null;
  onerror: null | (() => void) = null;
  private currentSrc = '';

  set src(value: string) {
    this.currentSrc = value;
    queueMicrotask(() => this.onload?.());
  }

  get src() {
    return this.currentSrc;
  }
}

function blobToBytes(blob: Blob): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
    reader.onerror = reject;
    reader.readAsArrayBuffer(blob);
  });
}

describe('files.ts', () => {
  beforeEach(() => {
    vi.stubGlobal('Image', MockImage as unknown as typeof Image);
    vi.mocked(URL.createObjectURL).mockClear();
    vi.mocked(URL.revokeObjectURL).mockClear();
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue({
      drawImage: vi.fn()
    } as unknown as CanvasRenderingContext2D);
    vi.spyOn(HTMLCanvasElement.prototype, 'toDataURL').mockReturnValue('data:image/png;base64,mock');
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.stubGlobal('Image', originalImage);
  });

  it('should load a JS file', async () => {
    const jsFile = '../../config/uno.js';
    const appendChild = vi.spyOn(document.head, 'appendChild').mockImplementation((node: Node) => {
      queueMicrotask(() => (node as HTMLScriptElement).onload?.(new Event('load')));
      return node;
    });
    const script = await loadJS(jsFile);
    expect(script).toBeInstanceOf(HTMLScriptElement);
    expect(script?.src).toContain('/config/uno.js');
    expect(appendChild).toHaveBeenCalled();
  });

  it('should load a CSS file', async () => {
    const cssFile = 'https://www.taosdata.com/wp-content/uploads/master-slider/custom.css?ver=5.4';
    const appendChild = vi.spyOn(document.head, 'appendChild').mockImplementation((node: Node) => {
      queueMicrotask(() => (node as HTMLLinkElement).onload?.(new Event('load')));
      return node;
    });
    const link = await loadCss(cssFile);
    expect(link).toBeInstanceOf(HTMLLinkElement);
    expect(link.href).toContain('custom.css');
    expect(appendChild).toHaveBeenCalled();
  });

  it('should load an image', async () => {
    vi.stubGlobal(
      'Image',
      class extends MockImage {
        width = 100;
        height = 100;
      }
    );

    const img = await loadImage('/fake.png');
    expect(img).toHaveProperty('width');
    expect(img).toHaveProperty('height');
  });

  it('should convert base64 to Blob', () => {
    const base64 = 'data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==';
    const blob = base64ToBlob(base64);
    expect(blob).toBeInstanceOf(Blob);
  });

  // urlToBase64 uses a canvas (stubbed in vitest.setup.ts) and an Image that we trigger synchronously
  it('should convert URL to base64', async () => {
    vi.stubGlobal(
      'Image',
      class extends MockImage {
        width = 10;
        height = 10;
      }
    );

    await expect(urlToBase64('/fake.png')).resolves.toContain('data:image/png;base64,');
  });

  it('should convert Blob to File', async () => {
    const blob = new Blob(['Hello, World!'], { type: 'text/plain' });
    const file = await blobToFile([blob], 'test.txt', 'text/plain');
    expect(file).toBeInstanceOf(File);
    expect(file.name).toBe('test.txt');
  });

  it('should convert base64 to File', () => {
    const base64 = 'data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==';
    const file = base64ToFile(base64, 'test.txt');
    expect(file).toBeInstanceOf(File);
    expect(file.name).toBe('test.txt');
  });

  it('should download data as Blob', () => {
    const data = 'Hello, World!';
    const filename = 'test.txt';
    const createObjectURLMock = vi.mocked(URL.createObjectURL);
    const revokeObjectURLMock = vi.mocked(URL.revokeObjectURL);
    const appendChildMock = vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => node);
    const removeChildMock = vi.spyOn(document.body, 'removeChild').mockImplementation((node: Node) => node);
    downloadByData(data, filename);
    expect(createObjectURLMock).toHaveBeenCalled();
    expect(revokeObjectURLMock).toHaveBeenCalled();
    expect(appendChildMock).toHaveBeenCalled();
    expect(removeChildMock).toHaveBeenCalled();
  });

  it('should download data as base64', () => {
    const base64 = 'data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==';
    const filename = 'test.txt';
    const createObjectURLMock = vi.mocked(URL.createObjectURL);
    const revokeObjectURLMock = vi.mocked(URL.revokeObjectURL);
    const appendChildMock = vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => node);
    const removeChildMock = vi.spyOn(document.body, 'removeChild').mockImplementation((node: Node) => node);
    downloadByBase64(base64, filename);
    expect(createObjectURLMock).toHaveBeenCalled();
    expect(revokeObjectURLMock).toHaveBeenCalled();
    expect(appendChildMock).toHaveBeenCalled();
    expect(removeChildMock).toHaveBeenCalled();
  });

  it('should download file by URL', () => {
    const url = 'test.txt';
    const filename = 'test.txt';
    const appendChildMock = vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => node);
    const removeChildMock = vi.spyOn(document.body, 'removeChild').mockImplementation((node: Node) => node);
    downloadByUrl(url, filename);
    expect(appendChildMock).toHaveBeenCalled();
    expect(removeChildMock).toHaveBeenCalled();
  });

  it('should convert Blob to text', async () => {
    const blob = new Blob(['Hello, World!'], { type: 'text/plain' });
    const text = await blobToText(blob);
    expect(text).toBe('Hello, World!');
  });

  it('should prepend CSV UTF-8 BOM only once', () => {
    const csv = 'name,age\nJohn,30';
    expect(withCsvUtf8Bom(csv)).toBe(UTF8_BOM + csv);
    expect(withCsvUtf8Bom(UTF8_BOM + csv)).toBe(UTF8_BOM + csv);
  });

  it('should export data as CSV with a UTF-8 BOM blob', async () => {
    const data = [{ name: 'John', age: 30 }];
    const filename = 'data';
    const createObjectURLMock = vi.mocked(URL.createObjectURL);
    const appendChildMock = vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => node);
    const removeChildMock = vi.spyOn(document.body, 'removeChild').mockImplementation((node: Node) => node);
    exportCsv(data, undefined, filename);
    expect(createObjectURLMock).toHaveBeenCalled();
    expect(appendChildMock).toHaveBeenCalled();
    expect(removeChildMock).toHaveBeenCalled();
    const blob = createObjectURLMock.mock.calls[0][0] as Blob;
    const bytes = await blobToBytes(blob);
    expect(Array.from(bytes.slice(0, 3))).toEqual([0xef, 0xbb, 0xbf]);
    await expect(blobToText(blob)).resolves.toBe('name,age\nJohn,30');
    const link = appendChildMock.mock.calls[0][0] as HTMLAnchorElement;
    expect(link.href).toBe('blob:url');
    expect(link.download).toBe('data.csv');
  });
});
