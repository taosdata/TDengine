import { describe, it, expect, vi } from 'vitest';
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
  exportCsv
} from '../files';

describe('files.ts', () => {
  it('should load a JS file', async () => {
    const jsFile = '../../config/uno.js';
    const script = await loadJS(jsFile);
    expect(script).toBeInstanceOf(HTMLScriptElement);
    expect(script?.src).toContain(jsFile);
  });

  it('should load a CSS file', async () => {
    const cssFile = 'https://www.taosdata.com/wp-content/uploads/master-slider/custom.css?ver=5.4';
    const link = await loadCss(cssFile);
    expect(link).toBeInstanceOf(HTMLLinkElement);
    expect(link.href).toContain(cssFile);
  });

  it('should load an image', async () => {
    const imgUrl = 'https://www.taosdata.com/wp-content/uploads/2022/02/site-logo.png';
    const img = await loadImage(imgUrl);
    expect(img).toHaveProperty('width');
    expect(img).toHaveProperty('height');
  });

  it('should convert base64 to Blob', () => {
    const base64 = 'data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==';
    const blob = base64ToBlob(base64);
    expect(blob).toBeInstanceOf(Blob);
  });

  // Bug 修复：添加了 await
  it('should convert URL to base64', async () => {
    const url = 'test.png';
    const base64 = await urlToBase64(url);
    expect(base64).toContain('data:image/png;base64,');
  });

  // Bug 修复：添加了 await
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
    const createObjectURLMock = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:url');
    const revokeObjectURLMock = vi.spyOn(URL, 'revokeObjectURL').mockImplementation((url: string) => {
      console.log(url);
    });
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
    const createObjectURLMock = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:url');
    const revokeObjectURLMock = vi.spyOn(URL, 'revokeObjectURL').mockImplementation((url: string) => {
      console.log(url);
    });
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

  // Bug 修复：添加了 await
  it('should convert Blob to text', async () => {
    const blob = new Blob(['Hello, World!'], { type: 'text/plain' });
    const text = await blobToText(blob);
    expect(text).toBe('Hello, World!');
  });

  it('should export data as CSV', () => {
    const data = [{ name: 'John', age: 30 }];
    const filename = 'data.csv';
    const createObjectURLMock = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:url');
    const revokeObjectURLMock = vi.spyOn(URL, 'revokeObjectURL').mockImplementation((url: string) => {
      console.log(url);
    });
    const appendChildMock = vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => node);
    const removeChildMock = vi.spyOn(document.body, 'removeChild').mockImplementation((node: Node) => node);
    exportCsv(data, undefined, filename);
    expect(createObjectURLMock).toHaveBeenCalled();
    expect(revokeObjectURLMock).toHaveBeenCalled();
    expect(appendChildMock).toHaveBeenCalled();
    expect(removeChildMock).toHaveBeenCalled();
  });
});
