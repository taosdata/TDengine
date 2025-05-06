import { describe, it, expect, vi } from 'vitest';
import * as utils from '../index';

describe('utils', () => {
  it('should copy text to clipboard', () => {
    const text = 'Hello, world!';
    document.execCommand = vi.fn();
    utils.copy(text);
    expect(document.execCommand).toHaveBeenCalledWith('copy', true);
  });

  it('should get clipboard text', () => {
    const text = 'Hello, world!';
    document.execCommand = vi.fn(() => {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      document.body.appendChild(textarea);
      return true;
    });
    utils.getClipboardText(result => {
      expect(result).toBe(text);
    });
  });

  it('should transform size correctly', () => {
    expect(utils.transformSize(1024, 'KB')).toBe('1 MB');
    expect(utils.transformSize(1048576, 'KB', 'MB', true)).toEqual([1024, 'MB']);
  });

  it('should transform capacity percent correctly', () => {
    expect(utils.transformCapacityPercent(1024, 2048, 'KB')).toBe('1/2 MB');
  });

  it('should handle float correctly', () => {
    expect(utils.handleFloat(1.2345)).toBe(1.23);
    expect(utils.handleFloat(1.2345, 3)).toBe(1.235);
  });

  it('should escape HTML correctly', () => {
    expect(utils.escapeHtml('<div>"Hello"&\'World\'</div>')).toBe(
      '&lt;div&gt;&quot;Hello&quot;&amp;&#39;World&#39;&lt;/div&gt;'
    );
  });

  it('should convert HTML to text correctly', () => {
    expect(utils.htmlToText('<div>Hello <b>World</b></div>')).toBe('Hello World');
  });

  it('should transform uptime correctly', () => {
    expect(utils.transformUpTime(3661)).toBe('1h1min');
  });

  it('should generate uuid correctly', () => {
    const uuid = utils.uuid();
    expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  });

  it('should convert JSON to object correctly', () => {
    expect(utils.jsonToObj('{"key": "value"}')).toEqual({ key: 'value' });
  });

  it('should convert blob to object correctly', async () => {
    const blob = new Blob([JSON.stringify({ key: 'value' })], { type: 'application/json' });
    const result = await utils.blobToObject(blob);
    expect(result).toEqual({ key: 'value' });
  });

  it('should open new window correctly', () => {
    window.open = vi.fn();
    utils.openNewWindow('https://example.com');
    expect(window.open).toHaveBeenCalledWith('https://example.com', '_blank');
  });

  it('should remove special characters correctly', () => {
    expect(utils.removeSpecialChar('Hello, World!')).toBe('Hello World');
  });

  it('should process uptime correctly', () => {
    expect(utils.processUptime(3661)).toBe('1h1min');
  });

  it('should request interval correctly', () => {
    const fn = vi.fn();
    const cancel = utils.requestInterval(fn, 1000, true, true);
    expect(fn).toHaveBeenCalled();
    cancel();
  });

  it('should get axis type correctly', () => {
    expect(utils.getAxisType('2023-01-01')).toBe('time');
    expect(utils.getAxisType('123')).toBe('value');
    expect(utils.getAxisType('abc')).toBe('category');
  });

  it('should get mouse position correctly', () => {
    const event = { clientX: 100, clientY: 100, target: { offsetLeft: 50, offsetTop: 50 } } as unknown as MouseEvent;
    const position = utils.getMousePosition(event);
    expect(position).toEqual({ x: 50, y: 50 });
  });
});
