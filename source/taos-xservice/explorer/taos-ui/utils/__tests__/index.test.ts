import { describe, it, expect, vi, afterEach } from 'vitest';
import * as utils from '../index';

describe('utils', () => {
  // Tracks the descriptor that existed before the copy test mutates document.execCommand,
  // so afterEach can restore it exactly and not leak the property into other tests.
  let priorExecCommandDescriptor: PropertyDescriptor | null = null;

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
    // Restore document.execCommand to its pre-test state (absent in jsdom by default).
    if (priorExecCommandDescriptor !== null) {
      Object.defineProperty(document, 'execCommand', priorExecCommandDescriptor);
      priorExecCommandDescriptor = null;
    } else if (Object.getOwnPropertyDescriptor(document, 'execCommand')) {
      delete (document as unknown as Record<string, unknown>)['execCommand'];
    }
  });

  it('should copy text to clipboard', () => {
    const text = 'Hello, world!';
    // Force polyfill path by ensuring clipboard API is absent.
    vi.stubGlobal('navigator', { clipboard: null });
    // Capture the prior descriptor (undefined in jsdom) so afterEach can restore it.
    priorExecCommandDescriptor = Object.getOwnPropertyDescriptor(document, 'execCommand') ?? null;
    // jsdom doesn't have execCommand; define it so vi.spyOn can create a restorable mock.
    Object.defineProperty(document, 'execCommand', { value: () => true, writable: true, configurable: true });
    const execCommandSpy = vi.spyOn(document, 'execCommand').mockReturnValue(true);
    utils.copy(text);
    expect(execCommandSpy).toHaveBeenCalledWith('copy', true);
  });

  it('should get clipboard text', async () => {
    const readText = vi.fn().mockResolvedValue('Hello, world!');
    vi.stubGlobal('navigator', { clipboard: { readText } });

    const success = vi.fn();
    await utils.getClipboardText(success);
    expect(success).toHaveBeenCalledWith('Hello, world!');
  });

  it('should transform size correctly', () => {
    expect(utils.transformSize(1024, 'KB')).toBe('1 MB');
    expect(utils.transformSize(1048576, 'KB', 'MB', true)).toEqual([1024, 'MB']);
  });

  it('should transform capacity percent correctly', () => {
    expect(utils.transformCapacityPercent(1024, 2048, 'KB')).toBe('1/2 MB');
  });

  it('should not go out of bounds in transformCapacityPercent at the last unit', () => {
    // At max unit (YB), should clamp instead of advancing index past the array
    expect(utils.transformCapacityPercent(1024, 2048, 'YB')).toBe('1024/2048 YB');
    // ZB should advance exactly one step to YB and then stop
    expect(utils.transformCapacityPercent(1024, 2048, 'ZB')).toBe('1/2 YB');
  });

  it('should handle float correctly', () => {
    // 1.2345 in IEEE 754 is stored slightly below 1.2345, so:
    //   toFixed(2) → "1.23" (the 4 in the third decimal does not round up)
    //   toFixed(3) → "1.234" (the 5 in the fourth decimal does not round up)
    expect(utils.handleFloat(1.2345)).toBe(1.23);
    expect(utils.handleFloat(1.2345, 3)).toBe(1.234);
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
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    utils.openNewWindow('https://example.com');
    expect(openSpy).toHaveBeenCalledWith('https://example.com', '_blank');
  });

  it('should remove special characters correctly', () => {
    expect(utils.removeSpecialChar('Hello, World!')).toBe('Hello World');
  });

  it('should process uptime correctly', () => {
    expect(utils.processUptime(3661)).toBe('1h1min');
  });

  it('should request interval correctly', async () => {
    vi.useFakeTimers();
    vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => setTimeout(() => cb(Date.now()), 16));

    const fn = vi.fn();
    const cancel = utils.requestInterval(fn, 1000, true, true);
    await vi.advanceTimersByTimeAsync(1000);
    expect(fn).toHaveBeenCalledTimes(1);
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
