import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { wsExport } from '../wsexporter';
import streamSaver from 'streamsaver';
import { connect } from '@tdengine/websocket';

// jsdom ships an incomplete Blob: no arrayBuffer(), no stream().
// Replace the global with a minimal implementation that supports stream() so the
// production code path (csvBlob.stream() → reader → writer.write()) executes.
class MockBlob {
  private readonly parts: string[];
  readonly type: string;

  constructor(parts: string[], options?: BlobPropertyBag) {
    this.parts = parts;
    this.type = options?.type ?? '';
  }

  get size() {
    return new TextEncoder().encode(this.parts.join('')).length;
  }

  stream(): ReadableStream<Uint8Array> {
    const bytes = new TextEncoder().encode(this.parts.join(''));
    return new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes);
        controller.close();
      }
    });
  }
}

vi.mock('streamsaver', () => ({
  default: {
    createWriteStream: vi.fn(() => ({
      getWriter: vi.fn(() => ({
        write: vi.fn(),
        close: vi.fn(),
        abort: vi.fn()
      }))
    }))
  }
}));

vi.mock('@tdengine/websocket', () => ({
  connect: vi.fn(),
  TaosResult: vi.fn(() => ({
    setRows: vi.fn(),
    data: [{ ts: '2024-01-01', value: 42 }]
  }))
}));

// Return non-empty CSV so the Blob has bytes and writer.write is actually invoked.
vi.mock('json-2-csv', () => ({
  json2csv: vi.fn().mockReturnValue('ts,value\n2024-01-01,42')
}));

vi.mock('config', () => ({
  project: {
    isCloud: false,
    isAliyun: false
  }
}));

describe('wsExport', () => {
  beforeEach(() => {
    vi.stubGlobal('Blob', MockBlob);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('executes one loop iteration and writes CSV bytes', async () => {
    const mockWriter = {
      write: vi.fn().mockResolvedValue(undefined),
      close: vi.fn(),
      abort: vi.fn()
    };
    const mockFileStream = {
      getWriter: vi.fn(() => mockWriter)
    };
    (streamSaver.createWriteStream as ReturnType<typeof vi.fn>).mockReturnValue(mockFileStream);

    const mockWsInterface = {
      query: vi.fn().mockResolvedValue({}),
      // First fetch: completed=false triggers one loop iteration; second: exits the loop.
      fetch: vi.fn().mockResolvedValueOnce({ completed: false }).mockResolvedValueOnce({ completed: true }),
      fetchBlock: vi.fn().mockResolvedValue(undefined),
      freeResult: vi.fn()
    };
    const mockWs = {
      connect: vi.fn().mockResolvedValue(undefined),
      _wsInterface: mockWsInterface,
      close: vi.fn()
    };
    (connect as ReturnType<typeof vi.fn>).mockReturnValue(mockWs);

    const gatewayURL = 'ws://example.com';
    const token = 'mock-token';
    const sql = 'SELECT * FROM table';
    const withHeaders = true;

    await wsExport(gatewayURL, token, sql, withHeaders);

    expect(streamSaver.createWriteStream).toHaveBeenCalled();
    expect(mockFileStream.getWriter).toHaveBeenCalled();
    expect(connect).toHaveBeenCalledWith(expect.any(String));
    expect(mockWs.connect).toHaveBeenCalled();
    expect(mockWsInterface.query).toHaveBeenCalledWith(sql);
    // Two fetch calls: one before the loop and one at the end of the single iteration.
    expect(mockWsInterface.fetch).toHaveBeenCalledTimes(2);
    expect(mockWsInterface.fetchBlock).toHaveBeenCalledTimes(1);
    // The core assertion: bytes from the CSV Blob must have been written.
    expect(mockWriter.write).toHaveBeenCalled();
    expect(mockWriter.close).toHaveBeenCalled();
  });
});
