import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { localExport, wsExport } from '../wsexporter';
import { UTF8_BOM } from '../files';
import streamSaver from 'streamsaver';
import { connect, TaosResult } from '@tdengine/websocket';
import FileSaver from 'file-saver';

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

  async text() {
    return this.parts.join('');
  }
}

vi.mock('streamsaver', () => ({
  default: {
    createWriteStream: vi.fn()
  }
}));

vi.mock('@tdengine/websocket', () => ({
  connect: vi.fn(),
  TaosResult: vi.fn().mockImplementation(function () {
    this.data = [];
    this.setRows = (rows: { data?: unknown[] }) => {
      this.data = rows.data ?? [];
    };
  })
}));

// Return non-empty CSV so the Blob has bytes and writer.write is actually invoked.
vi.mock('json-2-csv', () => ({
  json2csv: vi.fn().mockReturnValue('ts,value\n2024-01-01,42')
}));

vi.mock('file-saver', () => ({
  default: {
    saveAs: vi.fn()
  }
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
    vi.mocked(streamSaver.createWriteStream).mockReturnValue(mockFileStream as never);

    const queryResult = { id: 1 };
    const fetchResponses = [
      { completed: false, data: [['ts', 1n]] },
      { completed: true, data: [] }
    ];
    const mockWsInterface = {
      query: vi.fn().mockResolvedValue(queryResult),
      fetch: vi
        .fn()
        .mockResolvedValueOnce(fetchResponses[0])
        .mockResolvedValueOnce(fetchResponses[1]),
      fetchBlock: vi.fn().mockImplementation(async (rows, result) => {
        result.setRows(rows);
      }),
      freeResult: vi.fn()
    };
    const mockWs = {
      connect: vi.fn().mockResolvedValue(undefined),
      close: vi.fn(),
      _wsInterface: mockWsInterface
    };
    vi.mocked(connect).mockReturnValue(mockWs as never);

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
    expect(mockWriter.write).toHaveBeenCalledTimes(2);
    expect(Array.from(mockWriter.write.mock.calls[0][0] as Uint8Array)).toEqual(
      Array.from(new TextEncoder().encode(UTF8_BOM))
    );
    expect(mockWriter.close).toHaveBeenCalled();
    expect(connect).toHaveBeenCalledWith(expect.stringContaining('/rest/ws?token=mock-token'));
    expect(mockWs.connect).toHaveBeenCalled();
    expect(mockWsInterface.query).toHaveBeenCalledWith(sql);
    expect(TaosResult).toHaveBeenCalledWith(queryResult);
    expect(mockWsInterface.fetchBlock).toHaveBeenCalled();
    expect(mockWsInterface.freeResult).toHaveBeenCalledWith(queryResult);
    expect(mockWs.close).toHaveBeenCalled();
  });

  it('exports local query results with a UTF-8 BOM', async () => {
    const queryResult = {
      head: [{ field: 'ts' }, { field: 'value' }],
      data: [['2024-01-01', '中文']]
    };

    localExport(queryResult);

    expect(FileSaver.saveAs).toHaveBeenCalled();
    const blob = vi.mocked(FileSaver.saveAs).mock.calls[0][0] as unknown as MockBlob;
    await expect(blob.text()).resolves.toBe(`${UTF8_BOM}ts,value\n2024-01-01,中文`);
  });
});
