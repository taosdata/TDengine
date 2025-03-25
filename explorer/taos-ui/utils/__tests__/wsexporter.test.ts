import { describe, it, expect, vi } from 'vitest';
import { wsExport } from '../wsexporter';
import streamSaver from 'streamsaver';
import { WSConfig, sqlConnect } from '@tdengine/websocket'; // 替换为实际的库

vi.mock('streamsaver', () => ({
  createWriteStream: vi.fn(() => ({
    getWriter: vi.fn(() => ({
      write: vi.fn(),
      close: vi.fn()
    }))
  }))
}));

vi.mock('@tdengine/websocket', () => ({
  WSConfig: vi.fn(),
  sqlConnect: vi.fn()
}));

describe('wsExport', () => {
  it('should export data to CSV', async () => {
    const mockWriter = {
      write: vi.fn(),
      close: vi.fn()
    };
    const mockFileStream = {
      getWriter: vi.fn(() => mockWriter)
    };
    streamSaver.createWriteStream.mockReturnValue(mockFileStream);

    const mockWs = {
      query: vi.fn(() => ({
        next: vi.fn().mockResolvedValueOnce(true).mockResolvedValueOnce(false),
        getData: vi.fn(() => ({
          /* mock data */
        }))
      }))
    };
    sqlConnect.mockResolvedValue(mockWs);

    const gatewayURL = 'ws://example.com';
    const token = 'mock-token';
    const sql = 'SELECT * FROM table';
    const withHeaders = true;

    await wsExport(gatewayURL, token, sql, withHeaders);

    expect(streamSaver.createWriteStream).toHaveBeenCalled();
    expect(mockFileStream.getWriter).toHaveBeenCalled();
    expect(mockWriter.write).toHaveBeenCalled();
    expect(mockWriter.close).toHaveBeenCalled();
    expect(sqlConnect).toHaveBeenCalledWith(expect.any(WSConfig));
    expect(mockWs.query).toHaveBeenCalledWith(sql);
  });
});
