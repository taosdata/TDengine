import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  addLogEvent,
  formatDurationLabel,
  getLogCreatedAt,
  getLogId,
  handleSqlExecuteFail,
  handleSqlExecuteSuccess,
  parseStoredLogRecords
} from './utils'

function captureNextLog() {
  return new Promise<Recordable>(resolve => {
    const off = addLogEvent.on(log => {
      off()
      resolve(log)
    })
  })
}

function captureNextLogs(count: number) {
  return new Promise<Recordable[]>(resolve => {
    const logs: Recordable[] = []
    const off = addLogEvent.on(log => {
      logs.push(log)
      if (logs.length >= count) {
        off()
        resolve(logs)
      }
    })
  })
}

function makeSqlResult(overrides: Partial<RestApiResult> = {}): RestApiResult {
  return {
    code: 0,
    rows: 1,
    timing: 2_500_000,
    column_meta: [['v', 'INT', 4] as any],
    data: [['1']],
    ...overrides
  } as RestApiResult
}

describe('sql timing display', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('converts timing from ns to ms and computes network time', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const waitLog = captureNextLog()

    handleSqlExecuteSuccess(makeSqlResult({ timing: 2_500_000 }), 'select 1', 1000)
    const log = await waitLog

    expect(log.totalTime).toBe(4000)
    expect(log.executeTime).toBe(2.5)
    expect(log.networkTime).toBe(3997.5)
  })

  it('does not fabricate execute/network values when timing is missing', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const waitLog = captureNextLog()

    handleSqlExecuteSuccess(makeSqlResult({ timing: undefined as any }), 'select 1', 1000)
    const log = await waitLog

    expect(log.totalTime).toBe(4000)
    expect(log.executeTime).toBeNull()
    expect(log.networkTime).toBeNull()
  })

  it('emits createdAt for successful logs', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1234567890)
    const waitLog = captureNextLog()

    handleSqlExecuteSuccess(makeSqlResult({ timing: 2_500_000 }), 'select 1', 1000)
    const log = await waitLog

    expect(log.createdAt).toBe(1234567890)
  })

  it('emits createdAt for failed logs', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1234567890)
    const waitLog = captureNextLog()

    handleSqlExecuteFail({ code: 9750, desc: 'Database not specified' } as RestApiResult, 'select count(*) from meters', 1000)
    const log = await waitLog

    expect(log.createdAt).toBe(1234567890)
  })

  it('emits unique stable logId for each log record', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1234567890)
    const waitLogs = captureNextLogs(2)

    handleSqlExecuteSuccess(makeSqlResult({ timing: 2_500_000 }), 'select 1', 1000)
    handleSqlExecuteSuccess(makeSqlResult({ timing: 2_500_000 }), 'select 1', 1000)
    const [first, second] = await waitLogs

    const firstId = getLogId(first)
    const secondId = getLogId(second)
    expect(firstId).toMatch(/^1234567890-/)
    expect(secondId).toMatch(/^1234567890-/)
    expect(firstId).not.toBe(secondId)
  })

  it('accepts both createdAt and legacy createAt log timestamps', () => {
    expect(getLogCreatedAt({ createdAt: 1700000000000 } as Recordable)).toBe(1700000000000)
    expect(getLogCreatedAt({ createAt: 1800000000000 } as Recordable)).toBe(1800000000000)
    expect(getLogCreatedAt({} as Recordable)).toBeNull()
  })

  it('does not throw when object has non-function toString', () => {
    expect(() => formatDurationLabel({ toString: 1 } as any)).not.toThrow()
    expect(formatDurationLabel({ toString: 1 } as any)).toBe('--')
  })

  it('parses only array logs from local storage payload', () => {
    expect(parseStoredLogRecords('[{"sql":"select 1"}]')).toEqual([{ sql: 'select 1' }])
    expect(parseStoredLogRecords('{"sql":"select 1"}')).toEqual([])
  })

  it('returns empty list for malformed local storage payload', () => {
    expect(parseStoredLogRecords('{not-json')).toEqual([])
  })

  it('formats duration with only s/ms units and up to two decimals', () => {
    expect(formatDurationLabel(6020.791487)).toBe('6.02 s')
    expect(formatDurationLabel(11.208512999999584)).toBe('11.21 ms')
    expect(formatDurationLabel(6032)).toBe('6.03 s')
    expect(formatDurationLabel(1000)).toBe('1 s')
    expect(formatDurationLabel(null)).toBe('--')
  })
})
