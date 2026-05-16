import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';

const {
  mockValidateFormFields,
  mockFormatFromData,
  mockFetchTicketApi,
  mockCheckReadyFile,
  mockGetDatasets,
  mockInject,
  isShowDatasetTable,
  datasetTableData
} = vi.hoisted(() => ({
  mockValidateFormFields: vi.fn(),
  mockFormatFromData: vi.fn((value: Record<string, unknown>) => value),
  mockFetchTicketApi: vi.fn(),
  mockCheckReadyFile: vi.fn(),
  mockGetDatasets: vi.fn(),
  mockInject: vi.fn(() => ({ refs: { formRef: {} } })),
  isShowDatasetTable: { value: false },
  datasetTableData: { value: undefined as any }
}));

let intervalCallback: (() => Promise<void>) | undefined;

function createDeferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

vi.mock('vue', async () => {
  const actual = await vi.importActual<typeof import('vue')>('vue');
  return {
    ...actual
  };
});

vi.mock('./util', async () => {
  return {
    validateFormFields: mockValidateFormFields,
    formatFromData: mockFormatFromData,
    isShowDatasetTable,
    datasetTableData
  };
});

vi.mock('./useDataIn', () => ({
  getDataInProps: () => ({
    dataSource: {
      api: {
        fechTicketApi: mockFetchTicketApi,
        checkReadyFile: mockCheckReadyFile,
        getDatasets: mockGetDatasets
      }
    }
  })
}));

async function flushPromises() {
  await Promise.resolve();
  await Promise.resolve();
}

describe('useSearchPoint', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    vi.stubGlobal('inject', mockInject);
    intervalCallback = undefined;
    isShowDatasetTable.value = false;
    datasetTableData.value = undefined;

    mockValidateFormFields.mockImplementation((_formRef, onValid) => {
      onValid({ type: 'mqtt' }, 7);
    });

    vi.spyOn(globalThis, 'setInterval').mockImplementation(((callback: TimerHandler) => {
      intervalCallback = callback as () => Promise<void>;
      return 1 as unknown as ReturnType<typeof setInterval>;
    }) as typeof setInterval);
    vi.spyOn(globalThis, 'clearInterval').mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('clears previous preview data when requesting a new preview ticket fails', async () => {
    isShowDatasetTable.value = true;
    datasetTableData.value = { code: 0, data: { list: [{ name: 'old preview' }] } };
    mockFetchTicketApi.mockRejectedValueOnce(new Error('ticket failed'));

    const { default: useSearchPoint } = await import('./useSearchPoint');
    const { search } = useSearchPoint();

    search();
    await flushPromises();

    expect(isShowDatasetTable.value).toBe(false);
    expect(datasetTableData.value).toBeUndefined();
  });

  it('clears previous preview data when readiness polling fails', async () => {
    isShowDatasetTable.value = true;
    datasetTableData.value = { code: 0, data: { list: [{ name: 'old preview' }] } };
    mockFetchTicketApi.mockResolvedValueOnce({ ticket: 'ticket-1' });
    mockCheckReadyFile.mockRejectedValueOnce(new Error('poll failed'));

    const { default: useSearchPoint } = await import('./useSearchPoint');
    const { search } = useSearchPoint();

    search();
    await flushPromises();
    await intervalCallback?.();
    await flushPromises();

    expect(isShowDatasetTable.value).toBe(false);
    expect(datasetTableData.value).toBeUndefined();
  });

  it('ignores stale ticket responses from superseded preview requests', async () => {
    const firstTicketRequest = createDeferred<{ ticket: string }>();
    mockFetchTicketApi.mockReturnValueOnce(firstTicketRequest.promise).mockResolvedValueOnce({ ticket: 'ticket-2' });

    const { default: useSearchPoint } = await import('./useSearchPoint');
    const { search, ticket } = useSearchPoint();

    search();
    await flushPromises();

    search();
    await flushPromises();

    expect(ticket.value).toBe('ticket-2');

    firstTicketRequest.resolve({ ticket: 'ticket-1' });
    await flushPromises();

    expect(ticket.value).toBe('ticket-2');
  });
});
