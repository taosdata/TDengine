import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import axios from 'axios';
import { HttpRequest } from '../axios';

vi.mock('axios');
vi.mock('element-plus', () => ({
  ElLoading: {
    service: vi.fn(() => ({
      close: vi.fn()
    }))
  }
}));

describe('HttpRequest', () => {
  let httpRequest: HttpRequest;

  beforeEach(() => {
    httpRequest = new HttpRequest({ loading: true });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('should set request interceptor', () => {
    const onFulfilled = vi.fn();
    const onRejected = vi.fn();
    httpRequest.setRequestInterceptor(onFulfilled, onRejected);
    expect(httpRequest.instance.interceptors.request.use).toHaveBeenCalledWith(onFulfilled, onRejected);
  });

  it('should set response interceptor', () => {
    const onFulfilled = vi.fn();
    const onRejected = vi.fn();
    httpRequest.setResponseInterceptor(onFulfilled, onRejected);
    expect(httpRequest.instance.interceptors.response.use).toHaveBeenCalledWith(onFulfilled, onRejected);
  });

  it('should make a request', async () => {
    const response = { data: 'test' };
    const request = vi.spyOn(axios, 'request').mockResolvedValue(response);
    const result = await httpRequest.request({ url: 'test' });
    expect(request).toHaveBeenCalled();
    expect(result).toEqual(response);
  });
});
