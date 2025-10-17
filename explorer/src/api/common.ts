import { request } from '@/utils/request.ts';

//执行开始方法
export function executeStart(id) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${id}/start`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

//执行停止方法
export function executeStop(id) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${id}/stop`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export function executeDel(id, yesDeleteFile) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${id}?after_delete=${yesDeleteFile ? 'clear' : ''}`,
    method: 'delete'
  });
}
