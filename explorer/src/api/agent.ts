import { request } from '@/utils/request.ts';
import pathDetector from '@/utils/pathDetector';
import store from '@/store';

export function getAgentsData() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/agents`,
    method: 'get',
    withCredentials: true
  });
}
export function addNewAgent(name) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/agents`,
    method: 'post',
    data: {
      cluster_id: localStorage.getItem('local_clusterID'),
      dsn: localStorage.getItem('base_url'),
      name,
      user_id: localStorage.getItem('username')
    }
  });
}

export function deleteAgent(id) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/agents/${id}`,
    method: 'delete'
  });
}

export function editAgent(name, id) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/agents/${id}`,
    method: 'patch',
    data: {
      name
    }
  });
}
