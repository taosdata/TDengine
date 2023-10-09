import { request } from "@/utils/request";

export function getAgentsData(clusterid, userid) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents?cluster_id=${clusterid}&user_id=${userid}`,
    method: "get",
  });
}
export function addNewAgent(name) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents`,
    method: "post",
    data: {
      cluster_id: localStorage.getItem("local_clusterID"),
      dsn: localStorage.getItem("base_url"),
      name,
      user_id: localStorage.getItem("username"),
    },
  });
}

export function deleteAgent(id) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents/${id}`,
    method: "delete",
  });
}

export function editAgent(id, data) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents/${id}`,
    method: "patch",
    data: data,
  });
}
