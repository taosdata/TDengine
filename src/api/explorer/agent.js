import { request } from "@/utils/request";

export function getAgentsData() {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents?cluster_id=${localStorage.getItem("local_clusterID")}&user_id=${localStorage.getItem("username")}`,
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

export function editAgent(name,id) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/agents/${id}`,
    method: "patch",
    data:{
      name
    }
  });
}
