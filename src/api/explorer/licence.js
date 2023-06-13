import { request } from "@/utils/request";

//激活 licence
export function activeLicence(data) {
  return request({
      baseURL: process.env.VUE_APP_EXPLORER_API,
      headers:{
        "Content-Type":"application/json"
      },
      url: '/license',
      method: "post",
      data
  });
}