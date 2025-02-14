import { request } from "@/utils/request.ts";

//激活 licence
export function activeLicence(data: Recordable) {
  return request({
      baseURL: import.meta.env.VUE_APP_EXPLORER_API,
      headers:{
        "Content-Type":"application/json"
      },
      url: '/license',
      method: "post",
      data
  });
}