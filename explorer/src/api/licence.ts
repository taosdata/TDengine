import { request } from "@/utils/request.ts";
import pathDetector from '@/utils/pathDetector';

//激活 licence
export function activeLicence(data: Recordable) {
  return request({
      baseURL: pathDetector.getApiBasePath(),
      headers:{
        "Content-Type":"application/json"
      },
      url: '/license',
      method: "post",
      data
  });
}
