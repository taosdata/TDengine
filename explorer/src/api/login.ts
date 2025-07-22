import { request } from "@/utils/request.ts";

//获取cluster的url和dashboard的url
export function getUrls() {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/profile`,
        method: "get",
        headers: {
            noAuth: true
        }
    });
}
export function fetchApiByCluster(token: string, data: Recordable) {
    return request({
        baseURL: import.meta.env.VITE_APP_BASE_URL,
        url: `/api/-/rest/sql`,
        method: "post",
        headers: {
            Authorization: token,
            "Content-Type": "text/plain"
        },
        data
    });
}
// 检查是否有绑定账号
export function fetchIsbinding() {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/isbinding`,
        method: "get",
        headers: {
            noAuth: true
        }
    });
}
// 获取图形验证码
export function fetchCaptcha(phone_email: string, ts: number | string) {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/captcha?phone_email=${phone_email}&ts=${ts}`,
        method: "get",
        headers: {
            noAuth: true
        },
        responseType: 'blob',
    });
}
// 发送验证码
export function fetchVerificationCode(phone_email: string, captcha: string, ts: string, lang: string) {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/verification-code?phone_email=${phone_email}&captcha=${captcha}&ts=${ts}&lang=${lang}`,
        method: "get",
        headers: {
            noAuth: true
        }
    });
}
// 校验验证码
export function getVerificationResult(data: Recordable) {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/verification-code`,
        method: "post",
        data,
        headers: {
            noAuth: true
        }
    });
}

// 校验验证码
export function reportTaosdInfo(data: Recordable) {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/taosd-info`,
        method: "post",
        data,
        headers: {
            noAuth: true
        }
    });
}

// 导入权限
export function importTaosInfo(data: Recordable) {
    return request({
        baseURL: import.meta.env.VITE_APP_EXPLORER_API,
        url: `/import`,
        method: "post",
        data,
        timeout: 10000,
    })
}