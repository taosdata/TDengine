/**
 * 是否火狐浏览器
 * @returns {boolean}
 */
export function isFirefox() {
    return navigator.userAgent.includes("Firefox");
}