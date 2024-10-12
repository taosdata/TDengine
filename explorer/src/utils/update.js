
// 引入提示框页面组件
import Modal from "../components/updateModal.vue"
import Vue from "vue"
import i18n from "@/lang";
 
let time = 0            // 计算轮询次数
let version = ""        // 缓存的版本号
 
// 轮询用检测方法
let timerFuncion = async () => {
  // debugger
    // 次数超过的时候 停止轮询 防止用户挂着网页一直轮询
    if (time >= 5) {
        // 仅清除计时器
        clearInterval(timer)
        return timer = null
    }
    // fetch 部署后同层级的version文件 并且加上时间戳参数 防止去访问本地硬盘的缓存
    let res = await fetch(`/version.txt?v=${new Date().getTime().toString()}`).then((res) => {
        return res.json()
    }).catch((err) => {
        console.log(err)
        return clearTimer() // 访问失败就完全关闭轮询
    })
    // console.log("存储的version:" + version)
    // console.log("获取到version:" + res)
    // console.log("比较结果:", res == version)
 
    // 首次加载网页的时候 存储第一份version
    if (!version) {
      version = res
    } else if (res && version != res) {
        // 弹出更新提示 发现verison文件更新了 就代表新部署了
        // 借鉴Element的Message实现挂载vue组件到页面上
        let MessageConstructor = Vue.extend(Modal);
        let instance = new MessageConstructor({
          data: {i18n}
        })
        instance.id = new Date().getTime().toString()
        instance.$mount();
        document.body.appendChild(instance.$el);
        return clearTimer()
    }
 
    time++
}
// 检测鼠标是否移动 移动代表用户活跃中 把轮询比较用的次数一直清0
let moveFunction = () => {
    time = 0
    // 长时间挂机后 不在轮询的网页 在鼠标活跃于窗口的时候重新检测
    if (!timer) {
        timer = setInterval(timerFuncion, 10000)
    }
}
// 当被main.js 引用的时候 开始轮询于监听鼠标移动事件
let timer = setInterval(timerFuncion, 10000)
window.addEventListener("mousemove", moveFunction)
// 完全清除轮询 不轮询 不监听鼠标事件
let clearTimer = () => {
    clearInterval(timer)
    window.removeEventListener("mousemove", moveFunction)
    timer = null
}
 