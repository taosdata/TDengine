let slideFlag = true;
let slideBottom = true;
// eslint-disable-next-line no-empty-function
const noop = function () {};
let scrollLockByCode = false;

export const watchScroll = function ({ containerId = "app", onSlideOnce = noop, onSlideBottomOnce = noop }) {
  const handler = function () {
    // console.log('watch scorll, scrollLockByCode=', scrollLockByCode)
    let clientHeight = document.documentElement.clientHeight || document.body.clientHegiht; //页面高度
    let divObj = document.getElementById(containerId);
    let scrollHeight = divObj && divObj.scrollHeight; //滚动条总高度
    //滚动条已经滚动的高度(被卷曲的高度)
    let oTop = document.body.scrollTop == 0 ? document.documentElement.scrollTop : document.body.scrollTop;
    // 滑动事件
    if (oTop >= 50 && slideFlag) {
      onSlideOnce && onSlideOnce();
      slideFlag = false;
    }
    // 触底事件
    if (oTop + clientHeight >= scrollHeight && slideBottom) {
      onSlideBottomOnce && onSlideBottomOnce();
      slideBottom = false;
    }
  };
  // 绑定滚动监听
  bindScroll(handler);
};

// 绑定监听，仅仅使用一次addEventListener来处理
const handlerArray = [];
export const bindScroll = function (handler, handlerAll) {
  if (Object.prototype.toString.call(handler).toLowerCase() !== "[object function]") {
    return;
  }
  // 如果是全量监听，则给函数添加标记
  if (handlerAll === true) {
    handler._handleAll = true;
  }
  handlerArray.push(handler);
  // 启动初始化绑定
  if (!handlerArray._inited) {
    window.addEventListener(
      "scroll",
      function (event) {
        handlerArray.forEach(fn => {
          // 全量监听的代码每次都执行
          // 否则只在非程序滚动的过程中监听
          if (fn._handleAll || !scrollLockByCode) {
            try {
              fn(event);
            } catch (err) {
              console.error("scroll handler error", err);
            }
          }
        });
      },
      {
        capture: true,
        passive: true,
      }
    );
    handlerArray._inited = true;
  }
  // 返回一个解绑函数
  return function () {
    const index = handlerArray.indexOf(handler);
    index >= 0 && handlerArray.splice(index, 1);
  };
};

// 缓动到页面的指定元素位置
export const scrollTo = function (element, speed = 70, offset = 50, freeze = 100) {
  // 如果是数字，则模拟dom接口返回定位数据
  let giveNumber = false;
  if (typeof element === "number" && !isNaN(element) && element >= 0) {
    giveNumber = true;
  }
  if (!giveNumber) {
    if (!element) {
      return Promise.resolve();
    }
    if (!element.getBoundingClientRect) {
      element = document.querySelectorAll(element)[0];
    }
    if (!element) {
      return Promise.resolve();
    }
  }
  // 返回一个异步任务
  return new Promise(resolve => {
    let top = 0;
    if (!giveNumber) {
      // 获取元素相对窗口的top值，此处应加上窗口本身的偏移
      top = window.pageYOffset + element.getBoundingClientRect().top;
    } else {
      top = element;
    }
    let currentTop = window.pageYOffset;
    let requestId;
    // 如果移动距离超过1000像素，则提速
    const distance = Math.abs(currentTop - top);
    speed = distance > 1000 ? (speed * distance) / 1000 : speed;

    // 对移动的目标增加一些空隙
    top = top < offset ? 0 : top - offset;
    speed = currentTop >= top ? -1 * speed : speed;
    //采用requestAnimationFrame，平滑动画
    function step() {
      currentTop += speed;
      if (Math.abs(currentTop - top) >= Math.abs(speed)) {
        window.scrollTo(0, currentTop);
        requestId = window.requestAnimationFrame(step);
      } else {
        window.scrollTo(0, top);
        window.cancelAnimationFrame(requestId);
        // 锁定时间
        // console.log("freeze time============", freeze)
        scrollTo.freezetimer = setTimeout(function () {
          scrollLockByCode = false;
        }, freeze);
        resolve();
      }
    }
    // console.log("set scrollLockByCode true")
    scrollTo.freezetimer && clearTimeout(scrollTo.freezetimer);
    scrollLockByCode = true;
    window.requestAnimationFrame(step);
  });
};

// 将指定元素滚动到页面中心
// 主要为了修复输入法遮挡input的情况
export const scrollToCenter = function (dom, speed = 80, _forceStop) {
  if (!dom || dom.offsetTop === undefined) {
    return;
  }
  const domHeight = dom.offsetHeight;
  const clientHeight = document.documentElement.clientHeight;
  const offset = clientHeight - domHeight;
  if (!domHeight || offset <= 0) {
    // 锁定时间，防止watchScroll执行滚动处理代码
    scrollTo(dom, speed, 0, 1000);
    return;
  }
  scrollTo(dom, speed, offset / 2, 1000).then(function () {
    if (_forceStop) {
      return;
    }
    // 滚定完毕后再次检测一下位置，防止部分sb浏览器处理不及时
    // 再次修复定位后就不再继续调整，防止死循环
    if (clientHeight != document.documentElement.clientHeight) {
      scrollToCenter(dom, speed, true);
    }
  });
};
