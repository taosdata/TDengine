import { computed, ComputedRef } from "vue"
import { mapState, useStore } from "vuex"

type StateMap = { [key: string]: string } | string[];

export default function (state: StateMap): { [key: string]: ComputedRef<any> }{
    // 1. 获取实例 $store
    const store = useStore()
    // 2. 遍历状态数据
    const storeStateFns = mapState(state)
    // 3. 存放处理好的数据对象
    const storeState = {}
    // 4. 对每个函数进行computed
    Object.keys(storeStateFns).forEach(fnKey => {
        const fn = storeStateFns[fnKey].bind({ $store: store })
        // 遍历生成这种数据结构 => {name: ref(), age: ref()}
        storeState[fnKey] = computed(fn)
    })
    return storeState
}

