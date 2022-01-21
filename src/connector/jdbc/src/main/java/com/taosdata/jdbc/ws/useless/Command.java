package com.taosdata.jdbc.ws;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * taosadapter
 * <p>
 * // 1. 进行认证
 * {"action":"conn","args":{"user":"root","password":"taosdata"}}
 * // 2. 查询数据  可能出现的异常{"code":-2147483382,"message":"Ref is not there"} 原因是超过30s没有拉取
 * {"action":"query","args":{"sql":"select * from test.meters limit 10000"}}
 * // 3. 获取数据 根据查询返回id寻找fetch地址。可以并发执行，由id区分
 * {"action":"fetch","args":{"id":1}}
 * // 4. 二进制获取返回信息  返回3000多条记录是tasd设置的返回阈值
 * {"action":"fetch_block","args":{"id":1}}
 * // 5. json形式返回信息
 * {"action":"fetch_json","args":{"id":1}}
 * // 6. 下次一次查询 可能返回结果为complete表示结束
 * {"action":"fetch","args":{"id":1}}
 */
// 没有释放资源的接口
// use databse
public class Command {
    private String action;
    private Map<String, Object> args;

    public Command() {
    }

    public Command(String action, Map<String, Object> args) {
        this.action = action;
        this.args = args;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
