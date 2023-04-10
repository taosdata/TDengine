package com.taosdata.controller;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.caches.StatusCache;
import com.taosdata.model.dto.DataInfo;
import com.taosdata.model.dto.ReqDto;
import com.taosdata.model.dto.ResDto;
import com.taosdata.model.dto.bum.NettyInfo;
import com.taosdata.model.dto.bum.QueueInfo;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.enums.ResEnums;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 监控概览 服务接口
 *
 * @author ZYP
 */
@RestController
@RequestMapping(value = "/api/dashboard")
public class DashboardController {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected Logger sys_user_logger = LoggerFactory.getLogger("sys-user");

    /**
     * 获取进程信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getProcessInfo")
    @ResponseBody
    public ResDto getProcessInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
        // 定义响应内容
        ResDto resDto = new ResDto();
        try {
            // 获取请求源ip
            reqDto.setIp(request.getRemoteAddr());
            // 请求日志
            sys_user_logger.info(reqDto.toString());
            // 验证账号等信息
            // TODO
            // 获取请求包体（泛型转化）
            // TODO
            // 结果数据
            DataInfo<JSONObject> dataInfo = new DataInfo<>(new JSONObject());
            dataInfo.getData().put("startTime", StatusCache.getStartTime());
            dataInfo.getData().put("status", StatusCache.getStatus());
            dataInfo.getData().put("description", StatusCache.getDescription());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("查询进程信息过程中发生异常，exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取线程信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getThreadInfo")
    @ResponseBody
    public ResDto getThreadInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
        // 定义响应内容
        ResDto resDto = new ResDto();
        try {
            // 获取请求源ip
            reqDto.setIp(request.getRemoteAddr());
            // 请求日志
            sys_user_logger.info(reqDto.toString());
            // 验证账号等信息
            // TODO
            // 获取请求包体（泛型转化）
            // TODO
            // 结果数据
            DataInfo<List<ThreadInfo>> dataInfo = new DataInfo<>(new ArrayList<>());
            dataInfo.setData(StatusCache.getThreadInfo());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("查询线程信息过程中发生异常，exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取内存队列信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getQueueInfo")
    @ResponseBody
    public ResDto getQueueInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
        // 定义响应内容
        ResDto resDto = new ResDto();
        try {
            // 获取请求源ip
            reqDto.setIp(request.getRemoteAddr());
            // 请求日志
            sys_user_logger.info(reqDto.toString());
            // 验证账号等信息
            // TODO
            // 获取请求包体（泛型转化）
            // TODO
            // 结果数据
            DataInfo<List<QueueInfo>> dataInfo = new DataInfo<>(new ArrayList<>());
            dataInfo.setData(StatusCache.getQueueInfo());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("查询线程信息过程中发生异常，exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取连接信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getNettyInfo")
    @ResponseBody
    public ResDto getNettyInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
        // 定义响应内容
        ResDto resDto = new ResDto();
        try {
            // 获取请求源ip
            reqDto.setIp(request.getRemoteAddr());
            // 请求日志
            sys_user_logger.info(reqDto.toString());
            // 验证账号等信息
            // TODO
            // 获取请求包体（泛型转化）
            // TODO
            // 结果数据
            DataInfo<NettyInfo> dataInfo = new DataInfo<>(new NettyInfo());
            dataInfo.setData(StatusCache.getNettyInfo());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("查询连接信息过程中发生异常，exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取速度信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getSpeedInfo")
    @ResponseBody
    public ResDto getSpeedInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
        // 定义响应内容
        ResDto resDto = new ResDto();
        try {
            // 获取请求源ip
            reqDto.setIp(request.getRemoteAddr());
            // 请求日志
            sys_user_logger.info(reqDto.toString());
            // 验证账号等信息
            // TODO
            // 获取请求包体（泛型转化）
            // TODO
            // 结果数据
            DataInfo<Map<String, Long>> dataInfo = new DataInfo<>(new HashMap<>());
            dataInfo.getData().put(FluxEnums.PushData.getDesc(), FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).getSpeed());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("查询速度信息过程中发生异常，exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }
}
