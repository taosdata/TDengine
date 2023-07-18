package com.taosdata.controller;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.model.dto.DataInfo;
import com.taosdata.model.dto.ReqDto;
import com.taosdata.model.dto.ResDto;
import com.taosdata.model.dto.bum.InfluxdbInfo;
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
@RequestMapping(value = "/dashboard")
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
            logger.error("An exception occurred during the process of querying 'Process Information', exception={}", e.getMessage());
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
            logger.error("An exception occurred during the process of querying 'Thread Information', exception={}", e.getMessage());
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
            logger.error("An exception occurred during the process of querying 'Queue Information', exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取Influxdb连接信息
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getInfluxdbInfo")
    @ResponseBody
    public ResDto getInfluxdbInfo(@RequestBody ReqDto reqDto, HttpServletRequest request) {
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
            DataInfo<InfluxdbInfo> dataInfo = new DataInfo<>(new InfluxdbInfo());
            dataInfo.setData(StatusCache.getInfluxdbInfo());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("An exception occurred during the process of querying 'InfluxDB Connection Information', exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取Socket连接信息
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
            logger.error("An exception occurred during the process of querying 'Socket Connection Information', exception={}", e.getMessage());
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
            dataInfo.getData().put(FluxEnums.ReadData.getDesc(), FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).getSpeed());
            dataInfo.getData().put(FluxEnums.PushData.getDesc(), FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).getSpeed());
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("An exception occurred during the process of querying 'Speed Information', exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取任务统计
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getTaskStatistic")
    @ResponseBody
    public ResDto getTaskStatistic(@RequestBody ReqDto reqDto, HttpServletRequest request) {
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
            dataInfo.getData().put("totalTaskEstimated", StatisticCache.totalReadTaskEstimated);
            dataInfo.getData().put("totalTaskCreated", StatisticCache.createdTaskSet.size());
            dataInfo.getData().put("completedTask", StatisticCache.completedTaskSet.size());
            dataInfo.getData().put("TotalRead", StatisticCache.totalRead);
            dataInfo.getData().put("TotalPush", StatisticCache.totalPush);
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("An exception occurred during the process of querying 'Task Statistics', exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }

    /**
     * 获取全部监控数据
     *
     * @param reqDto
     * @param request
     * @return
     */
    @RequestMapping(value = "/getAll")
    @ResponseBody
    public ResDto getAll(@RequestBody ReqDto reqDto, HttpServletRequest request) {
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
            // 进程信息
            JSONObject processInfo = new JSONObject();
            processInfo.put("startTime", StatusCache.getStartTime());
            processInfo.put("status", StatusCache.getStatus());
            processInfo.put("description", StatusCache.getDescription());
            dataInfo.getData().put("processInfo", processInfo);
            // 线程信息
            dataInfo.getData().put("threadInfo", StatusCache.getThreadInfo());
            // 队列信息
            dataInfo.getData().put("queueInfo", StatusCache.getQueueInfo());
            // Influxdb连接信息
            dataInfo.getData().put("influxdbInfo", StatusCache.getInfluxdbInfo());
            // Socket连接信息
            dataInfo.getData().put("nettyInfo", StatusCache.getNettyInfo());
            // 速度信息
            Map<String, Long> speedInfo = new HashMap<>();
            speedInfo.put(FluxEnums.ReadData.getDesc(), FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).getSpeed());
            speedInfo.put(FluxEnums.PushData.getDesc(), FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).getSpeed());
            dataInfo.getData().put("speedInfo", speedInfo);
            // 任务统计
            JSONObject taskStatistic = new JSONObject();
            taskStatistic.put("totalTaskEstimated", StatisticCache.totalReadTaskEstimated);
            taskStatistic.put("totalTaskCreated", StatisticCache.createdTaskSet.size());
            taskStatistic.put("completedTask", StatisticCache.completedTaskSet.size());
            taskStatistic.put("totalRead", StatisticCache.totalRead);
            taskStatistic.put("totalPush", StatisticCache.totalPush);
            dataInfo.getData().put("taskStatistic", taskStatistic);
            // 封装响应
            resDto.setCode(ResEnums.SUCCESS.getCode());
            resDto.setMsg(ResEnums.SUCCESS.getMsg());
            resDto.setData(dataInfo);
            // 响应日志
            sys_user_logger.info(resDto.toString());
        } catch (Exception e) {
            logger.error("An exception occurred during the process of querying 'All Monitoring Data', exception={}", e.getMessage());
            // 封装响应
            resDto.setCode(ResEnums.EXCEPTION.getCode());
            resDto.setMsg(ResEnums.EXCEPTION.getMsg() + ": " + e.getMessage());
        }
        return resDto;
    }
}
