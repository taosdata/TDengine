package com.taosdata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.caches.MetricCache;
import com.taosdata.config.OpentsdbConfig;
import com.taosdata.model.entity.OpentsdbDataEntity;
import com.taosdata.model.entity.OpentsdbDataPointEntity;
import com.taosdata.model.entity.OpentsdbMetricEntity;
import com.taosdata.model.enums.ResEnums;
import com.taosdata.service.OpentsdbService;
import com.taosdata.utils.HttpUtils;
import com.taosdata.utils.exception.ArtificialException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * OpenTSDB数据库操作服务实现类（OpenTSDB目前无身份验证和访问控制系统）
 *
 * @author ZYP
 */
@Service
public class OpentsdbServiceImpl implements OpentsdbService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private OpentsdbConfig opentsdbConfig;

    /**
     * 查询所有metric列表
     *
     * @param url
     * @return
     * @throws ArtificialException
     */
    @Override
    public JSONArray fetchMetricList(@Nullable String url) throws ArtificialException {
        // 参数不存在则使用配置
        if (StringUtils.isEmpty(url)) {
            url = opentsdbConfig.getUrl();
        }
        // 拼接请求url
        if (url.endsWith("/")) {
            url += opentsdbConfig.getApiMetrics();
        } else {
            url += "/" + opentsdbConfig.getApiMetrics();
        }
        // 请求参数
        String params = "type=metrics&max=" + Integer.MAX_VALUE;
        try {
            // 获取结果并解析为JSONArray
            return JSONArray.parseArray(HttpUtils.sendGet(url, params));
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        }
    }

    /**
     * 获取opentsdb中指定metric与时间段的数据
     *
     * @param url
     * @param metric
     * @param startTime
     * @param stopTime
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<OpentsdbDataEntity> fetchData(@Nullable String url, String metric, String startTime, String stopTime) throws ArtificialException {
        // 参数不存在则使用配置
        if (StringUtils.isEmpty(url)) {
            url = opentsdbConfig.getUrl();
        }
        // 拼接请求url
        if (url.endsWith("/")) {
            url += opentsdbConfig.getApiData();
        } else {
            url += "/" + opentsdbConfig.getApiData();
        }
        // 请求参数 http://opentsdb.net/docs/build/html/api_http/query/index.html
        JSONObject query = new JSONObject();
        query.put("aggregator", "none");
        query.put("metric", metric);
        // query.put("rate", false);
        // query.put("rateOptions", Map);
        // query.put("downsample", "");
        // query.put("tags", Map);
        // query.put("filters", List);
        // query.put("explicitTags", false);
        // query.put("percentiles", List);
        // query.put("rollupUsage", "");

        JSONArray queries = new JSONArray();
        queries.add(query);

        JSONObject params = new JSONObject();
        params.put("start", startTime);
        params.put("end", stopTime);
        params.put("queries", queries);
        // params.put("noAnnotations", false);
        // params.put("globalAnnotations", false);
        params.put("msResolution", true);
        // params.put("showTSUIDs", false);
        // params.put("showSummary", false);
        // params.put("showStats", false);
        // params.put("showQuery", false);
        // params.put("delete", false);
        // params.put("timezone", "UTC");
        // params.put("useCalendar", false);
        try {
            // 返回列表
            List<OpentsdbDataEntity> opentsdbDataEntityList = new ArrayList<>();
            // 获取内存中的表结构
            OpentsdbMetricEntity opentsdbMetricEntity = MetricCache.metricMap.get(metric);
            // 获取结果并解析为JSONArray
            JSONArray jsonArray = JSONArray.parseArray(HttpUtils.sendPostJson(url, params.toJSONString()));
            // 遍历结果集进行封装
            for (Object object : jsonArray) {
                try {
                    if (object instanceof JSONObject) {
                        OpentsdbDataEntity opentsdbDataEntity = new OpentsdbDataEntity();
                        opentsdbDataEntity.setAggregateTags(new HashMap<>());
                        opentsdbDataEntity.setTags(new HashMap<>());
                        opentsdbDataEntity.setDps(new ArrayList<>());
                        // 转换为JSON实体类
                        JSONObject opentsdbDataObject = (JSONObject) object;
                        // 获取metric
                        opentsdbDataEntity.setMetric(opentsdbDataObject.getString("metric"));
                        // 获取aggregateTags
                        // JSONArray aggregateTags = opentsdbDataObject.getJSONArray("aggregateTags");
                        // 获取tags
                        JSONObject tags = opentsdbDataObject.getJSONObject("tags");
                        for (String tagKey : tags.keySet()) {
                            opentsdbDataEntity.getTags().put(tagKey, tags.get(tagKey));
                            // 如果存在新增字段，需要更新缓存
                            if (!opentsdbMetricEntity.getTagSet().contains(tagKey)) {
                                // 更新缓存
                                opentsdbMetricEntity.getTagSet().add(tagKey);
                            }
                        }
                        // 获取dps
                        JSONObject dps = opentsdbDataObject.getJSONObject("dps");
                        for (String timestamp : dps.keySet()) {
                            try {
                                OpentsdbDataPointEntity opentsdbDataPointEntity = new OpentsdbDataPointEntity();
                                opentsdbDataPointEntity.setTimestamp(Long.parseLong(timestamp));
                                opentsdbDataPointEntity.setValue(dps.get(timestamp));
                                opentsdbDataEntity.getDps().add(opentsdbDataPointEntity);
                            } catch (Exception e) {
                                logger.error("Failed to parse data point: {}:{}", opentsdbDataObject.getString("metric"), timestamp, e);
                            }
                        }
                        // 设置表结构
                        opentsdbDataEntity.setOpentsdbMetricEntity(opentsdbMetricEntity);
                        // 判断data point是否空
                        if (opentsdbDataEntity.getDps().size() > 0) {
                            // 放入列表
                            opentsdbDataEntityList.add(opentsdbDataEntity);
                        }
                    } else {
                        // 无效数据
                        logger.error("Failed to parse data, not legal JSON data: {}", object);
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse data: {}", object, e);
                }
            }
            return opentsdbDataEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        }
    }

    /**
     * 异常处理
     *
     * @param e
     */
    private void handlerException(Exception e) {
        String errMsg = e.getMessage();
        if (StringUtils.isNotEmpty(errMsg) && (errMsg.contains("Connection timed out") || errMsg.contains("Connection refused"))) {
            // url错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(101);
        } else if (StringUtils.isNotEmpty(errMsg) && (errMsg.contains("Unsupported or unrecognized SSL message") || errMsg.contains("The plain HTTP request was sent to HTTPS port"))) {
            // 协议错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(102);
        } else if (StringUtils.isNotEmpty(errMsg) && errMsg.contains("Bad Request")) {
            // 请求格式错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(103);
        }
    }
}
