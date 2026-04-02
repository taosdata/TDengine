import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.type.Aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiValuedPointTester {
    public static void main(String[] args) {
        MultiValuedPointTester tester = new MultiValuedPointTester();
//        tester.printMultiValuedPointWriteRequestBody();
        tester.printMultiValuedPointReadRequest();
    }

    public void printMultiValuedPointWriteRequestBody() {
        int numOfPoints = 1;
        List<MultiValuedPoint> mvPoints = new ArrayList<>(numOfPoints);
        for (int i = 0; i < numOfPoints; i++) {
            MultiValuedPoint multiValuedPoint = MultiValuedPoint.metric("metric1", "wind")
                    .fields("speed", 30.58)
                    .fields("direction", "Northwest")
                    .fields("level", 5)
                    .fields("description", "Fresh breeze")
                    .tag("sensor", "6A04-1802-23FE")
                    .tag("city", "hangzhou")
                    .tag("province", "zhejiang")
                    .timestamp(1532934538L)
                    .build();
            mvPoints.add(multiValuedPoint);
        }


        List<Point> singleValuedPoints = new ArrayList<Point>();
        for (MultiValuedPoint mvPoint : mvPoints) {
            for (Map.Entry<String, Object> field : mvPoint.getFields().entrySet()) {
                Point singleValuedPoint = Point.metric(field.getKey())
                        .tag(mvPoint.getTags())
                        .timestamp(mvPoint.getTimestamp())
                        .value(field.getValue())
                        .build();
                singleValuedPoints.add(singleValuedPoint);
            }
        }

        String jsonString = JSON.toJSONString(singleValuedPoints, SerializerFeature.DisableCircularReferenceDetect);
        System.out.println(singleValuedPoints.toString());
        System.out.println(jsonString);
    }

    public void printMultiValuedPointReadRequest() {
        List<MultiValuedQueryMetricDetails> metricDetails = new ArrayList<>();
        MultiValuedQueryMetricDetails metricDetail_1 = MultiValuedQueryMetricDetails.field("speed").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_1);
        MultiValuedQueryMetricDetails metricDetail_2 = MultiValuedQueryMetricDetails.field("direction").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_2);
        MultiValuedQueryMetricDetails metricDetail_3 = MultiValuedQueryMetricDetails.field("level").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_3);
        MultiValuedQueryMetricDetails metricDetail_4 = MultiValuedQueryMetricDetails.field("description").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_4);
        // Query without limit and offset
        MultiValuedSubQuery subQuery = MultiValuedSubQuery.metric("metric1", "wind")
                .fieldsInfo(metricDetails)
                .tag("city", "hangzhou")
                .limit(10).offset(2)
                .build();
        MultiValuedQuery multiValuedQuery = MultiValuedQuery.start(1532934338).end(1532934538).sub(subQuery).build();

        if (multiValuedQuery.getQueries().size() != 1) {
            System.out.println("Sorry. SDK does not support multiple multi-valued sub queries for now.");
        }

        List<SubQuery> singleValuedSubQueries = new ArrayList<SubQuery>();
        Map<String, String> fieldAndDpFilter = new HashMap<String, String>();
        long startTime = multiValuedQuery.getStart();
        long endTime = multiValuedQuery.getEnd();
        for (MultiValuedSubQuery subQuery1 : multiValuedQuery.getQueries()) {
            for (MultiValuedQueryMetricDetails metricDetails1 : subQuery1.getFieldsInfo()) {
                if(metricDetails1.getDpValue() != null && !metricDetails1.getDpValue().isEmpty()) {
                    fieldAndDpFilter.put(metricDetails1.getField(), metricDetails1.getDpValue());
                }
                SubQuery singleValuedSubQuery = SubQuery.metric(metricDetails1.getField()).aggregator(metricDetails1.getAggregatorType())
                        .tag(subQuery.getTags())
                        .downsample(metricDetails1.getDownsample())
                        .rate(metricDetails1.getRate())
                        .dpValue(metricDetails1.getDpValue())
                        .build();
                singleValuedSubQueries.add(singleValuedSubQuery);
            }
        }
        Query query = Query.timeRange(startTime, endTime).sub(singleValuedSubQueries).build();
        System.out.println(query.toJSON());
    }
}
