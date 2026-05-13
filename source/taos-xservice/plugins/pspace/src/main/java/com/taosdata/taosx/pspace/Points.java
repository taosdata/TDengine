package com.taosdata.taosx.pspace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunwayland.pspace.PSpaceClient;
import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.entity.PsTagBase;
import com.sunwayland.pspace.entity.PsTagForProps;
import com.sunwayland.pspace.entity.PsTagProp;
import com.sunwayland.pspace.structs.StructPropValue;
import com.sunwayland.pspace.entity.dto.PsTagQueryFilter;
import com.sunwayland.pspace.enums.PsDataTypeEnum;
import com.sunwayland.pspace.enums.PsTagAnalogPropEnum;
import com.sunwayland.pspace.enums.PsTagTypeEnum;
import com.sunwayland.pspace.enums.interfaces.PsTagPropBaseInterface;
import com.taosdata.taosx.pspace.config.Configuration;
import com.taosdata.taosx.pspace.config.PointsConfig;

public class Points {
    private static final Logger logger = LoggerFactory.getLogger(Points.class);

    public static List<Point> load(Configuration config) throws Exception {
        return load(config, null);
    }

    /**
     * Load points, reusing an existing PSpaceClient connection if provided.
     *
     * @param config         existing configuration
     * @param existingClient optional pre-connected client (avoids creating a second
     *                       connection)
     */
    public static List<Point> load(Configuration config, PSpaceClient existingClient) throws Exception {
        logger.info("run pSpace points");

        if (config == null) {
            logger.error("No configuration provided.");
            throw new Exception("No configuration provided");
        }
        if (config.getNodes() == null) {
            logger.error("No nodes configuration found.");
            throw new Exception("No nodes configuration found");
        }

        // get root node id
        Long root = config.getNodes().getRoot();

        // get name_filter from [points] config
        Optional<String> nameFilter = Optional.empty();
        boolean includeDataType = false;
        PointsConfig pointsConfig = config.getPoints();
        if (pointsConfig != null) {
            if (pointsConfig.getNameFilter() != null && !pointsConfig.getNameFilter().isEmpty()) {
                nameFilter = Optional.of(pointsConfig.getNameFilter());
                logger.info("Using name_filter: {}", nameFilter.get());
            }
            if (pointsConfig.getIncludeDataType() != null && pointsConfig.getIncludeDataType()) {
                includeDataType = true;
                logger.info("Will include DATA_TYPE for each point");
            }
        }

        // Reuse existing client or create a new connection
        PSpaceClient client = existingClient != null ? existingClient : config.tryConnect();

        return getPoints(root, nameFilter, includeDataType, client);
    }

    private static List<Point> getPoints(Long root, Optional<String> nameFilter,
            boolean includeDataType, PSpaceClient client) throws Exception {
        List<Point> points = new ArrayList<>();

        PsTagQueryFilter psTagQueryFilter = new PsTagQueryFilter();
        psTagQueryFilter.setRootTagId(root);
        psTagQueryFilter.setQueryLevel((short) 0); // 所有层级
        psTagQueryFilter.setQuerySelf(true);

        if (nameFilter.isPresent()) {
            List<StructPropValue> propFilter = Arrays.asList(
                    new StructPropValue(PsTagAnalogPropEnum.LONG_NAME, nameFilter.get()));
            psTagQueryFilter.setFilterProps(propFilter);
        }

        List<PsTagPropBaseInterface> psTagPropBaseInterfaces = Arrays.asList(
                PsTagAnalogPropEnum.TAG_ID,
                PsTagAnalogPropEnum.TAG_TYPE,
                PsTagAnalogPropEnum.NAME,
                PsTagAnalogPropEnum.LONG_NAME,
                PsTagAnalogPropEnum.DESCRIPTION);

        PsResult<PsTagBase> psResult = client.tagQuery(psTagQueryFilter, psTagPropBaseInterfaces);
        if (psResult.isSuccess()) {
            Map<Long, Point> pointMap = new HashMap<>();
            for (PsTagBase t : psResult.getData()) {
                if (t.getTagType() == PsTagTypeEnum.PS_NODE)
                    continue;

                Point point = new Point();
                point.setId(t.getTagId());
                point.setName(t.getName());
                point.setLongName(t.getLongName());
                point.setType(t.getTagType().name());
                point.setDesc(t.getDescription());

                points.add(point);
                pointMap.put(t.getTagId(), point);
            }

            // batch fetch DATA_TYPE for all points
            if (includeDataType && !points.isEmpty()) {
                List<Long> tagIds = new ArrayList<>(pointMap.keySet());
                logger.info("fetching DATA_TYPE for {} points", tagIds.size());

                List<PsTagPropBaseInterface> dataTypeProps = Arrays.asList(
                        PsTagAnalogPropEnum.DATA_TYPE);
                PsResult<PsTagForProps> propsResult = client.tagGetTagListProps(tagIds,
                        dataTypeProps);
                if (propsResult.isSuccess()) {
                    for (PsTagForProps tp : propsResult.getData()) {
                        Point point = pointMap.get(tp.getId());
                        if (point == null)
                            continue;

                        Optional<PsTagProp> dataTypeProp = tp.getProps().stream()
                                .filter(p -> p.getId() == PsTagAnalogPropEnum.DATA_TYPE.getId())
                                .findFirst();

                        if (dataTypeProp.isPresent() && dataTypeProp.get().getValue() != null) {
                            Object val = dataTypeProp.get().getValue();
                            if (val instanceof Number) {
                                PsDataTypeEnum dt = PsDataTypeEnum.getByCode(((Number) val).shortValue());
                                if (dt != null) {
                                    point.setDataType(dt.getName());
                                }
                            }
                        }
                    }
                    logger.info("successfully fetched DATA_TYPE for points");
                } else {
                    logger.warn("failed to fetch DATA_TYPE: {}", propsResult.toString());
                }
            }
        } else {
            throw new Exception("Failed to get children points: " + psResult.toString());
        }

        logger.info("get {} points", points.size());

        return points;
    }

}
