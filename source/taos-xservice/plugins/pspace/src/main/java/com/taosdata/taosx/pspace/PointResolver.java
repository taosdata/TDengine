package com.taosdata.taosx.pspace;

import com.sunwayland.pspace.PSpaceClient;
import com.taosdata.taosx.pspace.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.entity.PsTagForProps;
import com.sunwayland.pspace.entity.PsTagProp;
import com.sunwayland.pspace.enums.PsDataTypeEnum;
import com.sunwayland.pspace.enums.PsTagAnalogPropEnum;
import com.sunwayland.pspace.enums.interfaces.PsTagPropBaseInterface;

/**
 * Shared logic for resolving point IDs and point metadata for all run modes.
 * <p>
 * Implements the two-rule priority from {@code pspace-points.md}:
 * <ol>
 * <li>If {@code points.point_ids} is set → use directly.</li>
 * <li>Otherwise → query via {@link Points#load(Configuration)}.</li>
 * </ol>
 */
public final class PointResolver {

    private static final Logger logger = LoggerFactory.getLogger(PointResolver.class);

    /** Immutable result of point resolution. */
    public static class ResolvedPoints {
        private final List<Long> tagIds;
        private final Map<Long, String> nameMap; // tagId → point name
        private final Map<Long, String> typeMap; // tagId → pSpace data type (may be null)

        public ResolvedPoints(List<Point> points) {
            this.tagIds = points.stream().map(Point::getId).collect(Collectors.toList());
            this.nameMap = points.stream()
                    .collect(Collectors.toMap(Point::getId, p -> p.getName() != null ? p.getName() : ""));
            this.typeMap = points.stream()
                    .filter(p -> p.getDataType() != null)
                    .collect(Collectors.toMap(Point::getId, Point::getDataType, (a, b) -> a));
        }

        public List<Long> getTagIds() {
            return tagIds;
        }

        public Map<Long, String> getNameMap() {
            return nameMap;
        }

        public Map<Long, String> getTypeMap() {
            return typeMap;
        }

        public String getPointName(long tagId) {
            return nameMap.getOrDefault(tagId, String.valueOf(tagId));
        }

        public String getDataType(long tagId) {
            return typeMap.get(tagId);
        }

        public int size() {
            return tagIds.size();
        }
    }

    private PointResolver() {
    }

    /**
     * Resolve points according to the two-rule priority.
     *
     * @param cfg    full configuration (connection, nodes, points sections)
     * @param client connected pSpace client
     * @return resolved points with metadata
     */
    public static ResolvedPoints resolve(Configuration cfg, PSpaceClient client) throws Exception {
        // Rule 1: explicit point_ids from config
        if (cfg.getPoints() != null
                && cfg.getPoints().getPointIds() != null
                && !cfg.getPoints().getPointIds().isEmpty()) {

            List<Long> pointIds = cfg.getPoints().getPointIds();
            logger.info("Using {} point IDs from config (rule 1)", pointIds.size());

            // Query NAME and DATA_TYPE from pSpace for each point ID
            boolean includeDataType = cfg.getPoints().getIncludeDataType() != null
                    && cfg.getPoints().getIncludeDataType();

            List<PsTagPropBaseInterface> propsToQuery = new ArrayList<>();
            propsToQuery.add(PsTagAnalogPropEnum.NAME);
            propsToQuery.add(PsTagAnalogPropEnum.LONG_NAME);
            if (includeDataType) {
                propsToQuery.add(PsTagAnalogPropEnum.DATA_TYPE);
            }

            List<Point> points = pointIds.stream().map(id -> {
                Point p = new Point();
                p.setId(id);
                p.setName(String.valueOf(id)); // fallback
                return p;
            }).collect(Collectors.toList());

            try {
                PsResult<PsTagForProps> propsResult = client.tagGetTagListProps(pointIds, propsToQuery);
                if (propsResult.isSuccess() && propsResult.getData() != null) {
                    Map<Long, Point> pointMap = points.stream()
                            .collect(Collectors.toMap(Point::getId, p -> p));
                    for (PsTagForProps tp : propsResult.getData()) {
                        Point point = pointMap.get(tp.getId());
                        if (point == null)
                            continue;

                        for (PsTagProp prop : tp.getProps()) {
                            if (prop.getId() == PsTagAnalogPropEnum.NAME.getId()
                                    && prop.getValue() != null) {
                                point.setName(prop.getValue().toString());
                            } else if (prop.getId() == PsTagAnalogPropEnum.LONG_NAME.getId()
                                    && prop.getValue() != null) {
                                point.setLongName(prop.getValue().toString());
                            } else if (includeDataType
                                    && prop.getId() == PsTagAnalogPropEnum.DATA_TYPE.getId()
                                    && prop.getValue() != null) {
                                Object val = prop.getValue();
                                if (val instanceof Number) {
                                    PsDataTypeEnum dt = PsDataTypeEnum.getByCode(((Number) val).shortValue());
                                    if (dt != null) {
                                        point.setDataType(dt.getName());
                                    }
                                }
                            }
                        }
                    }
                    logger.info("Fetched properties for {} points from pSpace", propsResult.getData().size());
                } else {
                    logger.warn("Failed to fetch point properties, using tagId as name");
                }
            } catch (Exception e) {
                logger.warn("Error fetching point properties: {}, using tagId as name", e.getMessage());
            }

            return new ResolvedPoints(points);
        }

        // Rule 2: query points from pSpace via node tree
        // Reuse the existing client to avoid creating a second connection,
        // which could interfere with PSpaceClient SDK's internal state.
        logger.info("Querying points from pSpace (rule 2)");
        List<Point> points = Points.load(cfg, client);
        if (points == null || points.isEmpty()) {
            throw new Exception("No points found from pSpace (check nodes.root and points config)");
        }
        logger.info("Resolved {} points from pSpace", points.size());
        return new ResolvedPoints(points);
    }
}
