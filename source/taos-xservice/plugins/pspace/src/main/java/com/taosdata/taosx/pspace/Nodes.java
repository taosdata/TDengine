package com.taosdata.taosx.pspace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunwayland.pspace.PSpaceClient;
import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.entity.PsTagBase;
import com.sunwayland.pspace.entity.dto.PsTagQueryFilter;
import com.sunwayland.pspace.enums.PsTagAnalogPropEnum;
import com.sunwayland.pspace.enums.PsTagTypeEnum;
import com.sunwayland.pspace.enums.interfaces.PsTagPropBaseInterface;
import com.taosdata.taosx.pspace.config.Configuration;

public class Nodes {
    private static final Logger logger = LoggerFactory.getLogger(Nodes.class);

    public static List<Node> load(Configuration config) throws Exception {
        logger.info("run pSpace nodes");

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
        // connect to pSpace
        PSpaceClient client = config.tryConnect();

        return getChildren(root, client);
    }

    private static List<Node> getChildren(Long root, PSpaceClient client) throws Exception {
        List<Node> nodes = new ArrayList<>();

        PsTagQueryFilter psTagQueryFilter = new PsTagQueryFilter();
        psTagQueryFilter.setRootTagId(root);
        psTagQueryFilter.setQueryLevel((short) 1);
        psTagQueryFilter.setQuerySelf(false);

        List<PsTagPropBaseInterface> psTagPropBaseInterfaces = Arrays.asList(
                PsTagAnalogPropEnum.TAG_ID,
                PsTagAnalogPropEnum.TAG_TYPE,
                PsTagAnalogPropEnum.NAME,
                PsTagAnalogPropEnum.LONG_NAME,
                PsTagAnalogPropEnum.DESCRIPTION);

        PsResult<PsTagBase> psResult = client.tagQuery(psTagQueryFilter, psTagPropBaseInterfaces);
        if (psResult.isSuccess()) {
            for (PsTagBase t : psResult.getData()) {
                // 仅返回类型为节点（PS_NODE）的 Tag
                if (t.getTagType() != PsTagTypeEnum.PS_NODE)
                    continue;

                Node node = new Node();
                node.setId(t.getTagId());
                node.setName(t.getName());
                node.setLongName(t.getLongName());
                boolean hasChildNode = hasChildNode(t.getTagId(), client);
                node.setIsLeaf(!hasChildNode);

                nodes.add(node);
            }
        } else {
            throw new Exception("Failed to get children nodes: " + psResult.toString());
        }

        return nodes;
    }

    /**
     * 判断给定 Tag 是否还有子节点（仅判断子层级中是否存在 PS_NODE 类型）。
     */
    private static boolean hasChildNode(Long tagId, PSpaceClient client) throws Exception {
        PsTagQueryFilter f = new PsTagQueryFilter();
        f.setRootTagId(tagId);
        f.setQueryLevel((short) 1); // 只查一层子节点
        f.setQuerySelf(false);

        List<PsTagPropBaseInterface> props = Arrays.asList(
                PsTagAnalogPropEnum.TAG_ID,
                PsTagAnalogPropEnum.TAG_TYPE);

        PsResult<PsTagBase> r = client.tagQuery(f, props);
        if (r.isSuccess()) {
            return r.getData().stream().anyMatch(child -> child.getTagType() == PsTagTypeEnum.PS_NODE);
        } else {
            throw new Exception("Failed to query child nodes: " + r.toString());
        }
    }

}
