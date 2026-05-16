package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.AbstractParameterMetaData;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulParameterMetaData extends AbstractParameterMetaData {

    RestfulParameterMetaData(Object[] parameters) {
        super(parameters);
    }
}
