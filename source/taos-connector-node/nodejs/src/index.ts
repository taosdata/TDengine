import { WsSql } from "./sql/wsSql";
import { WSConfig } from "./common/config";
import { WsConsumer } from "./tmq/wsTmq";
import logger, { setLevel } from "./common/log";
import { WebSocketConnectionPool } from "./client/wsConnectorPool";

let sqlConnect = async (conf: WSConfig) => {
    try {
        return await WsSql.open(conf);
    } catch (err: any) {
        logger.error(err);
        throw err;
    }
};

let tmqConnect = async (configMap: Map<string, string>) => {
    try {
        return await WsConsumer.newConsumer(configMap);
    } catch (err: any) {
        logger.error(err);
        throw err;
    }
};

let setLogLevel = (level: string) => {
    setLevel(level);
};

let destroy = () => {
    WebSocketConnectionPool.instance().destroyed();
};

export { sqlConnect, tmqConnect, setLogLevel, destroy };
