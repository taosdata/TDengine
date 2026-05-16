import { WSRows } from "../sql/wsRows";
import { StmtBindParams } from "./wsParamsBase";

export interface WsStmt {
    getStmtId(): bigint | undefined | null;
    prepare(sql: string): Promise<void>;
    setTableName(tableName: string): Promise<void>;
    setTags(paramsArray: StmtBindParams): Promise<void>;
    newStmtParam(): StmtBindParams;
    bind(paramsArray: StmtBindParams): Promise<void>;
    batch(): Promise<void>;
    exec(): Promise<void>;
    resultSet(): Promise<WSRows>;
    getLastAffected(): number | null | undefined;
    close(): Promise<void>;
}
