import { ErrorCode, TaosError } from "../common/wsError";
import { StmtBindParams } from "./wsParamsBase";

export class TableInfo {
    name: Uint8Array | undefined | null;
    tags: StmtBindParams | undefined | null;
    params?: StmtBindParams;
    length: number = 0;
    textEncoder = new TextEncoder();

    constructor(name?: string) {
        if (name && name.length > 0) {
            this.name = this.textEncoder.encode(name);
            this.length = this.name.length;
        }
    }

    public getTableName(): Uint8Array | undefined | null {
        return this.name;
    }

    public getTableNameLength(): number {
        return this.length;
    }

    public getTags(): StmtBindParams | undefined | null {
        return this.tags;
    }

    public getParams(): StmtBindParams | undefined | null {
        return this.params;
    }

    public setTableName(name: string): void {
        if (name && name.length > 0) {
            this.name = this.textEncoder.encode(name);
            this.length = this.name.length;
        } else {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Table name is invalid!"
            );
        }
    }

    async setTags(paramsArray: StmtBindParams): Promise<void> {
        if (paramsArray) {
            this.tags = paramsArray;
        } else {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Table tags is invalid!"
            );
        }
    }

    async setParams(bindParams: StmtBindParams): Promise<void> {
        if (!this.params) {
            this.params = bindParams;
        } else {
            if (bindParams._fieldParams) {
                this.params.mergeParams(bindParams);
            }
        }
    }
}
