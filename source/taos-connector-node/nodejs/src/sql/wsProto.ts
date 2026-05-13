export interface SchemalessMessageInfo {
    action: string;
    args: SchemalessParamsInfo;
}

export interface SchemalessParamsInfo {
    req_id?: number | undefined | null;
    protocol: number;
    ttl?: number;
    precision: string;
    data: string;
}

export enum Precision {
    NOT_CONFIGURED = "",
    HOURS = "h",
    MINUTES = "m",
    SECONDS = "s",
    MILLI_SECONDS = "ms",
    MICRO_SECONDS = "u",
    NANO_SECONDS = "ns",
}

export enum SchemalessProto {
    InfluxDBLineProtocol = 1,
    OpenTSDBTelnetLineProtocol = 2,
    OpenTSDBJsonFormatProtocol = 3,
}
