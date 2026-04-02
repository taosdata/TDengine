import { TmqConfig } from "../tmq/config";
import { WSConfig } from "./config";
import { Dsn, parse } from "./dsn";
import { ErrorCode, TDWebSocketClientError } from "./wsError";

export function getDsn(wsConfig: WSConfig): Dsn {
    const dsn = parse(wsConfig.getUrl());

    const user = wsConfig.getUser();
    if (user) {
        dsn.username = user;
    }

    const password = wsConfig.getPwd();
    if (password) {
        dsn.password = password;
    }

    const token = wsConfig.getToken();
    if (token) {
        dsn.params.set("token", token);
    } else if (dsn.params.has("token")) {
        wsConfig.setToken(dsn.params.get("token") || "");
    }

    const bearerToken = wsConfig.getBearerToken();
    if (bearerToken) {
        dsn.params.set("bearer_token", bearerToken);
    } else if (dsn.params.has("bearer_token")) {
        wsConfig.setBearerToken(dsn.params.get("bearer_token") || "");
    }

    const timezone = wsConfig.getTimezone();
    if (timezone) {
        dsn.params.set("timezone", timezone);
    } else if (dsn.params.has("timezone")) {
        wsConfig.setTimezone(dsn.params.get("timezone") || "");
    }

    const db = wsConfig.getDb();
    if (db && db.length > 0) {
        dsn.database = db;
    } else if (dsn.database.length > 0) {
        wsConfig.setDb(dsn.database);
    }

    return dsn;
}

export function isEmpty(value: any): boolean {
    if (value === null || value === undefined) return true;
    if (Array.isArray(value) && value.length === 0) return true;
    return false;
}

export function getBinarySql(
    action: bigint,
    reqId: bigint,
    resultId: bigint,
    sql?: string
): ArrayBuffer {
    if (sql) {
        const encoder = new TextEncoder();
        const buffer = encoder.encode(sql);
        let messageLen = 30 + buffer.length;
        let sqlBuffer = new ArrayBuffer(messageLen);
        let sqlView = new DataView(sqlBuffer);
        sqlView.setBigUint64(0, reqId, true);
        sqlView.setBigInt64(8, resultId, true);
        sqlView.setBigInt64(16, action, true);
        sqlView.setInt16(24, 1, true);
        sqlView.setInt32(26, buffer.length, true);
        let offset = 30;
        for (let i = 0; i < buffer.length; i++) {
            sqlView.setUint8(offset + i, buffer[i]);
        }
        return sqlBuffer;
    }

    let messageLen = 26;
    let sqlBuffer = new ArrayBuffer(messageLen);
    let sqlView = new DataView(sqlBuffer);
    sqlView.setBigUint64(0, reqId, true);
    sqlView.setBigInt64(8, resultId, true);
    sqlView.setBigInt64(16, action, true);
    sqlView.setInt16(24, 1, true);
    return sqlBuffer;
}

export function zigzagDecode(n: number): number {
    return (n >> 1) ^ -(n & 1);
}

export function safeDecodeURIComponent(str: string) {
    // Replace invalid "%" not followed by two hex characters with "%25"
    const cleaned = str.replace(/%(?![0-9A-Fa-f]{2})/g, "%25");
    try {
        return decodeURIComponent(cleaned);
    } catch (e) {
        throw new TDWebSocketClientError(
            ErrorCode.ERR_INVALID_URL,
            `Decoding ${str} error: ${e}`
        );
    }
}

/**
 * compare two semantic version numbers
 * @param v1 (e.g., "3.3.6.3-alpha")
 * @param v2 (e.g., "3.3.6.2")
 * @returns
 *   1 -> v1 > v2
 *   -1 -> v1 < v2
 *   0 -> v1 === v2
 */
export function compareVersions(v1: string, v2: string): number {
    // analyze the core part of the version number and pre release tags
    const [main1, pre1] = splitVersion(v1);
    const [main2, pre2] = splitVersion(v2);

    // compare the main version number section
    const mainComparison = compareMainVersions(main1, main2);
    if (mainComparison !== 0) return mainComparison;

    // comparing pre release tags with the same main version
    return comparePreReleases(pre1, pre2);
}

/**
 * Split version number into main version and pre release tags
 */
function splitVersion(version: string): [number[], string | null] {
    // split main version and pre release tags
    const parts = version.split("-");
    const main = parts[0];
    const prerelease = parts.length > 1 ? parts[1] : null;

    // split the main version into a numerical array
    const mainParts = main.split(".").map(Number);

    return [mainParts, prerelease];
}

/**
 * compare the main version number section
 */
function compareMainVersions(v1: number[], v2: number[]): number {
    const maxLength = Math.max(v1.length, v2.length);

    for (let i = 0; i < maxLength; i++) {
        // if partially missing, it is considered as 0
        const part1 = v1[i] || 0;
        const part2 = v2[i] || 0;

        if (part1 > part2) return 1;
        if (part1 < part2) return -1;
    }

    return 0;
}

/**
 * compare pre release tags
 */
function comparePreReleases(pre1: string | null, pre2: string | null): number {
    // both have no pre release tags → equal
    if (pre1 === null && pre2 === null) return 0;

    // versions with pre release tags have lower priority
    if (pre1 === null) return 1; // v1 is stable > v2
    if (pre2 === null) return -1; // v2 is stable > v1

    // compare pre release tag strings
    return pre1.localeCompare(pre2);
}

export function decimalToString(
    valueStr: string,
    fields_scale: bigint | null
): string {
    let decimalStr = valueStr;
    if (fields_scale && fields_scale > 0) {
        const scale = Number(fields_scale);
        const isNegative = decimalStr.startsWith("-");
        const absStr = isNegative ? decimalStr.slice(1) : decimalStr;

        if (absStr.length <= scale) {
            const paddedStr = absStr.padStart(scale + 1, "0");
            decimalStr = (isNegative ? "-" : "") + "0." + paddedStr.slice(1);
        } else {
            const integerPart = absStr.slice(0, absStr.length - scale);
            const decimalPart = absStr.slice(absStr.length - scale);
            decimalStr =
                (isNegative ? "-" : "") + integerPart + "." + decimalPart;
        }
    }
    return decimalStr;
}

const SENSITIVE_FIELD_REGEX =
    /("(?:password|token|bearer_token|td\.connect\.token)"\s*:\s*)"([^"\\]*(?:\\.[^"\\]*)*)"/g;

export function maskSensitiveForLog(message: string): string {
    return message.replace(SENSITIVE_FIELD_REGEX, '$1"[REDACTED]"');
}

export function maskUrlForLog(url: URL | null): string {
    if (!url) {
        return "";
    }

    const masked = new URL(url.toString());
    masked.password = "[REDACTED]";
    if (masked.searchParams.has("token")) {
        masked.searchParams.set("token", "[REDACTED]");
    }
    if (masked.searchParams.has("bearer_token")) {
        masked.searchParams.set("bearer_token", "[REDACTED]");
    }
    return masked.toString().replace(/%5BREDACTED%5D/g, "[REDACTED]");
}

export function maskTmqConfigForLog(config: TmqConfig): string {
    const masked = {
        ...config,
        otherConfigs: Object.fromEntries(config.otherConfigs)
    };
    return JSON.stringify(masked, (key, value) => {
        switch (key) {
            case 'dsn':
            case 'sqlDsn':
                return value ? value.toString() : value;
            case 'token':
            case 'password':
            case 'bearer_token':
            case 'td.connect.token':
                return value ? '[REDACTED]' : value;
            default:
                return value;
        }
    });
}

