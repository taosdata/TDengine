import { WsClient } from "@src/client/wsClient";
import { TSDB_OPTION_CONNECTION } from "@src/common/constant";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { Sleep, testNon3360, testPassword, testUsername } from "@test-helpers/utils";

function mockOpenDependencies() {
    jest.spyOn(WsClient.prototype, "connect").mockResolvedValue(undefined);
    jest.spyOn(WsClient.prototype, "checkVersion").mockResolvedValue(undefined);
    jest.spyOn(WsSql.prototype, "exec").mockResolvedValue({} as any);
    jest.spyOn(WsClient.prototype, "close").mockResolvedValue(undefined);
    return jest
        .spyOn(WsClient.prototype, "setOptionConnection")
        .mockResolvedValue(undefined);
}

describe("WsSql.open connection options", () => {
    afterEach(() => {
        jest.restoreAllMocks();
    });

    test("sets user_app and user_ip when provided", async () => {
        const setOptionConnectionSpy = mockOpenDependencies();
        const config = new WSConfig("ws://root:taosdata@localhost:6041");
        config.setUserApp("myApp");
        config.setUserIp("192.168.1.100");

        const wsSql = await WsSql.open(config);
        await wsSql.close();

        expect(setOptionConnectionSpy).toHaveBeenCalledWith(
            TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP,
            "myApp"
        );
        expect(setOptionConnectionSpy).toHaveBeenCalledWith(
            TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP,
            "192.168.1.100"
        );
    });

    test("clears user_app and user_ip when not provided", async () => {
        const setOptionConnectionSpy = mockOpenDependencies();
        const config = new WSConfig("ws://root:taosdata@localhost:6041");

        const wsSql = await WsSql.open(config);
        await wsSql.close();

        expect(setOptionConnectionSpy).toHaveBeenCalledWith(
            TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP,
            null
        );
        expect(setOptionConnectionSpy).toHaveBeenCalledWith(
            TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP,
            null
        );
    });
});

describe("WsSql.open connection options integration", () => {
    testNon3360("applies dsn user_app and user_ip to server connection info", async () => {
        const expectedUserIp = "192.168.1.1";
        const expectedUserApp = "node_test";
        const dsn =
            `ws://${testUsername()}:${testPassword()}` +
            `@localhost:6041?user_ip=${expectedUserIp}&user_app=${expectedUserApp}`;
        const config = new WSConfig(dsn);
        const wsSql = await WsSql.open(config);
        let wsRows;

        try {
            await Sleep(2000);
            wsRows = await wsSql.query("show connections");

            const meta = wsRows.getMeta() || [];
            const userIpIndex = meta.findIndex((field) => field.name === "user_ip");
            const userAppIndex = meta.findIndex((field) => field.name === "user_app");

            expect(userIpIndex).toBeGreaterThanOrEqual(0);
            expect(userAppIndex).toBeGreaterThanOrEqual(0);

            let found = false;
            while (await wsRows.next()) {
                const row = wsRows.getData();
                if (!Array.isArray(row)) {
                    continue;
                }
                if (
                    row[userIpIndex] === expectedUserIp &&
                    row[userAppIndex] === expectedUserApp
                ) {
                    found = true;
                    break;
                }
            }

            expect(found).toBe(true);
        } finally {
            if (wsRows) {
                await wsRows.close();
            }
            await wsSql.close();
        }
    });
});
