import { WSConfig } from "@src/common/config";
import { getDsn } from "@src/common/utils";
import { WS_SQL_ENDPOINT } from "@src/common/dsn";

describe("WSConfig to Dsn conversion", () => {
    test("parses multi-address dsn and keeps connector-level params", () => {
        const conf = new WSConfig(
            "ws://root:taosdata@host1:6041,host2:6042/mydb?retries=5&retry_backoff_ms=120&timezone=UTC"
        );

        const dsn = getDsn(conf);
        expect(dsn.scheme).toBe("ws");
        expect(dsn.addresses).toEqual([
            { host: "host1", port: 6041 },
            { host: "host2", port: 6042 },
        ]);
        expect(dsn.database).toBe("mydb");
        expect(dsn.endpoint).toBe(WS_SQL_ENDPOINT);
        expect(dsn.params.get("retries")).toBe("5");
        expect(dsn.params.get("retry_backoff_ms")).toBe("120");
        expect(dsn.params.get("timezone")).toBe("UTC");

        expect(conf.getDb()).toBe("mydb");
        expect(conf.getTimezone()).toBe("UTC");
    });

    test("applies WSConfig overrides on top of dsn", () => {
        const conf = new WSConfig("ws://root:taosdata@host1:6041/mydb");
        conf.setUser("admin");
        conf.setPwd("secret");
        conf.setToken("token-1");
        conf.setBearerToken("bearer-1");
        conf.setTimezone("Asia/Shanghai");
        conf.setUserApp("app-from-setter");
        conf.setUserIp("192.168.1.100");
        conf.setDb("override_db");

        const dsn = getDsn(conf);
        expect(dsn.username).toBe("admin");
        expect(dsn.password).toBe("secret");
        expect(dsn.database).toBe("override_db");
        expect(dsn.params.get("token")).toBe("token-1");
        expect(dsn.params.get("bearer_token")).toBe("bearer-1");
        expect(dsn.params.get("timezone")).toBe("Asia/Shanghai");
        expect(dsn.params.get("user_app")).toBe("app-from-setter");
        expect(dsn.params.get("user_ip")).toBe("192.168.1.100");
    });

    test("syncs user_app and user_ip between dsn params and WSConfig", () => {
        const conf = new WSConfig(
            "ws://root:taosdata@host1:6041/mydb?user_app=url-app&user_ip=10.0.0.8"
        );
        const dsnFromUrl = getDsn(conf);
        expect(dsnFromUrl.params.get("user_app")).toBe("url-app");
        expect(dsnFromUrl.params.get("user_ip")).toBe("10.0.0.8");
        expect(conf.getUserApp()).toBe("url-app");
        expect(conf.getUserIp()).toBe("10.0.0.8");

        conf.setUserApp("setter-app");
        conf.setUserIp("172.16.1.9");
        const dsnFromSetter = getDsn(conf);
        expect(dsnFromSetter.params.get("user_app")).toBe("setter-app");
        expect(dsnFromSetter.params.get("user_ip")).toBe("172.16.1.9");
    });
});
