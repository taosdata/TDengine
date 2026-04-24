import { TmqConfig } from "@src/tmq/config";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { testPassword, testUsername } from "@test-helpers/utils";
import { WS_SQL_ENDPOINT, WS_TMQ_ENDPOINT } from "@src/common/dsn";

describe("TmqConfig with dsn", () => {
    const baseDsn = "ws://localhost:6041";

    test("token field is null when CONNECT_TOKEN is not provided", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, baseDsn],
            [TMQConstants.CONNECT_USER, testUsername()],
            [TMQConstants.CONNECT_PASS, testPassword()],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.token).toBeNull();
    });

    test("token field is set when CONNECT_TOKEN is provided", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, baseDsn],
            [TMQConstants.CONNECT_TOKEN, "mytoken123"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.token).toBe("mytoken123");
    });

    test("writes token into dsn params for both tmq and sql dsn", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, baseDsn],
            [TMQConstants.CONNECT_TOKEN, "mytoken123"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.dsn?.params.get("bearer_token")).toBe("mytoken123");
        expect(cfg.sqlDsn?.params.get("bearer_token")).toBe("mytoken123");
    });

    test("parses multi-address dsn and keeps connector-level params", () => {
        const configMap = new Map([
            [
                TMQConstants.WS_URL,
                "ws://u:p@host1:6041,host2:6042/topicdb?retries=5&retry_backoff_ms=120"
            ],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.dsn?.addresses).toEqual([
            { host: "host1", port: 6041 },
            { host: "host2", port: 6042 },
        ]);
        expect(cfg.dsn?.endpoint).toBe(WS_TMQ_ENDPOINT);
        expect(cfg.sqlDsn?.endpoint).toBe(WS_SQL_ENDPOINT);
        expect(cfg.dsn?.params.get("retries")).toBe("5");
        expect(cfg.dsn?.params.get("retry_backoff_ms")).toBe("120");
    });

    test("applies CONNECT_USER and CONNECT_PASS overrides on top of dsn", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://raw_user:raw_pass@localhost:6041"],
            [TMQConstants.CONNECT_USER, "override_user"],
            [TMQConstants.CONNECT_PASS, "override_pass"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.user).toBe("override_user");
        expect(cfg.password).toBe("override_pass");
        expect(cfg.dsn?.username).toBe("override_user");
        expect(cfg.dsn?.password).toBe("override_pass");
        expect(cfg.sqlDsn?.username).toBe("override_user");
        expect(cfg.sqlDsn?.password).toBe("override_pass");
    });

    test("loads bearer_token from ws.url when CONNECT_TOKEN is absent", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://localhost:6041?bearer_token=url_token"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.token).toBe("url_token");
        expect(cfg.otherConfigs.get(TMQConstants.CONNECT_TOKEN)).toBe("url_token");
    });

    test("loads user_app and user_ip from config map", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, baseDsn],
            [TMQConstants.GROUP_ID, "g1"],
            [TMQConstants.USER_APP, "myApp"],
            [TMQConstants.USER_IP, "192.168.1.100"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.userApp).toBe("myApp");
        expect(cfg.userIp).toBe("192.168.1.100");
    });

    test("falls back to ws.url query user_app and user_ip when config map does not set them", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://localhost:6041?user_app=urlApp&user_ip=10.0.0.8"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.userApp).toBe("urlApp");
        expect(cfg.userIp).toBe("10.0.0.8");
    });

    test("keeps empty user_app and user_ip from config map without dsn fallback", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://localhost:6041?user_app=urlApp&user_ip=10.0.0.8"],
            [TMQConstants.GROUP_ID, "g1"],
            [TMQConstants.USER_APP, ""],
            [TMQConstants.USER_IP, ""],
        ]);
        const cfg = new TmqConfig(configMap);
        expect(cfg.userApp).toBe("");
        expect(cfg.userIp).toBe("");
    });

    test("CONNECT_TOKEN constant value is td.connect.token", () => {
        expect(TMQConstants.CONNECT_TOKEN).toBe("td.connect.token");
    });

    test("USER_APP and USER_IP constant values", () => {
        expect(TMQConstants.USER_APP).toBe("user_app");
        expect(TMQConstants.USER_IP).toBe("user_ip");
    });

    test("decode URL-encoded credentials for TMQ subscribe message", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://u%40ser:p%40ss@localhost:6041"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);

        const consumer = new (WsConsumer as any)(configMap);
        const msg = consumer.buildSubscribeMessage(["t1"], 1);

        expect(msg.args.user).toBe("u@ser");
        expect(msg.args.password).toBe("p@ss");
    });

    test("includes app and ip in TMQ subscribe message when configured", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://u:p@localhost:6041?user_app=urlApp&user_ip=10.0.0.8"],
            [TMQConstants.GROUP_ID, "g1"],
        ]);

        const consumer = new (WsConsumer as any)(configMap);
        const msg = consumer.buildSubscribeMessage(["t1"], 1);

        expect(msg.args.app).toBe("urlApp");
        expect(msg.args.ip).toBe("10.0.0.8");
    });

    test("omits app and ip in TMQ subscribe message when configured values are empty", () => {
        const configMap = new Map([
            [TMQConstants.WS_URL, "ws://u:p@localhost:6041"],
            [TMQConstants.GROUP_ID, "g1"],
            [TMQConstants.USER_APP, ""],
            [TMQConstants.USER_IP, ""],
        ]);

        const consumer = new (WsConsumer as any)(configMap);
        const msg = consumer.buildSubscribeMessage(["t1"], 1);

        expect(msg.args.app).toBeUndefined();
        expect(msg.args.ip).toBeUndefined();
    });
});
