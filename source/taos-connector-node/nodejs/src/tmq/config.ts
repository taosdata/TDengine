import { Dsn, parse, WS_TMQ_ENDPOINT } from "../common/dsn";
import { TMQConstants } from "./constant";

export class TmqConfig {
    dsn: Dsn | null = null;
    sqlDsn: Dsn | null = null;
    user: string | null = null;
    password: string | null = null;
    token: string | null = null;
    userApp: string | null = null;
    userIp: string | null = null;
    group_id: string | null = null;
    client_id: string | null = null;
    offset_rest: string | null = null;
    topics?: Array<string>;
    auto_commit: boolean = true;
    auto_commit_interval_ms: number = 5 * 1000;
    timeout: number = 60000;
    otherConfigs: Map<string, any>;

    constructor(wsConfig: Map<string, any>) {
        this.otherConfigs = new Map();
        for (const [key, value] of wsConfig) {
            switch (key) {
                case TMQConstants.WS_URL:
                    this.dsn = parse(value);
                    this.dsn.endpoint = WS_TMQ_ENDPOINT;
                    break;
                case TMQConstants.CONNECT_USER:
                    this.user = value;
                    break;
                case TMQConstants.CONNECT_PASS:
                    this.password = value;
                    break;
                case TMQConstants.CONNECT_TOKEN:
                    this.token = value;
                    this.otherConfigs.set(key, value);
                    break;
                case TMQConstants.USER_APP:
                    this.userApp = value;
                    break;
                case TMQConstants.USER_IP:
                    this.userIp = value;
                    break;
                case TMQConstants.GROUP_ID:
                    this.group_id = value;
                    break;
                case TMQConstants.CLIENT_ID:
                    this.client_id = value;
                    break;
                case TMQConstants.AUTO_OFFSET_RESET:
                    this.offset_rest = value;
                    break;
                case TMQConstants.ENABLE_AUTO_COMMIT:
                    this.auto_commit = value;
                    break;
                case TMQConstants.AUTO_COMMIT_INTERVAL_MS:
                    this.auto_commit_interval_ms = value;
                    break;
                case TMQConstants.CONNECT_MESSAGE_TIMEOUT:
                    this.timeout = value;
                    break;
                default:
                    this.otherConfigs.set(key, value);
            }
        }

        if (this.dsn) {
            if (this.user) {
                this.dsn.username = this.user;
            } else {
                this.user = this.dsn.username;
            }

            if (this.password) {
                this.dsn.password = this.password;
            } else {
                this.password = this.dsn.password;
            }

            if (this.token) {
                this.dsn.params.set("bearer_token", this.token);
            } else {
                const bearerToken = this.dsn.params.get("bearer_token");
                if (bearerToken) {
                    this.token = bearerToken;
                    this.otherConfigs.set(TMQConstants.CONNECT_TOKEN, bearerToken);
                } else {
                    this.dsn.params.delete("bearer_token");
                }
            }

            if (this.userApp == null && this.dsn.params.has("user_app")) {
                this.userApp = this.dsn.params.get("user_app") || null;
            }
            if (this.userIp == null && this.dsn.params.has("user_ip")) {
                this.userIp = this.dsn.params.get("user_ip") || null;
            }

            this.sqlDsn = new Dsn(
                this.dsn.scheme,
                this.dsn.username,
                this.dsn.password,
                this.dsn.addresses,
                this.dsn.database,
                this.dsn.params
            );
        }
    }
}
