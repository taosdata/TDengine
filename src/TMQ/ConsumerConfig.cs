using System.Collections.Generic;

namespace TDengine.TMQ
{
    public class ConsumerConfig : Config
    {
        public ConsumerConfig() : base()
        {
        }

        public ConsumerConfig(Config config) : base(config)
        {
        }

        public ConsumerConfig(IDictionary<string, string> config) : base(config)
        {
        }

        public string GroupId
        {
            get { return Get("group.id"); }
            set { this.SetObject("group.id", value); }
        }

        public string ClientId
        {
            get { return Get("client.id"); }
            set { this.SetObject("client.id", value); }
        }

        public string EnableAutoCommit
        {
            get { return Get("enable.auto.commit"); }
            set { this.SetObject("enable.auto.commit", value); }
        }

        public string AutoCommitIntervalMs
        {
            get => Get("auto.commit.interval.ms");

            set => this.SetObject("auto.commit.interval.ms", value);
        }


        public string AutoOffsetReset
        {
            get => Get("auto.offset.reset");
            set => this.SetObject("auto.offset.reset", value);
        }

        public string MsgWithTableName
        {
            get => Get("msg.with.table.name");
            set => this.SetObject("msg.with.table.name", value);
        }

        public string TDConnectIp
        {
            get => Get("td.connect.ip");
            set => this.SetObject("td.connect.ip", value);
        }

        public string TDUseSSL
        {
            get => Get("useSSL");
            set => this.SetObject("useSSL", value);
        }

        public string TDToken
        {
            get => Get("token");
            set => this.SetObject("token", value);
        }

        public string TDEnableCompression
        {
            get => Get("ws.message.enableCompression");
            set => this.SetObject("ws.message.enableCompression", value);
        }

        public string TDConnectUser
        {
            get => Get("td.connect.user");
            set => this.SetObject("td.connect.user", value);
        }

        public string TDConnectPasswd
        {
            get => Get("td.connect.pass");
            set => SetObject("td.connect.pass", value);
        }

        public string TDConnectPort
        {
            get => Get("td.connect.port");
            set => this.SetObject("td.connect.port", value);
        }

        public string TDDatabase
        {
            get => Get("td.connect.db");
            set => this.SetObject("td.connect.db", value);
        }

        public string TDConnectType
        {
            get => Get("td.connect.type");
            set => SetObject("td.connect.type", value);
        }

        public string TDReconnect
        {
            get => Get("ws.autoReconnect");
            set => SetObject("ws.autoReconnect", value);
        }

        public string TDReconnectRetryCount
        {
            get => Get("ws.reconnect.retry.count");
            set => SetObject("ws.reconnect.retry.count", value);
        }

        public string TDReconnectIntervalMs
        {
            get => Get("ws.reconnect.interval.ms");
            set => SetObject("ws.reconnect.interval.ms", value);
        }

        public string SessionTimeoutMs
        {
            get => Get("session.timeout.ms");
            set => SetObject("session.timeout.ms", value);
        }

        public string MaxPollIntervalMs
        {
            get => Get("max.poll.interval.ms");
            set => SetObject("max.poll.interval.ms", value);
        }
        
        public string ConnectionTimezone
        {
            get => Get("connectionTimezone");
            set => SetObject("connectionTimezone", value);
        }
    }
}