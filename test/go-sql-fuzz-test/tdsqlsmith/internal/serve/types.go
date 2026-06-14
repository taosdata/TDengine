package serve

// types.go defines the configuration and JSON response types used by the serve package.
//
// types.go 定义 serve 包所使用的配置与 JSON 响应类型。

import "time"

// Config holds the runtime settings for the serve HTTP server.
//
// Config 持有 serve HTTP 服务器的运行时设置。
type Config struct {
	Version     string // build version string reported by the server / 服务器上报的构建版本字符串
	Listen      string // address the HTTP server listens on (e.g. ":8080") / HTTP 服务器监听的地址（如 ":8080"）
	APIToken    string // bearer token required to access the API / 访问 API 所需的 bearer token
	DataDir     string // directory holding fuzz input data / 存放 fuzz 输入数据的目录
	OutDir      string // directory holding run report output subdirectories / 存放运行报告输出子目录的目录
	AllowOrigin string // value used for the Access-Control-Allow-Origin header / 用于 Access-Control-Allow-Origin 头的取值
}

// reportSummary is the condensed view of a run report returned by the reports listing endpoint.
//
// reportSummary 是 reports 列表端点返回的运行报告精简视图。
type reportSummary struct {
	RunID                   string    `json:"run_id"`                    // unique identifier of the run / 运行的唯一标识
	StartedAt               time.Time `json:"started_at"`                // time the run started / 运行开始的时间
	GeneratedAt             time.Time `json:"generated_at"`              // time the report was generated / 报告生成的时间
	ExecutionDurationMS     int64     `json:"execution_duration_ms"`     // total execution duration in milliseconds / 总执行时长（毫秒）
	Completed               bool      `json:"completed"`                 // whether the run completed normally / 运行是否正常完成
	IncidentCount           int       `json:"incident_count"`            // total number of incidents recorded / 记录的事件总数
	TaosdIncidentCount      int       `json:"taosd_incident_count"`      // number of taosd-side incidents / taosd 侧事件数
	TDsqlsmithIncidentCount int       `json:"tdsqlsmith_incident_count"` // number of tdsqlsmith-side incidents / tdsqlsmith 侧事件数
	TotalExecuted           int64     `json:"total_executed"`            // total statements executed / 已执行的语句总数
	QueryRuleHit            int       `json:"query_rule_hit"`            // number of required query rules that were hit / 已命中的必需查询规则数
	QueryRuleRequired       int       `json:"query_rule_required"`       // number of query rules required for coverage / 覆盖所需的查询规则数
	QueryRuleCoverageRatio  float64   `json:"query_rule_coverage_ratio"` // ratio of hit to required query rules / 命中数与必需查询规则数之比
	QueryRuleMissingCount   int       `json:"query_rule_missing_count"`  // number of required query rules not yet hit / 尚未命中的必需查询规则数
}
