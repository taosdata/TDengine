package grafana_webhook

const (
	StateOk       State = "ok"
	StatePaused   State = "paused"
	StateAlerting State = "alerting"
	StatePending  State = "pending"
	StateNoData   State = "no_data"
)

type State string

// Body represents Grafana request body
type Body struct {
	Title       string                   `json:"title"`
	RuleID      int                      `json:"ruleId"`
	RuleName    string                   `json:"ruleName"`
	RuleURL     string                   `json:"ruleUrl"`
	State       State                    `json:"state"`
	ImageURL    string                   `json:"imageUrl"`
	Message     string                   `json:"message"`
	EvalMatches []map[string]interface{} `json:"evalMatches"`
}

// BodyOnReadAllSizeLimitErr creates a default instance in case of a request size limit error
func BodyOnReadAllSizeLimitErr() *Body {
	return &Body{
		Title:   "undefined",
		Message: "request size limit exceeded",
	}
}
