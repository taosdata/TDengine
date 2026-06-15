// Package branchmodel loads a corpus of positive and negative SQL cases and
// tracks branch and rule coverage during fuzzing.
//
// branchmodel 包加载正例和负例 SQL 用例的语料,
// 并在 fuzzing 过程中跟踪分支和规则的覆盖情况。
package branchmodel

import "time"

// PositiveCase is a SQL statement expected to parse, together with the structural
// assertions used to confirm a generated statement matches its branch.
//
// PositiveCase 表示一条预期能解析成功的 SQL 语句,以及用于确认生成语句
// 匹配其分支的结构断言。
type PositiveCase struct {
	ID        string // unique case identifier / 用例唯一标识
	Rule      string // grammar rule the case targets / 用例针对的语法规则
	BranchSig string // signature describing the specific branch covered / 描述所覆盖具体分支的签名
	SQL       string // the SQL text of the case / 用例的 SQL 文本
	KeyAssert string // semicolon-separated key=value structural assertions / 以分号分隔的 key=value 结构断言
	Source    string // corpus file the case was loaded from / 用例所来自的语料文件
}

// NegativeCase is a SQL statement expected to be rejected with a given error type.
//
// NegativeCase 表示一条预期会以给定错误类型被拒绝的 SQL 语句。
type NegativeCase struct {
	ID      string // unique case identifier / 用例唯一标识
	Rule    string // grammar rule the case targets / 用例针对的语法规则
	SQL     string // the SQL text of the case / 用例的 SQL 文本
	ErrType string // expected error type/category / 期望的错误类型/类别
}

// Corpus holds the loaded positive, negative, and write-only SQL cases.
//
// Corpus 保存已加载的正例、负例以及只写 SQL 用例。
type Corpus struct {
	Positive []PositiveCase // statements expected to parse and match a branch / 预期能解析并匹配某分支的语句
	Negative []NegativeCase // statements expected to be rejected / 预期会被拒绝的语句
	WriteSQL []string       // auxiliary write statements used to set up state / 用于建立状态的辅助写入语句
}

// HitInfo records when and how a corpus case was first covered.
//
// HitInfo 记录某条语料用例首次被覆盖的时间和方式。
type HitInfo struct {
	CaseID    string    `json:"case_id"`              // id of the covered case / 被覆盖用例的 id
	SQL       string    `json:"sql"`                  // SQL text that produced the hit / 产生该命中的 SQL 文本
	At        time.Time `json:"at"`                   // time the case was first covered / 用例首次被覆盖的时间
	Rule      string    `json:"rule"`                 // grammar rule covered / 被覆盖的语法规则
	BranchSig string    `json:"branch_sig,omitempty"` // branch signature, when applicable / 分支签名(适用时)
	Source    string    `json:"source,omitempty"`     // corpus file the case came from / 用例所来自的语料文件
}

// CoverageSummary captures positive and negative branch coverage at a point in time.
//
// CoverageSummary 捕获某一时刻的正例和负例分支覆盖情况。
type CoverageSummary struct {
	Required       int      `json:"required"`              // number of positive cases / 正例数量
	Hit            int      `json:"hit"`                   // number of positive cases covered / 已覆盖的正例数量
	Missing        []string `json:"missing"`               // sorted ids of uncovered positive cases / 未覆盖正例的 id(已排序)
	RequiredNeg    int      `json:"required_negative"`     // number of negative cases / 负例数量
	HitNeg         int      `json:"hit_negative"`          // number of negative cases covered / 已覆盖的负例数量
	MissingNeg     []string `json:"missing_negative"`      // sorted ids of uncovered negative cases / 未覆盖负例的 id(已排序)
	CoverageRatio  float64  `json:"coverage_ratio"`        // positive Hit/Required / 正例 Hit/Required
	NegRejectRatio float64  `json:"negative_reject_ratio"` // negative HitNeg/RequiredNeg / 负例 HitNeg/RequiredNeg
}
