// Package config parses command-line arguments into typed configuration for the
// run, replay, and serve subcommands, including the sqlsmith-compatible flags.
//
// config 包将命令行参数解析为 run、replay 和 serve 子命令的类型化配置，
// 包含与 sqlsmith 兼容的参数。
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Command names the selected subcommand.
//
// Command 标识所选中的子命令。
type Command string

const (
	CommandRun    Command = "run"    // generate and execute fuzz queries / 生成并执行模糊测试查询
	CommandReplay Command = "replay" // replay statements from a run report / 从运行报告中重放语句
	CommandServe  Command = "serve"  // run the HTTP control/report server / 运行 HTTP 控制/报告服务器
)

// DefaultTargetDSN is the TDengine DSN used when none is provided.
//
// DefaultTargetDSN 是未提供 DSN 时使用的默认 TDengine DSN。
const DefaultTargetDSN = "root:taosdata@tcp(127.0.0.1:6030)/"

// DefaultOutDir is the default output directory for run artifacts.
//
// DefaultOutDir 是运行产物的默认输出目录。
const DefaultOutDir = "out"

// Parsed is the fully parsed command line: the selected command, its config, and global flags.
//
// Parsed 是完全解析后的命令行：所选命令、其配置以及全局参数。
type Parsed struct {
	Command     Command      // the selected subcommand / 所选的子命令
	Run         RunConfig    // configuration for the run command / run 命令的配置
	Replay      ReplayConfig // configuration for the replay command / replay 命令的配置
	Serve       ServeConfig  // configuration for the serve command / serve 命令的配置
	ShowHelp    bool         // print usage and exit / 打印用法并退出
	ShowVersion bool         // print version and exit / 打印版本并退出
}

// RunConfig holds the options for the run subcommand.
//
// RunConfig 保存 run 子命令的选项。
type RunConfig struct {
	DSN                  string        // TDengine DSN / TDengine DSN
	Seed                 int64         // RNG seed / 随机数生成器种子
	RNGState             string        // serialized RNG state to deserialize / 待反序列化的 RNG 状态序列化串
	Cases                int           // number of queries to generate (0 means duration-bound) / 要生成的查询数量（0 表示按时长限制）
	Duration             time.Duration // wall-clock run duration / 运行的挂钟时长
	StmtTimeout          time.Duration // per-statement timeout / 单条语句超时
	OutDir               string        // output directory (absolute after parsing) / 输出目录（解析后为绝对路径）
	CleanupSuccessRunDir bool          // remove temp child logs on clean exit / 干净退出时移除临时子进程日志
	MutationLevel        int           // mutation intensity in [0,3] / 变异强度，取值范围 [0,3]
	StopWhenCovered      bool          // stop once all required query rules are covered / 一旦覆盖所有必需查询规则即停止
	DryRun               bool          // parse-gate only, skip execution / 仅做解析校验，跳过执行
	Verbose              bool          // verbose progress output / 输出详细进度
	DumpAllQueries       bool          // print every generated query / 打印每条生成的查询
	DumpAllGraphs        bool          // dump generated AST graphs / 导出生成的 AST 图
	ExcludeCatalog       bool          // reserved compatibility option / 保留的兼容性选项
	LegacyMode           bool          // parsed via sqlsmith-compatible mode / 通过 sqlsmith 兼容模式解析
	WorkloadConfig       string        // path to a workload TOML config / 负载 TOML 配置文件路径
	ExecProfile          string        // execution profile: strict|balanced|aggressive / 执行档位：strict|balanced|aggressive
}

// ReplayConfig holds the options for the replay subcommand.
//
// ReplayConfig 保存 replay 子命令的选项。
type ReplayConfig struct {
	DSN         string        // TDengine DSN / TDengine DSN
	File        string        // path to the run report to replay (absolute after parsing) / 待重放的运行报告路径（解析后为绝对路径）
	Count       int           // number of replay iterations / 重放迭代次数
	StmtTimeout time.Duration // per-statement timeout / 单条语句超时
}

// ServeConfig holds the options for the serve subcommand.
//
// ServeConfig 保存 serve 子命令的选项。
type ServeConfig struct {
	Listen      string // listen address / 监听地址
	APIToken    string // bearer token required for API auth / API 鉴权所需的 bearer token
	DataDir     string // state data directory (absolute after parsing) / 状态数据目录（解析后为绝对路径）
	OutDir      string // run output directory (absolute after parsing) / 运行输出目录（解析后为绝对路径）
	AllowOrigin string // CORS Access-Control-Allow-Origin value / CORS Access-Control-Allow-Origin 取值
}

// legacyOptRE matches the sqlsmith-compatible "--option" / "--option=value" forms.
//
// legacyOptRE 匹配 sqlsmith 兼容的 "--option" / "--option=value" 形式。
var legacyOptRE = regexp.MustCompile(`^--(help|verbose|target|sqlite|monetdb|version|dump-all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-state|exclude-catalog|config|exec-profile)(?:=((?:.|\n)*))?$`)

// Parse interprets args, dispatching to the run/replay/serve subcommands or to
// the sqlsmith-compatible legacy mode when the first argument starts with "--".
//
// Parse 解释 args，将其分派到 run/replay/serve 子命令；当第一个参数以 "--"
// 开头时，则分派到 sqlsmith 兼容的旧版模式。
func Parse(args []string) (*Parsed, error) {
	if len(args) == 0 {
		return nil, usageErr("missing subcommand or classic options")
	}
	if strings.HasPrefix(args[0], "--") {
		return parseLegacy(args)
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		return &Parsed{ShowHelp: true}, nil
	}

	switch args[0] {
	case string(CommandRun):
		runCfg, err := parseRun(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandRun, Run: *runCfg}, nil
	case string(CommandReplay):
		replayCfg, err := parseReplay(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandReplay, Replay: *replayCfg}, nil
	case string(CommandServe):
		serveCfg, err := parseServe(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandServe, Serve: *serveCfg}, nil
	default:
		return nil, usageErr(fmt.Sprintf("unknown subcommand %q", args[0]))
	}
}

// parseLegacy parses the sqlsmith-compatible flat option list into a RunConfig,
// rejecting unsupported targets (sqlite/monetdb) and applying TDengine defaults.
//
// parseLegacy 将 sqlsmith 兼容的扁平选项列表解析为 RunConfig，
// 拒绝不支持的目标（sqlite/monetdb）并应用 TDengine 默认值。
func parseLegacy(args []string) (*Parsed, error) {
	opts := map[string]string{}
	for _, raw := range args {
		m := legacyOptRE.FindStringSubmatch(raw)
		if m == nil {
			return nil, usageErr(fmt.Sprintf("cannot parse option: %s", raw))
		}
		opts[m[1]] = m[2]
	}
	if _, ok := opts["help"]; ok {
		return &Parsed{ShowHelp: true}, nil
	}
	if _, ok := opts["version"]; ok {
		return &Parsed{ShowVersion: true}, nil
	}
	if opts["sqlite"] != "" || opts["monetdb"] != "" {
		return nil, usageErr("this implementation supports TDengine only; use --target")
	}

	cfg := RunConfig{
		DSN:                  DefaultTargetDSN,
		Seed:                 int64(os.Getpid()),
		RNGState:             opts["rng-state"],
		Cases:                0,
		StmtTimeout:          2 * time.Second,
		OutDir:               DefaultOutDir,
		CleanupSuccessRunDir: true,
		MutationLevel:        1,
		StopWhenCovered:      false,
		DryRun:               hasOpt(opts, "dry-run"),
		Verbose:              hasOpt(opts, "verbose"),
		DumpAllQueries:       hasOpt(opts, "dump-all-queries"),
		DumpAllGraphs:        hasOpt(opts, "dump-all-graphs"),
		ExcludeCatalog:       hasOpt(opts, "exclude-catalog"),
		LegacyMode:           true,
		WorkloadConfig:       strings.TrimSpace(opts["config"]),
		ExecProfile:          strings.TrimSpace(opts["exec-profile"]),
	}
	if cfg.ExecProfile == "" {
		cfg.ExecProfile = "strict"
	}
	if v := strings.TrimSpace(opts["target"]); v != "" {
		cfg.DSN = v
	}
	if s := strings.TrimSpace(opts["seed"]); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, usageErr(fmt.Sprintf("invalid --seed=%q: %v", s, err))
		}
		cfg.Seed = n
	}
	if mq := strings.TrimSpace(opts["max-queries"]); mq != "" {
		n, err := strconv.ParseInt(mq, 10, 64)
		if err != nil || n < 0 {
			return nil, usageErr(fmt.Sprintf("invalid --max-queries=%q", mq))
		}
		cfg.Cases = int(n)
	}
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut

	return &Parsed{Command: CommandRun, Run: cfg}, nil
}

// hasOpt reports whether key is present in the parsed legacy option map.
//
// hasOpt 报告 key 是否存在于已解析的旧版选项映射中。
func hasOpt(m map[string]string, key string) bool {
	_, ok := m[key]
	return ok
}

// parseRun parses the run subcommand flags, applies defaults, and validates the
// resulting RunConfig (cases, timeout, mutation level, exec profile, out dir).
//
// parseRun 解析 run 子命令参数、应用默认值，并校验得到的 RunConfig
// （cases、超时、变异级别、执行档位、输出目录）。
func parseRun(args []string) (*RunConfig, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &RunConfig{}
	target := fs.String("target", "", "TDengine DSN (sqlsmith-compatible alias)")
	seed := fs.Int64("seed", time.Now().UnixNano(), "seed")
	maxQueries := fs.Int("max-queries", -1, "sqlsmith-compatible alias for --cases")

	fs.StringVar(&cfg.DSN, "dsn", "", "TDengine DSN")
	fs.IntVar(&cfg.Cases, "cases", 2000, "generated query count")
	duration := fs.Duration("duration", 0, "run duration, e.g. 10m")
	fs.DurationVar(&cfg.StmtTimeout, "stmt-timeout", 2*time.Second, "statement timeout")
	fs.StringVar(&cfg.OutDir, "out-dir", DefaultOutDir, "output directory")
	fs.BoolVar(&cfg.CleanupSuccessRunDir, "cleanup-success-run-dir", true, "cleanup temporary child logs only when run exits cleanly and no core dump is observed")
	fs.IntVar(&cfg.MutationLevel, "mutation-level", 1, "0..3")
	fs.BoolVar(&cfg.StopWhenCovered, "stop-when-covered", true, "stop when all required query rules are covered")
	fs.BoolVar(&cfg.DryRun, "dry-run", false, "parse-gate only, skip TDengine execution")
	fs.BoolVar(&cfg.Verbose, "verbose", false, "verbose progress")
	fs.StringVar(&cfg.RNGState, "rng-state", "", "deserialize RNG state")
	fs.BoolVar(&cfg.DumpAllQueries, "dump-all-queries", false, "print generated queries")
	fs.BoolVar(&cfg.DumpAllGraphs, "dump-all-graphs", false, "dump generated AST graphs")
	fs.BoolVar(&cfg.ExcludeCatalog, "exclude-catalog", false, "reserved compatibility option")
	fs.StringVar(&cfg.WorkloadConfig, "config", "", "workload TOML config path (go-sqlsmith style)")
	fs.StringVar(&cfg.ExecProfile, "exec-profile", "strict", "execution profile: strict|balanced|aggressive")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	cfg.Seed = *seed
	cfg.Duration = *duration
	if *maxQueries >= 0 {
		cfg.Cases = *maxQueries
	}
	if strings.TrimSpace(*target) != "" {
		cfg.DSN = strings.TrimSpace(*target)
	}

	if cfg.Cases < 0 {
		return nil, usageErr("--cases must be >= 0")
	}
	if cfg.Cases == 0 && cfg.Duration <= 0 {
		cfg.Cases = 2000
	}
	if cfg.StmtTimeout <= 0 {
		return nil, usageErr("--stmt-timeout must be > 0")
	}
	if cfg.MutationLevel < 0 || cfg.MutationLevel > 3 {
		return nil, usageErr("--mutation-level must be in [0,3]")
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ExecProfile)) {
	case "strict", "balanced", "aggressive":
	default:
		return nil, usageErr("--exec-profile must be one of strict|balanced|aggressive")
	}
	if !cfg.DryRun && strings.TrimSpace(cfg.DSN) == "" {
		cfg.DSN = DefaultTargetDSN
	}

	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut
	return cfg, nil
}

// parseReplay parses the replay subcommand flags and validates that a report
// file is given with a positive count and timeout.
//
// parseReplay 解析 replay 子命令参数，并校验是否提供了报告文件，
// 且 count 和超时为正值。
func parseReplay(args []string) (*ReplayConfig, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &ReplayConfig{}
	fs.StringVar(&cfg.DSN, "dsn", DefaultTargetDSN, "TDengine DSN")
	fs.StringVar(&cfg.File, "file", "", "run_report.json path")
	fs.IntVar(&cfg.Count, "count", 1, "execution count")
	fs.DurationVar(&cfg.StmtTimeout, "stmt-timeout", 2*time.Second, "statement timeout")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	if strings.TrimSpace(cfg.File) == "" {
		return nil, usageErr("--file is required")
	}
	if cfg.Count <= 0 {
		return nil, usageErr("--count must be > 0")
	}
	if cfg.StmtTimeout <= 0 {
		return nil, usageErr("--stmt-timeout must be > 0")
	}

	absFile, err := filepath.Abs(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("resolve --file: %w", err)
	}
	cfg.File = absFile
	return cfg, nil
}

// parseServe parses the serve subcommand flags, requiring a listen address and
// API token and resolving the data and output directories to absolute paths.
//
// parseServe 解析 serve 子命令参数，要求提供监听地址和 API token，
// 并将数据目录和输出目录解析为绝对路径。
func parseServe(args []string) (*ServeConfig, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &ServeConfig{}
	fs.StringVar(&cfg.Listen, "listen", ":8080", "listen address")
	fs.StringVar(&cfg.APIToken, "api-token", envOr("TDSQLSMITH_API_TOKEN", "tdsqlsmith-dev-token"), "bearer token for API auth")
	fs.StringVar(&cfg.DataDir, "data-dir", "data", "state data directory")
	fs.StringVar(&cfg.OutDir, "out-dir", DefaultOutDir, "run output directory")
	fs.StringVar(&cfg.AllowOrigin, "allow-origin", "*", "CORS Access-Control-Allow-Origin value")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		return nil, usageErr("--listen is required")
	}
	if strings.TrimSpace(cfg.APIToken) == "" {
		return nil, usageErr("--api-token is required")
	}
	absData, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --data-dir: %w", err)
	}
	cfg.DataDir = absData
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut
	return cfg, nil
}

// Usage returns the multi-line CLI usage text.
//
// Usage 返回多行的命令行用法文本。
func Usage() string {
	return strings.TrimSpace(`
Usage:
  tdsqlsmith run [flags]
  tdsqlsmith replay [flags]
  tdsqlsmith serve [flags]

  # sqlsmith-compatible mode (no subcommand):
  tdsqlsmith --target=... --max-queries=1000 --verbose

Run flags:
  --dsn=...                  TDengine DSN
  --target=...               sqlsmith-compatible alias of --dsn
  --seed=INT                 RNG seed
  --rng-state=HEX            deserialize RNG state
  --cases=INT                generated query count
  --max-queries=INT          sqlsmith-compatible alias for --cases
  --duration=DURATION        run duration, e.g. 10m
  --stmt-timeout=DURATION    single statement timeout
  --out-dir=PATH             output directory
  --cleanup-success-run-dir=true|false (cleanup temp child logs; reports are always kept)
  --mutation-level=0..3      mutation intensity
  --stop-when-covered=true|false
  --dry-run
  --verbose
  --dump-all-queries
  --dump-all-graphs
  --exclude-catalog
  --config=PATH              workload TOML config (go-sqlsmith style)
  --exec-profile=PROFILE     strict|balanced|aggressive

Replay flags:
  --dsn=...
  --file=PATH
  --count=INT
  --stmt-timeout=DURATION

Serve flags:
  --listen=:8080
  --api-token=TOKEN
  --data-dir=PATH
  --out-dir=PATH
  --allow-origin=*
`)
}

// usageErr builds a trimmed usage error from msg.
//
// usageErr 根据 msg 构建一个去除首尾空白的用法错误。
func usageErr(msg string) error {
	return errors.New(strings.TrimSpace(msg))
}

// envOr returns the trimmed value of environment variable key, or fallback if unset/empty.
//
// envOr 返回环境变量 key 去除首尾空白后的值，若未设置或为空则返回 fallback。
func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
